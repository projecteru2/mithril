//! Cross-worker backend sharding: one pipelined connection per node, owned by
//! the worker its address hashes to; other workers hand requests across.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::backend::{OUTBOUND_QUEUE, Outbound, check_rearm, drain_channel, open, pump};
use crate::cache::{ReplyCache, TrackingFrames};
use crate::config::Config;
use crate::log_debug;

/// A request crossing to a node-owner worker.
pub type RemoteOutbound = Outbound<RemoteSink>;

/// Where a sharded reply is delivered.
pub enum RemoteSink {
    /// Ordered client reply pushed straight into the session's shared queue.
    Session {
        queue: std::sync::Arc<crate::client::SharedQueue>,
        seq: u64,
    },
    /// Single reply for mergers and broadcasts.
    One(oneshot::Sender<Bytes>),
}

/// A node connection handed to its owner worker to run.
pub struct NewConn {
    pub addr: String,
    pub readonly: bool,
    pub rx: mpsc::Receiver<RemoteOutbound>,
}

/// Cache invalidations crossing from a tracker to every worker.
pub type Invalidations = Arc<[Bytes]>;

/// Per-worker control channels: new pipes to run, invalidations to apply.
pub struct Controls {
    pub conns: mpsc::UnboundedSender<NewConn>,
    pub invals: mpsc::Sender<Invalidations>,
}

/// Process-wide shard fabric shared by every worker.
pub struct Fabric {
    controls: Vec<Controls>,
    conns: Mutex<[HashMap<Box<str>, mpsc::Sender<RemoteOutbound>>; 2]>,
}

impl Fabric {
    pub fn new(controls: Vec<Controls>) -> Arc<Fabric> {
        Arc::new(Fabric {
            controls,
            conns: Mutex::new([HashMap::new(), HashMap::new()]),
        })
    }

    /// Returns the node's shared pipe, dialing it on the owner worker first.
    pub fn pipe(&self, addr: &str, readonly: bool) -> mpsc::Sender<RemoteOutbound> {
        let mut conns = self
            .conns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let conns = &mut conns[usize::from(readonly)];
        if let Some(tx) = conns.get(addr)
            && !tx.is_closed()
        {
            return tx.clone();
        }
        let (tx, rx) = mpsc::channel(OUTBOUND_QUEUE);
        conns.insert(addr.into(), tx.clone());
        let _ = self.controls[self.owner(addr)].conns.send(NewConn {
            addr: addr.to_string(),
            readonly,
            rx,
        });
        tx
    }

    /// The worker that runs the node's pipe and tracker.
    pub fn owner(&self, addr: &str) -> usize {
        fnv(addr.as_bytes()) as usize % self.controls.len()
    }

    /// Redirects the node's live pipe at a new tracker; later dials learn it at handshake.
    pub async fn rearm(&self, addr: &str, frame: Bytes) -> Result<(), String> {
        let tx = self
            .conns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)[0]
            .get(addr)
            .filter(|tx| !tx.is_closed())
            .cloned();
        let Some(tx) = tx else {
            return Ok(());
        };
        let (otx, orx) = oneshot::channel();
        let sent = tx
            .send(RemoteOutbound {
                head: None,
                frame,
                expect: 1,
                sink: RemoteSink::One(otx),
            })
            .await;
        if sent.is_err() {
            return Ok(());
        }
        check_rearm(orx.await.ok())
    }

    /// Hands invalidations to every worker; false when one could not take them.
    pub fn invalidate(&self, keys: Invalidations) -> bool {
        let mut all = true;
        for c in &self.controls {
            all &= c.invals.try_send(keys.clone()).is_ok();
        }
        all
    }

    // generation-safe: a replacement pipe created meanwhile must survive
    fn forget(&self, addr: &str, readonly: bool) {
        let mut conns = self
            .conns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let conns = &mut conns[usize::from(readonly)];
        if conns.get(addr).is_some_and(|tx| tx.is_closed()) {
            conns.remove(addr);
        }
    }
}

/// Worker control loop: runs the pipes assigned here, applies broadcast invalidations.
pub async fn control_loop(
    mut ctl: mpsc::UnboundedReceiver<NewConn>,
    mut invals: mpsc::Receiver<Invalidations>,
    fabric: Arc<Fabric>,
    cfg: Arc<Config>,
    cache: Option<Rc<ReplyCache>>,
    tracking: Option<TrackingFrames>,
) {
    loop {
        tokio::select! {
            nc = ctl.recv() => {
                let Some(nc) = nc else { return };
                let frame = match &tracking {
                    Some(t) if !nc.readonly => t.borrow().get(nc.addr.as_str()).cloned(),
                    _ => None,
                };
                let fabric = fabric.clone();
                let cfg = cfg.clone();
                tokio::task::spawn_local(async move {
                    run_shard_conn(&nc.addr, nc.readonly, nc.rx, frame, &cfg).await;
                    fabric.forget(&nc.addr, nc.readonly);
                });
            }
            keys = invals.recv() => {
                let Some(keys) = keys else { return };
                if let Some(c) = &cache {
                    for k in keys.iter() {
                        c.invalidate(k);
                    }
                }
            }
        }
    }
}

async fn run_shard_conn(
    addr: &str,
    readonly: bool,
    mut rx: mpsc::Receiver<RemoteOutbound>,
    tracking: Option<Bytes>,
    cfg: &Config,
) {
    match open(addr, readonly, cfg, tracking.as_deref()).await {
        Ok(halves) => pump(addr, &mut rx, halves, None, deliver).await,
        Err(e) => {
            log_debug!("shard connect {addr}: {e}");
            drain_channel(&mut rx, deliver);
        }
    }
}

fn deliver(sink: RemoteSink, frame: Bytes) {
    match sink {
        RemoteSink::Session { queue, seq } => {
            let _ = queue.send(crate::client::Reply::At(seq, frame));
        }
        RemoteSink::One(tx) => {
            let _ = tx.send(frame);
        }
    }
}

pub(crate) fn fnv(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owners_spread_and_stay_stable() {
        let addrs: Vec<String> = (0..32).map(|i| format!("10.0.0.{i}:7000")).collect();
        let owners: Vec<usize> = addrs
            .iter()
            .map(|a| fnv(a.as_bytes()) as usize % 8)
            .collect();
        let again: Vec<usize> = addrs
            .iter()
            .map(|a| fnv(a.as_bytes()) as usize % 8)
            .collect();
        assert_eq!(owners, again);
        let mut seen = [0usize; 8];
        for &o in &owners {
            seen[o] += 1;
        }
        assert!(seen.iter().filter(|&&c| c > 0).count() >= 6, "{seen:?}");
    }
}

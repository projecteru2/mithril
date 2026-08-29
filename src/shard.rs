//! Cross-worker backend sharding: one pipelined connection per node, owned by
//! the worker its address hashes to; other workers hand requests across.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};

use crate::backend::{BATCH, ERR_BACKEND_LOST, OUTBOUND_QUEUE, ensure_read_room, write_slices};
use crate::config::Config;
use crate::log_debug;
use crate::resp;

/// A request crossing to a node-owner worker.
pub struct RemoteOutbound {
    pub head: Option<Bytes>,
    pub frame: Bytes,
    /// Number of backend replies this produces; only the last is delivered.
    pub expect: u32,
    pub reply: RemoteSink,
}

/// Where a sharded reply is delivered.
pub enum RemoteSink {
    /// Ordered client reply routed back through the home worker's intake.
    Session { home: u32, token: u64, seq: u64 },
    /// Single reply for mergers and broadcasts.
    One(oneshot::Sender<Bytes>),
}

/// A reply travelling back to its home worker.
pub struct RemoteReply {
    pub token: u64,
    pub seq: u64,
    pub frame: Bytes,
}

/// A node connection handed to its owner worker to run.
pub struct NewConn {
    pub addr: String,
    pub readonly: bool,
    pub rx: mpsc::Receiver<RemoteOutbound>,
}

/// Process-wide shard fabric shared by every worker.
pub struct Fabric {
    pub intakes: Vec<mpsc::UnboundedSender<RemoteReply>>,
    controls: Vec<mpsc::UnboundedSender<NewConn>>,
    conns: Mutex<HashMap<Box<str>, mpsc::Sender<RemoteOutbound>>>,
}

impl Fabric {
    pub fn new(
        intakes: Vec<mpsc::UnboundedSender<RemoteReply>>,
        controls: Vec<mpsc::UnboundedSender<NewConn>>,
    ) -> Arc<Fabric> {
        Arc::new(Fabric {
            intakes,
            controls,
            conns: Mutex::new(HashMap::new()),
        })
    }

    /// Returns the node's shared pipe, dialing it on the owner worker first.
    pub fn pipe(&self, addr: &str, readonly: bool) -> mpsc::Sender<RemoteOutbound> {
        let key = Self::key(addr, readonly);
        let Ok(mut conns) = self.conns.lock() else {
            let (tx, _) = mpsc::channel(1);
            return tx;
        };
        if let Some(tx) = conns.get(&*key)
            && !tx.is_closed()
        {
            return tx.clone();
        }
        let (tx, rx) = mpsc::channel(OUTBOUND_QUEUE);
        conns.insert(key, tx.clone());
        let owner = fnv(addr.as_bytes()) as usize % self.controls.len();
        let _ = self.controls[owner].send(NewConn {
            addr: addr.to_string(),
            readonly,
            rx,
        });
        tx
    }

    fn forget(&self, addr: &str, readonly: bool) {
        if let Ok(mut conns) = self.conns.lock() {
            conns.remove(&*Self::key(addr, readonly));
        }
    }

    fn key(addr: &str, readonly: bool) -> Box<str> {
        if readonly {
            format!("ro:{addr}").into()
        } else {
            addr.into()
        }
    }
}

/// Owner-worker loop: spawns a conn task for every pipe the fabric assigns here.
pub async fn control_loop(
    mut ctl: mpsc::UnboundedReceiver<NewConn>,
    fabric: Arc<Fabric>,
    cfg: Arc<Config>,
) {
    while let Some(nc) = ctl.recv().await {
        let fabric = fabric.clone();
        let cfg = cfg.clone();
        tokio::task::spawn_local(async move {
            run_shard_conn(&nc.addr, nc.readonly, nc.rx, &cfg, &fabric).await;
            fabric.forget(&nc.addr, nc.readonly);
        });
    }
}

// mirrors backend::run_conn for the crossable sink type; never abortable
async fn run_shard_conn(
    addr: &str,
    readonly: bool,
    mut rx: mpsc::Receiver<RemoteOutbound>,
    cfg: &Config,
    fabric: &Fabric,
) {
    struct Pending {
        expect: u32,
        reply: RemoteSink,
    }
    let setup = async {
        let stream = crate::backend::connect(addr, cfg.tcp_keepalive_secs)
            .await
            .map_err(|e| e.to_string())?;
        let (mut r, mut w) = stream.into_split();
        crate::backend::handshake(
            &mut r,
            &mut w,
            readonly,
            &cfg.backend_user,
            &cfg.backend_pass,
        )
        .await?;
        Ok::<_, String>((r, w))
    };
    let (mut read_half, mut write_half) = match setup.await {
        Ok(h) => h,
        Err(e) => {
            log_debug!("shard connect {addr}: {e}");
            drain(&mut rx, fabric);
            return;
        }
    };
    let deliver = |reply: RemoteSink, frame: Bytes| match reply {
        RemoteSink::Session { home, token, seq } => {
            let _ = fabric.intakes[home as usize].send(RemoteReply { token, seq, frame });
        }
        RemoteSink::One(tx) => {
            let _ = tx.send(frame);
        }
    };
    let mut pending: std::collections::VecDeque<Pending> = std::collections::VecDeque::new();
    let mut front_err: Option<Bytes> = None;
    let mut batch: Vec<RemoteOutbound> = Vec::with_capacity(BATCH);
    let mut frames: Vec<Bytes> = Vec::with_capacity(BATCH * 2);
    let mut buf = bytes::BytesMut::with_capacity(crate::backend::READ_INIT);
    'io: loop {
        loop {
            match resp::scan_value(&buf) {
                resp::Scan::Complete(len) => {
                    let frame = buf.split_to(len).freeze();
                    let is_err = frame.first() == Some(&b'-');
                    match pending.front_mut() {
                        Some(front) if front.expect > 1 => {
                            front.expect -= 1;
                            if front_err.is_none() && is_err {
                                front_err = Some(frame);
                            }
                        }
                        _ => {
                            if let Some(d) = pending.pop_front() {
                                let reply = match front_err.take() {
                                    Some(err) if is_err => err,
                                    _ => frame,
                                };
                                deliver(d.reply, reply);
                            }
                        }
                    }
                }
                resp::Scan::Invalid(e) => {
                    log_debug!("shard backend {addr} protocol error: {e}");
                    break 'io;
                }
                resp::Scan::Incomplete => break,
            }
        }
        ensure_read_room(&mut buf);
        tokio::select! {
            n = rx.recv_many(&mut batch, BATCH) => {
                if n == 0 {
                    break 'io;
                }
                frames.clear();
                for out in batch.drain(..) {
                    pending.push_back(Pending {
                        expect: out.expect,
                        reply: out.reply,
                    });
                    if let Some(h) = out.head {
                        frames.push(h);
                    }
                    frames.push(out.frame);
                }
                let mut slices: Vec<std::io::IoSlice<'_>> =
                    frames.iter().map(|f| std::io::IoSlice::new(f)).collect();
                if write_slices(&mut write_half, &mut slices).await.is_err() {
                    break 'io;
                }
            }
            r = read_half.read_buf(&mut buf) => {
                match r {
                    Ok(0) | Err(_) => break 'io,
                    Ok(_) => {}
                }
            }
        }
    }
    for p in pending.drain(..) {
        deliver(p.reply, Bytes::from_static(ERR_BACKEND_LOST));
    }
    drain(&mut rx, fabric);
}

fn drain(rx: &mut mpsc::Receiver<RemoteOutbound>, fabric: &Fabric) {
    rx.close();
    while let Ok(out) = rx.try_recv() {
        match out.reply {
            RemoteSink::Session { home, token, seq } => {
                let _ = fabric.intakes[home as usize].send(RemoteReply {
                    token,
                    seq,
                    frame: Bytes::from_static(ERR_BACKEND_LOST),
                });
            }
            RemoteSink::One(tx) => {
                let _ = tx.send(Bytes::from_static(ERR_BACKEND_LOST));
            }
        }
    }
}

fn fnv(data: &[u8]) -> u64 {
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

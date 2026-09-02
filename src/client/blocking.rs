//! Blocking commands on exclusive backend connections.

use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::oneshot;

use super::pipe::{parse_redirect, recv_or_lost};
use super::session::Session;
use super::{Cold, ERR_NO_OWNER, Reply, Shared, error_frame};
use crate::backend::{ASKING_FRAME, Outbound, Sink};
use crate::command::Spec;
use crate::crc16;
use crate::resp;

impl Session {
    pub(super) fn forward_xread(&self, spec: &Spec, frame: Bytes, argc: usize) -> Option<Cold<'_>> {
        let Some((slot, blocking)) = xread_slot(&frame, argc) else {
            self.emit_error("ERR Unbalanced XREAD list of streams");
            return None;
        };
        if blocking {
            return self.block_at(slot, frame);
        }
        let seq = self.alloc_seq();
        self.send_single(seq, slot, spec.is_readonly(), frame)
    }

    pub(super) fn forward_blocking(
        &self,
        spec: &Spec,
        frame: Bytes,
        argc: usize,
    ) -> Option<Cold<'_>> {
        let Some(slot) = self.key_slot(&frame, argc, spec.first_key as usize) else {
            self.emit_error("ERR missing key");
            return None;
        };
        self.block_at(slot, frame)
    }

    fn block_at(&self, slot: u16, frame: Bytes) -> Option<Cold<'_>> {
        self.gated(slot, move |s| {
            s.spawn_blocking(slot, frame);
            None
        })
    }

    fn spawn_blocking(&self, slot: u16, frame: Bytes) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let task = tokio::task::spawn_local(async move {
            let reply = blocking_round(&shared, slot, frame, None, false).await;
            let _ = reply_q.send(Reply::At(seq, reply));
        });
        let mut blocking = self.blocking.borrow_mut();
        blocking.retain(|(_, t)| !t.is_finished());
        blocking.push((seq, task));
    }
}

// the first stream key's slot and whether BLOCK precedes STREAMS; None when unbalanced
fn xread_slot(frame: &Bytes, argc: usize) -> Option<(u16, bool)> {
    let mut blocking = false;
    let mut streams = None;
    for (i, a) in resp::Args::new(frame, argc).enumerate() {
        match streams {
            None if a.eq_ignore_ascii_case(b"streams") => streams = Some(i),
            None => blocking |= a.eq_ignore_ascii_case(b"block"),
            Some(p) => {
                return (argc - p - 1)
                    .is_multiple_of(2)
                    .then(|| (crc16::slot(a), blocking));
            }
        }
    }
    None
}

async fn blocking_round(
    shared: &Rc<Shared>,
    slot: u16,
    frame: Bytes,
    redirect: Option<(bool, &str)>,
    retried: bool,
) -> Bytes {
    let topo = shared.topo.load_full();
    let (addr, asking) = match redirect {
        Some((ask, target)) => (Some(target), ask),
        None => (
            topo.owner(slot)
                .map(|i| topo.nodes[i as usize].addr.as_str()),
            false,
        ),
    };
    let Some(addr) = addr else {
        return Bytes::from_static(ERR_NO_OWNER);
    };
    let Some(lease) = shared.backends.take_exclusive(addr) else {
        return error_frame("ERR too many blocking connections");
    };
    let (tx, rx) = oneshot::channel();
    lease
        .conn()
        .send(Outbound {
            head: asking.then(|| Bytes::from_static(ASKING_FRAME)),
            frame: frame.clone(),
            expect: if asking { 2 } else { 1 },
            sink: Sink::One(tx),
        })
        .await;
    let reply = recv_or_lost(rx).await;
    lease.complete();
    if !retried
        && reply.first() == Some(&b'-')
        && let Some(redir) = parse_redirect(&reply)
    {
        let _ = shared.refresh.send(());
        return Box::pin(blocking_round(shared, slot, frame, Some(redir), true)).await;
    }
    reply
}

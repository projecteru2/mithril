//! Subscriber sessions: the relay connection and the confirmation window.

use std::collections::HashSet;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use super::link::{WriterLink, mark_closed};
use super::queue::ReplyQueue;
use super::session::Session;
use super::{ERR_NO_OWNER, MAX_INFLIGHT, Reply, Shared};
use crate::backend::{ERR_BACKEND_LOST, ensure_read_room};
use crate::command::{self, Kind, Spec};
use crate::log_debug;
use crate::resp;

pub(super) const PUBSUB_PUSH_WINDOW: usize = 4096;
const SUBS_LIMIT: usize = 32768;
const PUBSUB_FORWARD_QUEUE: usize = 64;

pub(super) struct PubsubHandle {
    tx: mpsc::Sender<Bytes>,
    task: tokio::task::JoinHandle<()>,
}

// reader-side subscription mirror; confirmation counts derive from it
#[derive(Default)]
pub(super) struct PubsubSim {
    channels: HashSet<Vec<u8>>,
    patterns: HashSet<Vec<u8>>,
}

impl PubsubSim {
    fn apply(&mut self, spec: &Spec, frame: &Bytes, argc: usize) {
        let target = match spec.name {
            "psubscribe" | "punsubscribe" => &mut self.patterns,
            _ => &mut self.channels,
        };
        let names = resp::Args::new(frame, argc).skip(1);
        if matches!(spec.name, "subscribe" | "psubscribe") {
            target.extend(names.map(<[u8]>::to_vec));
        } else if argc > 1 {
            for name in names {
                target.remove(name);
            }
        } else {
            target.clear();
        }
    }

    fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty()
    }

    // acks per command: named channels, or the matching set for a bare unsubscribe
    fn ack_count(&self, spec: &Spec, argc: usize) -> usize {
        if argc > 1 {
            return argc - 1;
        }
        match spec.name {
            "unsubscribe" => self.channels.len().max(1),
            "punsubscribe" => self.patterns.len().max(1),
            _ => 1,
        }
    }
}

// an aborted relay must not detach a child blocked in write_all
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Session {
    // drains already-promised confirmations before dropping the relay
    pub(super) async fn exit_pubsub_if_done(&self) -> bool {
        if !self.relay_dead() {
            if !self.subs.borrow().is_empty() {
                return false;
            }
            self.drain_acks().await;
        }
        self.stop_pubsub();
        true
    }

    pub(super) async fn drain_acks(&self) {
        while !self.link.ack_seqs.borrow().is_empty() {
            if self.relay_dead() || self.link.closed.get() {
                return;
            }
            if !self.notified_or_closed(&self.link.acks_drained).await {
                return;
            }
        }
    }

    pub(super) fn stop_pubsub(&self) {
        self.has_relay.set(false);
        if let Some(ps) = self.pubsub.borrow_mut().take() {
            ps.task.abort();
        }
        *self.subs.borrow_mut() = PubsubSim::default();
        backfill_acks(&self.link, &self.reply_q);
    }

    pub(super) fn dispatch_pubsub(&self, spec: &Spec, frame: Bytes, argc: usize) {
        match spec.name {
            "quit" => {
                self.closing.set(true);
                self.emit_local(Bytes::from_static(resp::OK));
                return;
            }
            "reset" => {
                self.stop_pubsub();
                self.do_reset();
                self.emit_local(Bytes::from_static(b"+RESET\r\n"));
                return;
            }
            "ping" => {}
            _ if spec.kind == Kind::Subscribe => {}
            _ => {
                self.emit_error(&format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / \
                     PING / QUIT / RESET are allowed in this context",
                    spec.name
                ));
                return;
            }
        }
        let Some(acks) = self.pubsub_admit(spec, &frame, argc) else {
            self.emit_error("ERR pubsub confirmation backlog exceeds limit");
            return;
        };
        self.promise(spec, &frame, argc, acks);
        let sent = self
            .pubsub
            .borrow()
            .as_ref()
            .is_some_and(|ps| ps.tx.try_send(frame).is_ok());
        if !sent {
            // the backfilled confirmations answer this command; more would desync
            self.stop_pubsub();
        }
    }

    pub(super) fn enter_pubsub(&self, spec: &Spec, first_frame: Bytes, argc: usize) {
        let Some(acks) = self.pubsub_admit(spec, &first_frame, argc) else {
            self.emit_error("ERR pubsub confirmation backlog exceeds limit");
            return;
        };
        let Some(addr) = self.any_master_addr() else {
            self.emit_error_frame(Bytes::from_static(ERR_NO_OWNER));
            return;
        };
        self.promise(spec, &first_frame, argc, acks);
        self.has_relay.set(true);
        let (tx, rx) = mpsc::channel::<Bytes>(PUBSUB_FORWARD_QUEUE);
        let _ = tx.try_send(first_frame);
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let link = self.link.clone();
        let task = tokio::task::spawn_local(async move {
            pubsub_relay(shared, addr, rx, reply_q, link).await;
        });
        *self.pubsub.borrow_mut() = Some(PubsubHandle { tx, task });
    }

    fn relay_dead(&self) -> bool {
        self.pubsub
            .borrow()
            .as_ref()
            .is_none_or(|ps| ps.task.is_finished())
    }

    // promised confirmations occupy the reply window until emitted: bound them
    fn pubsub_admit(&self, spec: &Spec, frame: &Bytes, argc: usize) -> Option<usize> {
        let subs = self.subs.borrow();
        let acks = subs.ack_count(spec, argc);
        if self.outstanding() as usize + acks > MAX_INFLIGHT {
            return None;
        }
        let target = match spec.name {
            "subscribe" => &subs.channels,
            "psubscribe" => &subs.patterns,
            _ => return Some(acks),
        };
        let mut grown = subs.channels.len() + subs.patterns.len();
        let mut fresh: HashSet<&[u8]> = HashSet::new();
        for a in resp::Args::new(frame, argc).skip(1) {
            if !target.contains(a) && fresh.insert(a) {
                grown += 1;
            }
        }
        (grown <= SUBS_LIMIT).then_some(acks)
    }

    fn promise(&self, spec: &Spec, frame: &Bytes, argc: usize, acks: usize) {
        if spec.kind == Kind::Subscribe {
            self.subs.borrow_mut().apply(spec, frame, argc);
        }
        self.promise_acks(acks);
    }

    fn promise_acks(&self, n: usize) {
        let mut seqs = self.link.ack_seqs.borrow_mut();
        for _ in 0..n {
            seqs.push_back(self.alloc_seq());
        }
    }
}

pub(super) fn pubsub_allowed(spec: &Spec) -> bool {
    spec.flags & command::FLAG_PUBSUB != 0
}

async fn pubsub_relay(
    shared: Rc<Shared>,
    addr: String,
    mut rx: mpsc::Receiver<Bytes>,
    reply_q: Rc<ReplyQueue>,
    link: Rc<WriterLink>,
) {
    let stream = match crate::backend::dial_raw(&addr, &shared.cfg).await {
        Ok(s) => s,
        Err(e) => {
            log_debug!("pubsub dial {addr}: {e}");
            backfill_acks(&link, &reply_q);
            mark_closed(&link);
            let _ = reply_q.send(Reply::Close);
            return;
        }
    };
    let (mut read_half, mut write_half) = stream.into_split();
    let _writer = AbortOnDrop(tokio::task::spawn_local(async move {
        while let Some(frame) = rx.recv().await {
            if write_half.write_all(&frame).await.is_err() {
                return;
            }
        }
    }));
    let mut buf = BytesMut::with_capacity(crate::backend::READ_INIT);
    let mut cur = resp::Cursor::default();
    let mut last_ack: Option<u64> = None;
    'io: loop {
        loop {
            match resp::scan_value_at(&buf, &mut cur) {
                resp::Scan::Complete(len) => {
                    let frame = buf.split_to(len).freeze();
                    let popped = (!is_publication(&frame))
                        .then(|| link.ack_seqs.borrow_mut().pop_front())
                        .flatten();
                    let reply = match popped {
                        Some(seq) => {
                            link.acks_drained.notify_one();
                            last_ack = Some(seq);
                            Reply::Ack(seq, frame)
                        }
                        None => {
                            if !charge_push(&link).await {
                                return;
                            }
                            Reply::Push {
                                after: last_ack,
                                frame,
                            }
                        }
                    };
                    if reply_q.send(reply).is_err() {
                        break 'io;
                    }
                }
                resp::Scan::Invalid(_) => break 'io,
                resp::Scan::Incomplete => break,
            }
        }
        ensure_read_room(&mut buf);
        if matches!(read_half.read_buf(&mut buf).await, Ok(0) | Err(_)) {
            break;
        }
    }
    backfill_acks(&link, &reply_q);
    // an idle subscriber sends nothing: the parked reader needs a wakeup
    mark_closed(&link);
    let _ = reply_q.send(Reply::Close);
}

// a push holds one window slot from here until the writer emits it
async fn charge_push(link: &Rc<WriterLink>) -> bool {
    while link.oob_budget.get() >= PUBSUB_PUSH_WINDOW {
        if link.closed.get() {
            return false;
        }
        link.oob_notify.notified().await;
    }
    link.oob_budget.set(link.oob_budget.get() + 1);
    true
}

// promised confirmation sequences must resolve or the writer never drains past them
fn backfill_acks(link: &Rc<WriterLink>, reply_q: &Rc<ReplyQueue>) {
    while let Some(seq) = link.ack_seqs.borrow_mut().pop_front() {
        let _ = reply_q.send(Reply::Ack(seq, Bytes::from_static(ERR_BACKEND_LOST)));
    }
    link.acks_drained.notify_one();
}

// true for publications; every other pubsub frame consumes a promised sequence
fn is_publication(frame: &[u8]) -> bool {
    if frame.first() != Some(&b'*') {
        return false;
    }
    let Some((n, after)) = resp::scan_int_line(frame, 1) else {
        return false;
    };
    if n < 3 {
        return false;
    }
    let Some(Ok(b)) = resp::scan_bulk(frame, after) else {
        return false;
    };
    let kind = &frame[b.payload_start..b.payload_end];
    kind.eq_ignore_ascii_case(b"message") || kind.eq_ignore_ascii_case(b"pmessage")
}

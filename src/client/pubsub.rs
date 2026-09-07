//! Subscriber sessions: the relay connection and the confirmation window.

use std::collections::HashSet;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use super::link::{WriterLink, mark_closed};
use super::pipe::parse_redirect;
use super::queue::ReplyQueue;
use super::session::Session;
use super::{ERR_CROSSSLOT, ERR_NO_OWNER, MAX_INFLIGHT, Reply, Shared, error_frame};
use crate::backend::{ERR_BACKEND_LOST, ensure_read_room};
use crate::command::{self, Kind, Spec};
use crate::crc16;
use crate::log_debug;
use crate::resp;

pub(super) const PUBSUB_PUSH_WINDOW: usize = 4096;
const SUBS_LIMIT: usize = 32768;
const PUBSUB_FORWARD_QUEUE: usize = 64;
const ERR_SHARD_NODE: &str = "ERR shard channels of one connection must live on one node";

pub(super) struct PubsubHandle {
    tx: mpsc::Sender<Bytes>,
    task: tokio::task::JoinHandle<()>,
    // shard channels must live on this node, as on a direct connection
    addr: String,
}

/// One command awaiting its confirmations: what frames confirm it, by kind and by the channels
/// still unconfirmed (a bare unsubscribe's are every subscription of its kind, taken once the
/// commands before it have settled), so the server's own unsubscribes stay pushes; `reserved`
/// names the subscriptions it adds, each held against SUBS_LIMIT until that channel confirms.
pub(super) struct PendingSub {
    expect: &'static [u8],
    channels: Vec<Bytes>,
    remaining: usize,
    reserved: HashSet<Bytes>,
}

impl PendingSub {
    // an error answers any command; a pong (an array while subscribed, a simple string or the
    // echoed bulk when no subscription took) or a confirmation naming one of the command's
    // channels answers its own, so the server's own unsubscribes stay pushes
    fn confirmed_by(&mut self, frame: &[u8]) -> bool {
        if frame.first() == Some(&b'-') {
            return true;
        }
        if self.expect == b"pong" && matches!(frame.first(), Some(b'+' | b'$')) {
            return true;
        }
        let Some((kind, name)) = push_parts(frame) else {
            return false;
        };
        if !kind.eq_ignore_ascii_case(self.expect) {
            return false;
        }
        if self.channels.is_empty() {
            return true;
        }
        match self.channels.iter().position(|c| c.as_ref() == name) {
            Some(i) => {
                self.channels.swap_remove(i);
                true
            }
            None => false,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum SubKind {
    Channel,
    Pattern,
    Shard,
}

impl SubKind {
    fn of(name: &[u8]) -> Option<SubKind> {
        match name {
            b"subscribe" | b"unsubscribe" => Some(SubKind::Channel),
            b"psubscribe" | b"punsubscribe" => Some(SubKind::Pattern),
            b"ssubscribe" | b"sunsubscribe" => Some(SubKind::Shard),
            _ => None,
        }
    }
}

// the subscriptions the server has confirmed, and how many the pending subscribes still add
#[derive(Default)]
pub(super) struct PubsubSim {
    channels: HashSet<Vec<u8>>,
    patterns: HashSet<Vec<u8>>,
    shards: HashSet<Vec<u8>>,
    promised: usize,
}

impl PubsubSim {
    /// Subscribed channels, patterns and shard channels, as CLIENT INFO counts them.
    pub(super) fn counts(&self) -> (usize, usize, usize) {
        (self.channels.len(), self.patterns.len(), self.shards.len())
    }

    // a confirmation is the truth, on a promised sequence or not: a subscribe enters, an
    // unsubscribe (the server's own too, after a slot moved) leaves; returns whether a
    // subscription newly entered, so its reservation can be released
    fn confirm(&mut self, kind: &[u8], name: &[u8]) -> bool {
        let Some(sub) = SubKind::of(kind) else {
            return false;
        };
        let set = match sub {
            SubKind::Channel => &mut self.channels,
            SubKind::Pattern => &mut self.patterns,
            SubKind::Shard => &mut self.shards,
        };
        if kind.ends_with(b"unsubscribe") {
            set.remove(name);
            false
        } else {
            set.insert(name.to_vec())
        }
    }

    fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty() && self.shards.is_empty()
    }

    fn set(&self, kind: SubKind) -> &HashSet<Vec<u8>> {
        match kind {
            SubKind::Channel => &self.channels,
            SubKind::Pattern => &self.patterns,
            SubKind::Shard => &self.shards,
        }
    }
}

/// Whether a command in pubsub mode waits for the confirmations in flight: a bare
/// unsubscribe is sized by the settled subscriptions, QUIT and RESET must not backfill them.
pub(super) fn settles_first(spec: &Spec, argc: usize) -> bool {
    matches!(spec.name, "quit" | "reset") || (argc == 1 && spec.name.ends_with("unsubscribe"))
}

// an aborted relay must not detach a child blocked in write_all
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Session {
    // drops the relay once the settled subscriptions are gone; the caller has drained
    pub(super) fn exit_pubsub_if_done(&self) -> bool {
        if !self.relay_dead() && !self.link.subs.borrow().is_empty() {
            return false;
        }
        self.stop_pubsub();
        true
    }

    // a push the relay holds while its window is full is part of the state in flight
    pub(super) async fn drain_acks(&self) {
        while !self.link.ack_seqs.borrow().is_empty() || self.link.pushing.get() {
            if self.relay_dead() || self.link.closed.get() {
                return;
            }
            self.link.draining.set(true);
            let woke = self.notified_or_closed(&self.link.acks_drained).await;
            self.link.draining.set(false);
            if !woke {
                return;
            }
        }
    }

    pub(super) fn stop_pubsub(&self) {
        self.has_relay.set(false);
        if let Some(ps) = self.pubsub.borrow_mut().take() {
            ps.task.abort();
        }
        *self.link.subs.borrow_mut() = PubsubSim::default();
        self.link.pending_subs.borrow_mut().clear();
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
                if !self.do_reset() {
                    self.emit_local(Bytes::from_static(b"+RESET\r\n"));
                }
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
        if spec.name == "ssubscribe" {
            let relay = self.pubsub.borrow().as_ref().map(|ps| ps.addr.clone());
            match self.shard_owner(&frame, argc) {
                Err(err) => {
                    self.emit_error_frame(err);
                    return;
                }
                Ok(owner) if relay.is_some_and(|r| r != owner) => {
                    self.emit_error(ERR_SHARD_NODE);
                    return;
                }
                Ok(_) => {}
            }
        }
        if self.promise(spec, &frame, argc).is_none() {
            self.emit_error("ERR pubsub confirmation backlog exceeds limit");
            return;
        }
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
        let addr = if spec.name == "ssubscribe" {
            match self.shard_owner(&first_frame, argc) {
                Ok(addr) => Some(addr),
                Err(err) => {
                    self.emit_error_frame(err);
                    return;
                }
            }
        } else {
            self.any_master_addr()
        };
        let Some(addr) = addr else {
            self.emit_error_frame(Bytes::from_static(ERR_NO_OWNER));
            return;
        };
        if self.promise(spec, &first_frame, argc).is_none() {
            self.emit_error("ERR pubsub confirmation backlog exceeds limit");
            return;
        }
        self.has_relay.set(true);
        let (tx, rx) = mpsc::channel::<Bytes>(PUBSUB_FORWARD_QUEUE);
        let _ = tx.try_send(first_frame);
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let link = self.link.clone();
        let relay_addr = addr.clone();
        let task = tokio::task::spawn_local(async move {
            pubsub_relay(shared, relay_addr, rx, reply_q, link).await;
        });
        *self.pubsub.borrow_mut() = Some(PubsubHandle { tx, task, addr });
    }

    // the master owning the one slot every shard channel of the request hashes to
    fn shard_owner(&self, frame: &Bytes, argc: usize) -> Result<String, Bytes> {
        let mut channels = resp::Args::new(frame, argc).skip(1);
        let Some(first) = channels.next() else {
            return Err(error_frame(
                "ERR wrong number of arguments for 'ssubscribe' command",
            ));
        };
        let slot = crc16::slot(first);
        if channels.any(|c| crc16::slot(c) != slot) {
            return Err(Bytes::from_static(ERR_CROSSSLOT));
        }
        let topo = self.topo();
        topo.owner_addr(slot)
            .map(str::to_string)
            .ok_or_else(|| Bytes::from_static(ERR_NO_OWNER))
    }

    fn relay_dead(&self) -> bool {
        self.pubsub
            .borrow()
            .as_ref()
            .is_none_or(|ps| ps.task.is_finished())
    }

    // the reply window bounds the promised confirmations and SUBS_LIMIT the subscriptions
    // confirmed or promised, both before the command is queued
    fn promise(&self, spec: &Spec, frame: &Bytes, argc: usize) -> Option<()> {
        let subscribing = matches!(spec.name, "subscribe" | "psubscribe" | "ssubscribe");
        let kind = SubKind::of(spec.name.as_bytes());
        let mut subs = self.link.subs.borrow_mut();
        let channels: Vec<Bytes> = match kind {
            Some(kind) if !subscribing && argc == 1 => subs
                .set(kind)
                .iter()
                .map(|c| Bytes::copy_from_slice(c))
                .collect(),
            _ => resp::Args::new(frame, argc)
                .skip(1)
                .map(|n| frame.slice_ref(n))
                .collect(),
        };
        let acks = channels.len().max(1);
        if self.outstanding() as usize + acks > MAX_INFLIGHT {
            return None;
        }
        let mut reserved = HashSet::new();
        if let Some(kind) = kind
            && subscribing
        {
            let held = subs.set(kind);
            for c in &channels {
                if !held.contains(c.as_ref()) {
                    reserved.insert(c.clone());
                }
            }
            let (c, p, s) = subs.counts();
            if c + p + s + subs.promised + reserved.len() > SUBS_LIMIT {
                return None;
            }
            subs.promised += reserved.len();
        }
        self.link.pending_subs.borrow_mut().push_back(PendingSub {
            expect: if spec.name == "ping" {
                b"pong"
            } else {
                spec.name.as_bytes()
            },
            channels,
            remaining: acks,
            reserved,
        });
        self.promise_acks(acks);
        Some(())
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
                    let popped = confirms_pending(&link, &frame)
                        .then(|| link.ack_seqs.borrow_mut().pop_front())
                        .flatten();
                    let reply = match popped {
                        Some(seq) => {
                            // one error answers a whole command: its other promised
                            // sequences emit nothing
                            for _ in 0..reconcile_ack(&shared, &link, &frame) {
                                if let Some(s) = link.ack_seqs.borrow_mut().pop_front() {
                                    let _ = reply_q.send(Reply::Ack(s, Bytes::new()));
                                }
                            }
                            link.acks_drained.notify_one();
                            last_ack = Some(seq);
                            Reply::Ack(seq, frame)
                        }
                        None => {
                            link.pushing.set(true);
                            if !charge_push(&link).await {
                                return;
                            }
                            reconcile_push(&link, &frame);
                            Reply::Push {
                                after: last_ack,
                                frame,
                            }
                        }
                    };
                    if reply_q.send(reply).is_err() {
                        break 'io;
                    }
                    if link.pushing.replace(false) && link.draining.get() {
                        link.acks_drained.notify_one();
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

// a refused command (a redirect from stale topology, say) never reaches the confirmed set
// and retires every confirmation it still had; a redirect also asks for the topology it
// revealed; returns how many promised sequences are left to void
fn reconcile_ack(shared: &Shared, link: &WriterLink, frame: &[u8]) -> usize {
    if frame.first() == Some(&b'-') && parse_redirect(frame).is_some() {
        let _ = shared.refresh.send(());
    }
    absorb_ack(link, frame)
}

// the ack accounting a redirect refresh does not need: retire one promised sequence of the
// head command, settling a confirmed subscription (releasing its reservation) or voiding the
// rest on an error
fn absorb_ack(link: &WriterLink, frame: &[u8]) -> usize {
    let mut pending = link.pending_subs.borrow_mut();
    let Some(entry) = pending.front_mut() else {
        return 0;
    };
    entry.remaining -= 1;
    let mut void = 0;
    if frame.first() == Some(&b'-') {
        void = entry.remaining;
        entry.remaining = 0;
    } else if let Some((kind, name)) = push_parts(frame) {
        let mut subs = link.subs.borrow_mut();
        if subs.confirm(kind, name) && entry.reserved.remove(name) {
            subs.promised -= 1;
        }
    }
    if entry.remaining == 0 {
        link.subs.borrow_mut().promised -= entry.reserved.len();
        pending.pop_front();
    }
    void
}

fn reconcile_push(link: &WriterLink, frame: &[u8]) {
    if let Some((kind, name)) = push_parts(frame) {
        link.subs.borrow_mut().confirm(kind, name);
    }
}

// the kind and channel of a pubsub frame: its first two bulks (a subscribed PING's pong has
// only two elements)
fn push_parts(frame: &[u8]) -> Option<(&[u8], &[u8])> {
    if frame.first() != Some(&b'*') {
        return None;
    }
    let (n, after) = resp::scan_int_line(frame, 1)?;
    if n < 2 {
        return None;
    }
    let kind = resp::scan_bulk(frame, after)?.ok()?;
    let name = resp::scan_bulk(frame, kind.next)?.ok()?;
    Some((
        &frame[kind.payload_start..kind.payload_end],
        &frame[name.payload_start..name.payload_end],
    ))
}

// promised confirmation sequences must resolve or the writer never drains past them
fn backfill_acks(link: &Rc<WriterLink>, reply_q: &Rc<ReplyQueue>) {
    while let Some(seq) = link.ack_seqs.borrow_mut().pop_front() {
        let _ = reply_q.send(Reply::Ack(seq, Bytes::from_static(ERR_BACKEND_LOST)));
    }
    link.acks_drained.notify_one();
}

// a frame consumes a promised sequence only when it confirms the command at the head of the
// queue; publications and the server's own frames stay pushes
fn confirms_pending(link: &WriterLink, frame: &[u8]) -> bool {
    link.pending_subs
        .borrow_mut()
        .front_mut()
        .is_some_and(|p| p.confirmed_by(frame))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub_entry(reserved: &[&[u8]], acks: usize) -> PendingSub {
        PendingSub {
            expect: b"subscribe",
            channels: Vec::new(),
            remaining: acks,
            reserved: reserved.iter().map(|n| Bytes::copy_from_slice(n)).collect(),
        }
    }

    fn ack(link: &WriterLink, kind: &str, name: &[u8]) {
        let frame = format!(
            "*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:1\r\n",
            kind.len(),
            kind,
            name.len(),
            std::str::from_utf8(name).unwrap()
        );
        absorb_ack(link, frame.as_bytes());
    }

    #[test]
    fn reservations_release_as_confirmations_land() {
        let link = WriterLink::default();
        link.subs.borrow_mut().promised = 2;
        link.pending_subs
            .borrow_mut()
            .push_back(sub_entry(&[b"a", b"b"], 2));
        ack(&link, "subscribe", b"a");
        assert_eq!(link.subs.borrow().promised, 1);
        assert_eq!(link.subs.borrow().counts(), (1, 0, 0));
        ack(&link, "subscribe", b"b");
        assert_eq!(link.subs.borrow().promised, 0);
        assert_eq!(link.subs.borrow().counts(), (2, 0, 0));
        assert!(link.pending_subs.borrow().is_empty());
    }

    #[test]
    fn a_duplicate_argument_reserves_once_and_releases_the_remainder() {
        let link = WriterLink::default();
        link.subs.borrow_mut().promised = 1;
        link.pending_subs
            .borrow_mut()
            .push_back(sub_entry(&[b"a"], 2));
        ack(&link, "subscribe", b"a");
        ack(&link, "subscribe", b"a");
        assert_eq!(link.subs.borrow().promised, 0);
        assert_eq!(link.subs.borrow().counts(), (1, 0, 0));
        assert!(link.pending_subs.borrow().is_empty());
    }

    #[test]
    fn a_reconfirmed_channel_does_not_spend_a_neighbours_reservation() {
        let link = WriterLink::default();
        link.subs.borrow_mut().channels.insert(b"old".to_vec());
        link.subs.borrow_mut().promised = 1;
        let mut unsub = sub_entry(&[], 1);
        unsub.expect = b"unsubscribe";
        link.pending_subs.borrow_mut().push_back(unsub);
        link.pending_subs
            .borrow_mut()
            .push_back(sub_entry(&[b"new"], 2));
        ack(&link, "unsubscribe", b"old");
        assert_eq!(link.subs.borrow().counts(), (0, 0, 0));
        ack(&link, "subscribe", b"old");
        assert_eq!(link.subs.borrow().promised, 1);
        assert_eq!(link.subs.borrow().counts(), (1, 0, 0));
        ack(&link, "subscribe", b"new");
        assert_eq!(link.subs.borrow().promised, 0);
        assert_eq!(link.subs.borrow().counts(), (2, 0, 0));
        assert!(link.pending_subs.borrow().is_empty());
    }

    #[test]
    fn confirmations_settle_the_mirror() {
        let link = WriterLink::default();
        reconcile_push(&link, b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n");
        assert_eq!(link.subs.borrow().counts(), (1, 0, 0));
        reconcile_push(&link, b"*3\r\n$7\r\nmessage\r\n$1\r\na\r\n$2\r\nhi\r\n");
        reconcile_push(&link, b"*3\r\n$12\r\nsunsubscribe\r\n$1\r\na\r\n:0\r\n");
        assert_eq!(link.subs.borrow().counts(), (1, 0, 0));
        reconcile_push(&link, b"*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:0\r\n");
        assert_eq!(link.subs.borrow().counts(), (0, 0, 0));
    }
}

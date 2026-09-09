//! State a session's reader, writer and relay share: the in-flight ring, gates, protocol flips.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::Lane;
use super::pubsub::{PendingSub, PubsubSim};
use super::watch::{NO_WATCH, Watched};
use crate::cache::ReplyCache;
use crate::multikey;

pub(super) struct InFlight {
    pub(super) seq: u64,
    pub(super) frame: Bytes,
    pub(super) expect: u32,
    pub(super) retried: bool,
    pub(super) reloaded: bool,
    pub(super) degraded: bool,
    pub(super) waited: bool,
    pub(super) fill: Option<Fill>,
    pub(super) db: u8,
    // where a redirect sent the request, for a reload the topology cannot place yet
    pub(super) target: Option<Box<str>>,
}

// sequences are allocated monotonically, so the ring stays sorted
pub(super) type InflightRing = RefCell<VecDeque<InFlight>>;

// state shared between a session's reader, writer, and pubsub relay
#[derive(Default)]
pub(super) struct WriterLink {
    // the writer is parked on the client socket: the worker's in-flight count skips this session
    pub(super) writer_blocked: Cell<bool>,
    pub(super) inflight: InflightRing,
    pub(super) emitted: Cell<u64>,
    // set when no reply can ever be written again; reader must stop dispatching
    pub(super) closed: Cell<bool>,
    pub(super) closed_notify: Notify,
    pub(super) proto_switches: ProtoSwitchQueue,
    pub(super) oob_budget: Cell<usize>,
    pub(super) oob_notify: Notify,
    // pre-allocated sequences for pending pubsub confirmations, in order
    pub(super) ack_seqs: RefCell<VecDeque<u64>>,
    pub(super) acks_drained: Notify,
    pub(super) pushing: Cell<bool>,
    pub(super) draining: Cell<bool>,
    // in-flight fills; the reply path skips the ring search at zero
    pub(super) fills_armed: Cell<usize>,
    pub(super) next_seq: Cell<u64>,
    // the session sends through the process-wide shard pipes
    pub(super) sharded: Cell<bool>,
    // the subscription mirror and, per promised confirmation, what it stands for: the relay
    // rolls a refused subscription back and mirrors the server's own unsubscribes
    pub(super) subs: RefCell<PubsubSim>,
    pub(super) pending_subs: RefCell<VecDeque<PendingSub>>,
    pub(super) db: Cell<u8>,
    pub(super) fanouts: RefCell<FanoutGates>,
    // detached tasks answering at a sequence: blocking commands, WATCH arming, watched EXEC
    pub(super) blocking: RefCell<Vec<(u64, JoinHandle<()>)>>,
    // WATCH arming, EXEC and release tasks: answer through the watch, never backfilled
    pub(super) watch_tasks: RefCell<Vec<JoinHandle<()>>>,
    // arming WATCHes wait until every earlier reply of the session has been emitted
    pub(super) fence_waiters: Cell<u32>,
    pub(super) fence_notify: Notify,
    pub(super) watch: RefCell<Option<Rc<Watched>>>,
    // every generation still owing replies; drained when the session goes
    pub(super) watches: RefCell<Vec<Rc<Watched>>>,
    // how many generations are live: the dispatch fast check
    pub(super) generations: Cell<u32>,
    // the watched slot, NO_WATCH when none: the dispatch fast check
    pub(super) watch_slot: Cell<u16>,
    pub(super) fanouts_any: Cell<bool>,
    // slots seen migrating: their same-slot multi-key commands take the gated path
    migrating: RefCell<Vec<u16>>,
    migrating_any: Cell<bool>,
}

impl WriterLink {
    /// The newest generation of a slot in the session's database; a watch armed under
    /// another database (before a SELECT) no longer routes the session's commands.
    pub(super) fn generation_in_db(&self, slot: u16) -> Option<Rc<Watched>> {
        let db = self.db.get();
        let watches = self.watches.borrow();
        watches
            .iter()
            .rev()
            .find(|w| w.slot == slot && w.db == db)
            .cloned()
    }

    /// Drops a watch from the slot's routing and from the generations still owing replies.
    pub(super) fn forget_watch(&self, watched: &Watched) {
        let mine = self
            .watch
            .borrow()
            .as_ref()
            .is_some_and(|w| std::ptr::eq(&**w, watched));
        if mine {
            self.watch_slot.set(NO_WATCH);
            self.watch.borrow_mut().take();
        }
        self.watches
            .borrow_mut()
            .retain(|w| !std::ptr::eq(&**w, watched));
        self.generations.set(self.watches.borrow().len() as u32);
    }

    pub(super) fn lane(&self, id: u64) -> Lane {
        self.lane_with(id, self.db.get())
    }

    // the lane a request was issued under; a later SELECT must not redirect it elsewhere
    pub(super) fn lane_with(&self, id: u64, db: u8) -> Lane {
        Lane {
            id,
            sharded: self.sharded.get(),
            db,
        }
    }

    pub(super) fn is_migrating(&self, slot: u16) -> bool {
        self.migrating_any.get() && self.migrating.borrow().contains(&slot)
    }

    pub(super) fn mark_migrating(&self, slot: u16) {
        let mut slots = self.migrating.borrow_mut();
        if !slots.contains(&slot) {
            slots.push(slot);
        }
        self.migrating_any.set(true);
    }

    pub(super) fn retain_migrating(&self, keep: impl Fn(u16) -> bool) {
        if !self.migrating_any.get() {
            return;
        }
        let mut slots = self.migrating.borrow_mut();
        slots.retain(|&s| keep(s));
        self.migrating_any.set(!slots.is_empty());
    }

    pub(super) fn gate_slots(&self, slots: &[u16], gate: &Rc<Notify>) {
        let mut gates = self.fanouts.borrow_mut();
        for &slot in slots {
            gates.insert(slot, gate.clone());
        }
        self.fanouts_any.set(true);
    }

    // a drained registry gives back a burst's capacity, like ParkedRing
    pub(super) fn release_gates(&self, slots: &[u16]) {
        let mut gates = self.fanouts.borrow_mut();
        for slot in slots {
            gates.remove(slot);
        }
        if gates.is_empty() {
            self.fanouts_any.set(false);
            if gates.capacity() > 256 {
                *gates = FanoutGates::default();
            }
        }
    }

    pub(super) fn detach_fill(&self, entry: &mut InFlight) -> Option<Fill> {
        let fill = entry.fill.take()?;
        self.fills_armed.set(self.fills_armed.get() - 1);
        Some(fill)
    }
}

// pending protocol flips; `armed` keeps the hot path off the RefCell
#[derive(Default)]
pub(super) struct ProtoSwitchQueue {
    armed: Cell<usize>,
    queue: RefCell<VecDeque<(u64, u8)>>,
}

impl ProtoSwitchQueue {
    pub(super) fn push(&self, at: u64, proto: u8) {
        self.queue.borrow_mut().push_back((at, proto));
        self.armed.set(self.armed.get() + 1);
    }

    pub(super) fn apply(&self, seq: u64, cur: &mut u8) {
        if self.armed.get() == 0 {
            return;
        }
        let mut q = self.queue.borrow_mut();
        while let Some(&(at, p)) = q.front() {
            if at > seq {
                break;
            }
            *cur = p;
            q.pop_front();
            self.armed.set(self.armed.get() - 1);
        }
    }
}

// the keys a reply fills: one for GET, every key of an MGET
pub(super) enum Fill {
    One(Bytes),
    Many(Vec<Bytes>),
}

impl Fill {
    // arms once the route is known; None when nothing could be armed
    pub(super) fn arm(self, cache: &ReplyCache) -> Option<Fill> {
        match self {
            Fill::One(key) => cache.begin_fill(&key).then_some(Fill::One(key)),
            Fill::Many(keys) => cache.begin_fills(keys).map(Fill::Many),
        }
    }

    pub(super) fn complete(&self, cache: &ReplyCache, frame: &[u8]) {
        match self {
            Fill::One(key) => cache.complete_fill(key, frame),
            Fill::Many(keys) => cache.complete_fills(keys, frame),
        }
    }

    pub(super) fn abandon(&self, cache: &ReplyCache) {
        match self {
            Fill::One(key) => cache.abandon_fill(key),
            Fill::Many(keys) => cache.abandon_fills(keys),
        }
    }
}

// same-slot commands wait for a fan-out's first round so no retry overtakes its retries
type FanoutGates = HashMap<u16, Rc<Notify>, BuildHasherDefault<multikey::SlotHasher>>;

pub(super) fn mark_closed(link: &WriterLink) {
    link.closed.set(true);
    link.closed_notify.notify_one();
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn close_notification_survives_a_late_reader() {
        let link = WriterLink::default();
        mark_closed(&link);
        tokio::time::timeout(Duration::from_millis(10), link.closed_notify.notified())
            .await
            .unwrap();
        assert!(link.closed.get());
    }
}

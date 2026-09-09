//! WATCH: an exclusive connection to the slot's master holds the watched keys until EXEC and
//! carries every command of that slot meanwhile, so they stay ordered with EXEC and reads see
//! what EXEC will check. The reader never waits for it: requests queue until the connection is
//! armed, detached tasks do the waiting, the slot keeps routing to the connection until EXEC or
//! the release has answered, and the connection goes back to the pool only once everything it
//! accepted has answered, the slot gated meanwhile.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use super::fanout::write_keys;
use super::link::WriterLink;
use super::pipe::{ColdSend, Pipe, parse_redirect, queue_on, recv_or_lost};
use super::queue::ReplyQueue;
use super::session::Session;
use super::{
    Cold, ERR_CROSSSLOT, ERR_EXCLUSIVE_LIMIT, ERR_NO_OWNER, ERR_TRYAGAIN, Reply, Shared,
    error_frame,
};
use crate::backend::{Conn, ERR_BACKEND_LOST, ExclusiveLease, Outbound, Sink};
use crate::command::{Kind, Spec};
use crate::crc16;
use crate::resp;

const FENCE: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const UNWATCH: &[u8] = b"*1\r\n$7\r\nUNWATCH\r\n";
const NIL_EXEC: &[u8] = b"*-1\r\n";
pub(super) const NO_WATCH: u16 = u16::MAX;

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    /// The lease is being taken behind the session's earlier work.
    Arming,
    Armed,
    /// An EXEC or the release is on the connection; the slot still routes there until it answers.
    Finishing,
    /// The connection died or was never had; the slot answers lost until the client lets go.
    Lost,
}

enum Queued {
    Cmd {
        seq: u64,
        head: Option<Bytes>,
        frame: Bytes,
    },
    Exec {
        seq: u64,
        blob: Bytes,
        expect: u32,
    },
    Release {
        seq: u64,
        reply: Bytes,
    },
}

enum Settle {
    Exec(u64, oneshot::Receiver<Bytes>, u64),
    Release(Option<(oneshot::Receiver<Bytes>, u64)>),
}

/// The keys a session watches live on this connection until EXEC or a release.
pub(super) struct Watched {
    pub(super) slot: u16,
    // the database the watch was armed in; its lease selected it, so a later SELECT skips it
    pub(super) db: u8,
    state: Cell<State>,
    // FLUSHALL passed on other connections: the EXEC answers nil, as it would on one node
    dirty: Cell<bool>,
    // an EXEC was accepted: the watch is spent even before it runs
    exec_taken: Cell<bool>,
    // UNWATCH, DISCARD, RESET or a refused EXEC arrived: the watch no longer constrains
    released: Cell<bool>,
    // that command's reply, held until the connection is released
    release: RefCell<Option<(u64, Bytes)>>,
    lease: RefCell<Option<ExclusiveLease>>,
    queued: RefCell<VecDeque<Queued>>,
    // sequences a task will answer: the WATCH, sent EXECs
    owed: RefCell<Vec<u64>>,
    // requests put on the connection; a terminal reply is held until the connection is quiet
    // unless more already followed it
    accepted: Cell<u64>,
    held: RefCell<Vec<(u64, Bytes)>>,
}

impl Watched {
    /// Whether the watch still constrains the session: a released or finishing one only
    /// orders what is left on its connection.
    pub(super) fn guarding(&self) -> bool {
        !self.released.get() && !self.exec_taken.get() && self.state.get() != State::Finishing
    }

    pub(super) fn mark_dirty(&self) {
        self.dirty.set(true);
    }

    /// Answers everything this watch still owes; for a session that is gone.
    pub(super) fn fail_all(&self, reply_q: &ReplyQueue) {
        let err = Bytes::from_static(ERR_BACKEND_LOST);
        for q in self.queued.borrow_mut().drain(..) {
            let (seq, reply) = match q {
                Queued::Cmd { seq, .. } | Queued::Exec { seq, .. } => (seq, err.clone()),
                Queued::Release { seq, reply } => (seq, reply),
            };
            let _ = reply_q.send(Reply::At(seq, reply));
        }
        for seq in self.owed.borrow_mut().drain(..) {
            let _ = reply_q.send(Reply::At(seq, err.clone()));
        }
        self.answer_held(reply_q);
        self.answer_release(reply_q);
    }

    fn answer_held(&self, reply_q: &ReplyQueue) {
        for (seq, reply) in std::mem::take(&mut *self.held.borrow_mut()) {
            self.answer(reply_q, seq, reply);
        }
    }

    // a terminal reply goes out at once when the client already queued more behind it,
    // otherwise once the connection is quiet, so a following mixed-slot command never meets
    // the draining connection
    fn settle_terminal(&self, reply_q: &ReplyQueue, seq: u64, reply: Bytes, mark: u64) {
        if self.accepted.get() > mark {
            self.answer(reply_q, seq, reply);
        } else {
            self.owed.borrow_mut().retain(|&s| s != seq);
            self.held.borrow_mut().push((seq, reply));
        }
    }

    // the last settler of a generation closes it: an earlier one leaves what is still owed
    // to the one that answers it
    async fn close_out_when_last(&self, link: &WriterLink, reply_q: &ReplyQueue) {
        if self.owed.borrow().is_empty() {
            self.close_out(link, reply_q).await;
        }
    }

    fn conn(&self) -> Option<Rc<Conn>> {
        self.lease.borrow().as_ref().map(|l| l.conn().clone())
    }

    fn answer(&self, reply_q: &ReplyQueue, seq: u64, reply: Bytes) {
        self.owed.borrow_mut().retain(|&s| s != seq);
        let _ = reply_q.send(Reply::At(seq, reply));
    }

    fn answer_release(&self, reply_q: &ReplyQueue) {
        if let Some((seq, reply)) = self.release.borrow_mut().take() {
            let _ = reply_q.send(Reply::At(seq, reply));
        }
    }

    // the WATCH itself failed: the client knows there is no watch, as on a direct connection
    fn lose(&self, link: &WriterLink, reply_q: &ReplyQueue, seq: u64, err: Bytes) {
        self.die(reply_q, seq, err);
        link.forget_watch(self);
    }

    // the connection died under a live watch: the slot answers lost until the client lets go,
    // so EXEC never runs unguarded
    fn die(&self, reply_q: &ReplyQueue, seq: u64, err: Bytes) {
        self.state.set(State::Lost);
        self.lease.borrow_mut().take();
        self.owed.borrow_mut().retain(|&s| s != seq);
        self.fail_all(reply_q);
        let _ = reply_q.send(Reply::At(seq, err));
    }

    fn queue_cmd(
        &self,
        reply_q: &Rc<ReplyQueue>,
        seq: u64,
        head: Option<Bytes>,
        frame: Bytes,
    ) -> Option<Box<ColdSend>> {
        let Some(conn) = self.conn() else {
            let _ = reply_q.send(Reply::At(seq, Bytes::from_static(ERR_BACKEND_LOST)));
            return None;
        };
        let expect = 1 + u32::from(head.is_some());
        self.accepted.set(self.accepted.get() + 1);
        queue_on(&Pipe::Local(conn), reply_q, seq, head, frame, expect).unwrap_or_default()
    }

    // an EXEC or UNWATCH: what follows counts as its tail, from the returned mark
    fn push_terminal(&self, frame: Bytes, expect: u32) -> Option<(oneshot::Receiver<Bytes>, u64)> {
        let rx = self.push_frame(frame, expect)?;
        self.accepted.set(self.accepted.get() + 1);
        Some((rx, self.accepted.get()))
    }

    // goes out at once so later requests queue behind it; the reply is awaited apart
    fn push_frame(&self, frame: Bytes, expect: u32) -> Option<oneshot::Receiver<Bytes>> {
        let conn = self.conn()?;
        let (tx, rx) = oneshot::channel();
        let out = Outbound {
            head: None,
            frame,
            expect,
            sink: Sink::One(tx),
        };
        if let Err(out) = conn.try_send(out) {
            // detached deliberately: a full queue drains at the backend's pace
            tokio::task::spawn_local(async move { conn.send(out).await });
        }
        Some(rx)
    }

    // the slot keeps routing here until a PING sent behind everything accepted answers with
    // nothing accepted after it; the connection then returns to the pool, or is dropped when
    // the PING got no answer
    async fn close_out(&self, link: &WriterLink, reply_q: &ReplyQueue) {
        let alive = loop {
            let mark = self.accepted.get();
            let pong = match self.push_frame(Bytes::from_static(FENCE), 1) {
                Some(rx) => recv_or_lost(rx).await.as_ref() == resp::PONG,
                None => false,
            };
            if !pong || self.accepted.get() == mark {
                break pong;
            }
        };
        // an EXEC accepted meanwhile has its own settler, which closes out after it
        if !self.owed.borrow().is_empty() {
            return;
        }
        self.answer_held(reply_q);
        self.answer_release(reply_q);
        link.forget_watch(self);
        if let Some(lease) = self.lease.borrow_mut().take()
            && alive
        {
            lease.complete();
        }
        self.state.set(State::Lost);
    }
}

impl Session {
    pub(super) fn handle_watch(&self, spec: &Spec, frame: Bytes, argc: usize) -> Option<Cold<'_>> {
        if self.in_multi.get() {
            self.emit_error("ERR WATCH inside MULTI is not allowed");
            return None;
        }
        let mut keys = spec.keys(resp::Args::new(&frame, argc).skip(1), argc);
        let Some(first) = keys.next() else {
            self.emit_error("ERR wrong number of arguments for 'watch' command");
            return None;
        };
        let slot = crc16::slot(first);
        let held = self.guarding_slot().filter(|_| {
            self.link
                .watch
                .borrow()
                .as_ref()
                .is_some_and(|w| w.state.get() != State::Lost)
        });
        if keys.any(|k| crc16::slot(k) != slot) || held.is_some_and(|s| s != slot) {
            self.emit_error_frame(Bytes::from_static(ERR_CROSSSLOT));
            return None;
        }
        let seq = self.alloc_seq();
        let held = self.link.watch.borrow().clone();
        if let Some(w) = held {
            if w.state.get() == State::Lost {
                // a new WATCH replaces a watch whose connection died
                self.link.forget_watch(&w);
            } else if w.guarding() {
                let cold = self.queue_watched(&w, seq, None, frame)?;
                return Some(Box::pin(cold.flush()));
            }
        }
        let addr = {
            let topo = self.topo();
            topo.owner_addr(slot).map(str::to_owned)
        };
        let Some(addr) = addr else {
            self.emit_at(seq, Bytes::from_static(ERR_NO_OWNER));
            return None;
        };
        let db = self.link.db.get();
        let watched = Rc::new(Watched {
            slot,
            db,
            state: Cell::new(State::Arming),
            dirty: Cell::new(false),
            exec_taken: Cell::new(false),
            released: Cell::new(false),
            release: RefCell::new(None),
            lease: RefCell::new(None),
            queued: RefCell::new(VecDeque::new()),
            owed: RefCell::new(vec![seq]),
            accepted: Cell::new(0),
            held: RefCell::new(Vec::new()),
        });
        self.link.watch_slot.set(slot);
        *self.link.watch.borrow_mut() = Some(watched.clone());
        self.link.watches.borrow_mut().push(watched.clone());
        self.link
            .generations
            .set(self.link.watches.borrow().len() as u32);
        // earlier requests still run on other connections, blocking commands and redirect
        // retries included: WATCH takes effect once every one of them has answered
        let (shared, reply_q, link) =
            (self.shared.clone(), self.reply_q.clone(), self.link.clone());
        let task = tokio::task::spawn_local(async move {
            arm(&shared, &reply_q, &link, &watched, (seq, frame), (addr, db)).await;
        });
        track_task(&self.link, task);
        None
    }

    /// The slot a transaction must stay in while a watch constrains the session.
    pub(super) fn guarding_slot(&self) -> Option<u16> {
        let watch = self.link.watch.borrow();
        watch.as_ref().filter(|w| w.guarding()).map(|w| w.slot)
    }

    /// UNWATCH, DISCARD and RESET: the connection goes back after what it already accepted,
    /// and `reply` goes out once it has; false when there is no watch to release.
    pub(super) fn release_watch(&self, reply: Bytes) -> bool {
        let Some(w) = self.link.watch.borrow().clone() else {
            return false;
        };
        let seq = self.alloc_seq();
        self.release_at(w, seq, reply);
        true
    }

    // a SELECT that changes the database ends the watch (its lease is bound to the old one),
    // delivering the SELECT reply at its own sequence once the release lands
    pub(super) fn end_watch_at(&self, seq: u64, reply: Bytes) -> bool {
        let Some(w) = self.link.watch.borrow().clone() else {
            return false;
        };
        self.release_at(w, seq, reply);
        true
    }

    fn release_at(&self, w: Rc<Watched>, seq: u64, reply: Bytes) {
        match w.state.get() {
            State::Lost => {
                self.link.forget_watch(&w);
                self.emit_at(seq, reply);
            }
            _ if w.released.get() => self.emit_at(seq, reply),
            State::Arming => {
                w.released.set(true);
                w.queued
                    .borrow_mut()
                    .push_back(Queued::Release { seq, reply });
            }
            State::Finishing => {
                w.released.set(true);
                *w.release.borrow_mut() = Some((seq, reply));
            }
            State::Armed => {
                w.released.set(true);
                w.state.set(State::Finishing);
                *w.release.borrow_mut() = Some((seq, reply));
                let sent = w.push_terminal(Bytes::from_static(UNWATCH), 1);
                let (reply_q, link) = (self.reply_q.clone(), self.link.clone());
                let task = tokio::task::spawn_local(async move {
                    settle_release(&w, &link, &reply_q, sent).await;
                });
                track_task(&self.link, task);
            }
        }
    }

    /// The watch generation a request belongs to when every key lives in its slot
    /// (Some(w, true)); Some(w, false) when only some keys do, or the keys span two
    /// generations; None when no key is watched.
    pub(super) fn watched_keys(
        &self,
        spec: &Spec,
        frame: &Bytes,
        argc: usize,
    ) -> Option<(Rc<Watched>, bool)> {
        if !matches!(
            spec.kind,
            Kind::Single
                | Kind::MultiSum
                | Kind::Mget
                | Kind::Mset
                | Kind::Eval
                | Kind::Xread
                | Kind::Blocking
        ) {
            return None;
        }
        let (mut found, mut whole) = (None::<Rc<Watched>>, true);
        for key in spec.all_keys(resp::Args::new(frame, argc).skip(1), argc) {
            match self.link.generation_in_db(crc16::slot(key)) {
                Some(w) => match &found {
                    Some(f) if !Rc::ptr_eq(f, &w) => whole = false,
                    Some(_) => {}
                    None => found = Some(w),
                },
                None => whole = false,
            }
        }
        found.map(|w| (w, whole))
    }

    /// Queues a command of the watched slot on the watching connection, behind any fan-out
    /// still touching the slot.
    pub(super) fn serve_watched(
        &self,
        watched: Rc<Watched>,
        spec: &Spec,
        frame: Bytes,
        argc: usize,
    ) -> Option<Cold<'_>> {
        if spec.is_write()
            && let Some(cache) = self.cache()
        {
            write_keys(spec, &frame, argc, |k| cache.invalidate(k));
        }
        let seq = self.alloc_seq();
        // a known script is loaded ahead: the oneshot path has no NOSCRIPT reload
        let head = matches!(spec.name, "evalsha" | "evalsha_ro")
            .then(|| self.shared.scripts.load_frame(&frame))
            .flatten();
        if self.fanouts_pending() {
            let slot = watched.slot;
            return Some(Box::pin(async move {
                if !self.wait_fanouts(&[slot]).await {
                    self.closing.set(true);
                    return;
                }
                if let Some(cold) = self.queue_watched(&watched, seq, head, frame) {
                    cold.flush().await;
                }
            }));
        }
        let cold = self.queue_watched(&watched, seq, head, frame)?;
        Some(Box::pin(cold.flush()))
    }

    fn queue_watched(
        &self,
        watched: &Watched,
        seq: u64,
        head: Option<Bytes>,
        frame: Bytes,
    ) -> Option<Box<ColdSend>> {
        match watched.state.get() {
            State::Arming => {
                watched
                    .queued
                    .borrow_mut()
                    .push_back(Queued::Cmd { seq, head, frame });
                None
            }
            State::Armed | State::Finishing if watched.conn().is_some_and(|c| c.is_dead()) => {
                watched.die(&self.reply_q, seq, Bytes::from_static(ERR_BACKEND_LOST));
                None
            }
            State::Armed | State::Finishing => watched.queue_cmd(&self.reply_q, seq, head, frame),
            State::Lost => {
                self.emit_at(seq, Bytes::from_static(ERR_BACKEND_LOST));
                None
            }
        }
    }

    /// Runs a MULTI/EXEC blob on the watching connection; a detached task returns it to the pool.
    pub(super) fn exec_watched(&self, watched: Rc<Watched>, seq: u64, blob: Bytes, expect: u32) {
        watched.exec_taken.set(true);
        if watched.dirty.replace(false) && watched.state.get() != State::Lost {
            self.release_at(watched, seq, Bytes::from_static(NIL_EXEC));
            return;
        }
        match watched.state.get() {
            State::Arming => {
                watched
                    .queued
                    .borrow_mut()
                    .push_back(Queued::Exec { seq, blob, expect })
            }
            State::Lost => self.emit_at(seq, Bytes::from_static(ERR_BACKEND_LOST)),
            State::Armed | State::Finishing => {
                let Some((rx, mark)) = watched.push_terminal(blob, expect) else {
                    self.emit_at(seq, Bytes::from_static(ERR_BACKEND_LOST));
                    return;
                };
                watched.owed.borrow_mut().push(seq);
                watched.state.set(State::Finishing);
                let (shared, reply_q, link) =
                    (self.shared.clone(), self.reply_q.clone(), self.link.clone());
                let task = tokio::task::spawn_local(async move {
                    settle_exec(&shared, &watched, &link, &reply_q, (seq, rx, mark)).await;
                });
                track_task(&self.link, task);
            }
        }
    }
}

// finished tasks leave the list as new ones join, so a long session's list stays small
fn track_task(link: &WriterLink, task: JoinHandle<()>) {
    let mut tasks = link.watch_tasks.borrow_mut();
    tasks.retain(|t| !t.is_finished());
    tasks.push(task);
}

// waits until the writer has emitted every reply before the WATCH, takes the lease, sends
// WATCH, then drains what queued meanwhile
async fn arm(
    shared: &Rc<Shared>,
    reply_q: &Rc<ReplyQueue>,
    link: &WriterLink,
    watched: &Rc<Watched>,
    (seq, frame): (u64, Bytes),
    (addr, db): (String, u8),
) {
    link.fence_wait(seq).await;
    let Some(lease) = shared.backends.take_exclusive(&addr, db) else {
        watched.lose(link, reply_q, seq, error_frame(ERR_EXCLUSIVE_LIMIT));
        return;
    };
    let reply = on_conn(lease.conn(), frame, 1).await;
    if reply.first() == Some(&b'-') {
        watched.lose(link, reply_q, seq, unleaked(shared, reply));
        return;
    }
    *watched.lease.borrow_mut() = Some(lease);
    watched.answer(reply_q, seq, reply);
    let mut settles = Vec::new();
    // one at a time: what is still queued stays answerable if the session goes meanwhile
    loop {
        let next = watched.queued.borrow_mut().pop_front();
        let Some(q) = next else {
            break;
        };
        match q {
            Queued::Cmd { seq, head, frame } => {
                if let Some(cold) = watched.queue_cmd(reply_q, seq, head, frame) {
                    watched.owed.borrow_mut().push(seq);
                    cold.flush().await;
                    watched.owed.borrow_mut().retain(|&s| s != seq);
                }
            }
            Queued::Exec { seq, blob, expect } => match watched.push_terminal(blob, expect) {
                Some((rx, mark)) => {
                    watched.owed.borrow_mut().push(seq);
                    settles.push(Settle::Exec(seq, rx, mark));
                }
                None => {
                    let _ = reply_q.send(Reply::At(seq, Bytes::from_static(ERR_BACKEND_LOST)));
                }
            },
            Queued::Release { seq, reply } => {
                *watched.release.borrow_mut() = Some((seq, reply));
                let sent = watched.push_terminal(Bytes::from_static(UNWATCH), 1);
                settles.push(Settle::Release(sent));
            }
        }
    }
    if settles.is_empty() {
        watched.state.set(State::Armed);
        return;
    }
    watched.state.set(State::Finishing);
    for settle in settles {
        match settle {
            Settle::Exec(seq, rx, mark) => {
                deliver_exec(shared, watched, reply_q, (seq, rx, mark)).await;
            }
            Settle::Release(sent) => {
                if unwatched(watched, sent).await {
                    watched.answer_release(reply_q);
                }
            }
        }
    }
    watched.close_out_when_last(link, reply_q).await;
}

// true when the client queued more behind the UNWATCH: its reply need not wait for quiet
async fn unwatched(watched: &Watched, sent: Option<(oneshot::Receiver<Bytes>, u64)>) -> bool {
    let Some((rx, mark)) = sent else {
        return false;
    };
    recv_or_lost(rx).await;
    watched.accepted.get() > mark
}

// a wait that cannot miss the notification between the check and the sleep
pub(super) async fn settled(notify: &Notify, pending: impl Fn() -> bool) {
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !pending() {
            return;
        }
        notified.await;
    }
}

async fn deliver_exec(
    shared: &Shared,
    watched: &Watched,
    reply_q: &ReplyQueue,
    (seq, rx, mark): (u64, oneshot::Receiver<Bytes>, u64),
) {
    let reply = unleaked(shared, recv_or_lost(rx).await);
    watched.settle_terminal(reply_q, seq, reply, mark);
}

async fn settle_exec(
    shared: &Shared,
    watched: &Watched,
    link: &WriterLink,
    reply_q: &ReplyQueue,
    sent: (u64, oneshot::Receiver<Bytes>, u64),
) {
    deliver_exec(shared, watched, reply_q, sent).await;
    watched.close_out_when_last(link, reply_q).await;
}

async fn settle_release(
    watched: &Watched,
    link: &WriterLink,
    reply_q: &ReplyQueue,
    sent: Option<(oneshot::Receiver<Bytes>, u64)>,
) {
    if unwatched(watched, sent).await {
        watched.answer_release(reply_q);
    }
    watched.close_out_when_last(link, reply_q).await;
}

async fn on_conn(conn: &Conn, frame: Bytes, expect: u32) -> Bytes {
    let (tx, rx) = oneshot::channel();
    conn.send(Outbound {
        head: None,
        frame,
        expect,
        sink: Sink::One(tx),
    })
    .await;
    recv_or_lost(rx).await
}

// clients believe the proxy owns every slot: a redirect becomes a retry request, and the
// topology it reveals is refreshed so the retry lands
fn unleaked(shared: &Shared, reply: Bytes) -> Bytes {
    if parse_redirect(&reply).is_some() {
        let _ = shared.refresh.send(());
        Bytes::from_static(ERR_TRYAGAIN)
    } else {
        reply
    }
}

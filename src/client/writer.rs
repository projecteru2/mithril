//! The reply writer: ordered emission, redirects and degrades, cache fills.

use std::cell::RefMut;
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Notify, oneshot};

use super::fanout::{Singles, multikey_plan, request_slot, resend_singles, write_keys};
use super::link::{Fill, Hop, InFlight, InflightRing, WriterLink, mark_closed};
use super::pipe::{parse_redirect, pipe_for, queue_on, recv_or_lost, scatter_one, stage_expect};
use super::pubsub::PUBSUB_PUSH_WINDOW;
use super::queue::ReplyQueue;
use super::scripting::evalsha_target;
use super::{ERR_TRYAGAIN, Lane, Reply, Shared};
use crate::backend::{ASKING_FRAME, BATCH, ERR_BACKEND_LOST, write_frames};
use crate::cache::CACHING_REFUSED;
use crate::resp;
use crate::stats;

// the servers' own sentence; a script that returns a NOSCRIPT error of its own passes through
const NOSCRIPT: &[u8] = b"-NOSCRIPT No matching script";

// a request taken back from the ring: frame, replies still expected, its cache ticket
type Retry = (Bytes, u32, Option<Fill>);

// an atomic slot migration's handoff leaves the two nodes briefly disagreeing on the owner: a
// request redirected again waits between its next hops, doubling from here
pub(super) const REDIRECT_WAIT: Duration = Duration::from_millis(2);
pub(super) const REDIRECT_HOPS: u32 = 6;

// out-of-order replies by sequence distance; the back slot is always Some
#[derive(Default)]
struct ParkedRing {
    base: u64,
    slots: VecDeque<Option<(Bytes, bool)>>,
}

impl ParkedRing {
    fn put(&mut self, seq: u64, frame: Bytes, ack: bool) {
        if self.slots.is_empty() {
            self.base = seq;
        } else if seq < self.base {
            self.slots.reserve((self.base - seq) as usize);
            for _ in seq..self.base {
                self.slots.push_front(None);
            }
            self.base = seq;
        }
        let idx = (seq - self.base) as usize;
        if idx >= self.slots.len() {
            self.slots.resize(idx + 1, None);
        }
        self.slots[idx] = Some((frame, ack));
    }

    fn take(&mut self, seq: u64) -> Option<(Bytes, bool)> {
        while self.base < seq && !self.slots.is_empty() {
            self.slots.pop_front();
            self.base += 1;
        }
        if self.base != seq {
            return None;
        }
        let frame = self.slots.front_mut()?.take()?;
        self.slots.pop_front();
        self.base += 1;
        // a drained ring must not retain a large excursion's capacity for the connection's life
        if self.slots.is_empty() && self.slots.capacity() > 1024 {
            self.slots = VecDeque::new();
        }
        Some(frame)
    }

    fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

/// Where a request taken back from the ring goes next.
struct Resend<'a> {
    shared: &'a Rc<Shared>,
    reply_q: &'a Rc<ReplyQueue>,
    link: &'a WriterLink,
    client_id: u64,
}

impl Resend<'_> {
    // the retry carries no CACHING opt-in: it cannot fill; a lost shard answers the client
    async fn requeue(
        &self,
        target: &str,
        seq: u64,
        head: Option<Bytes>,
        (req, base_expect, fill): Retry,
        extra: u32,
        db: u8,
    ) {
        if let Some(fill) = fill
            && let Some(cache) = &self.shared.cache
        {
            fill.abandon(cache);
        }
        let pipe = pipe_for(
            self.shared,
            target,
            self.link.lane_with(self.client_id, db),
            false,
        );
        match queue_on(&pipe, self.reply_q, seq, head, req, base_expect + extra) {
            Ok(Some(cold)) => cold.flush().await,
            Ok(None) => {}
            Err(()) => {
                let _ = self
                    .reply_q
                    .send(Reply::At(seq, Bytes::from_static(ERR_BACKEND_LOST)));
            }
        }
    }
}

struct ExitBump<'a> {
    shared: &'a Shared,
    link: &'a WriterLink,
    reply: &'a ReplyQueue,
}

impl Drop for ExitBump<'_> {
    fn drop(&mut self) {
        self.reply.close();
        mark_closed(self.link);
        if let Some(cache) = &self.shared.cache {
            for e in self.link.inflight.borrow_mut().iter_mut() {
                if let Some(fill) = e.fill.take() {
                    fill.abandon(cache);
                }
            }
        }
        stats::bump(&self.shared.wstats.writers_exited);
        self.link.oob_notify.notify_waiters();
    }
}

pub(super) async fn write_loop(
    shared: Rc<Shared>,
    mut write_half: OwnedWriteHalf,
    reply_q: Rc<ReplyQueue>,
    mut close_rx: oneshot::Receiver<u64>,
    link: Rc<WriterLink>,
    client_id: u64,
) {
    let _exit = ExitBump {
        shared: &shared,
        link: &link,
        reply: &reply_q,
    };
    let mut next_emit: u64 = 0;
    let mut swept_to: u64 = 0;
    // protocol flips apply at the HELLO reply's sequence, not before
    let mut cur_proto: u8 = 2;
    // reader's final sequence; draining to it lets a departed client close
    let mut close_at: Option<u64> = None;
    let mut close_now = false;
    let mut parked = ParkedRing::default();
    let mut held_pushes: VecDeque<(u64, Bytes)> = VecDeque::new();
    let mut batch: Vec<Reply> = Vec::with_capacity(BATCH);
    let mut ready: Vec<Bytes> = Vec::with_capacity(BATCH);
    loop {
        if let Some(n) = close_at
            && next_emit >= n
            && parked.is_empty()
            && held_pushes.is_empty()
        {
            return;
        }
        tokio::select! {
            _ = reply_q.recv_batch(&mut batch, BATCH) => {}
            r = &mut close_rx, if close_at.is_none() => {
                match r {
                    Ok(n) => {
                        close_at = Some(n);
                        continue;
                    }
                    Err(_) => return,
                }
            }
        }
        // one yield lets already-delivered replies join the same flush
        for pass in 0..2 {
            for reply in batch.drain(..) {
                let (seq, mut frame) = match reply {
                    Reply::Close => {
                        close_now = true;
                        continue;
                    }
                    // a push never overtakes the confirmation it followed
                    Reply::Push { after, frame } => {
                        match after {
                            Some(a) if a >= next_emit => held_pushes.push_back((a, frame)),
                            _ => emit_push(&link, &mut ready, frame, cur_proto),
                        }
                        continue;
                    }
                    Reply::Ack(seq, frame) => {
                        if seq >= next_emit {
                            if frame.first() == Some(&b'-') {
                                stats::bump(&shared.wstats.errors);
                            }
                            parked.put(seq, frame, true);
                        }
                        continue;
                    }
                    Reply::At(seq, frame) => (seq, frame),
                };
                if seq < next_emit {
                    continue;
                }
                if frame.first() == Some(&b'-') {
                    if let Some((ask, target)) = parse_redirect(&frame) {
                        if let Some((retry, db)) = take_retry(&link, seq, ask, target) {
                            stats::bump(&shared.wstats.redirects);
                            let _ = shared.refresh.send(());
                            let head = ask.then(|| Bytes::from_static(ASKING_FRAME));
                            let resend = Resend {
                                shared: &shared,
                                reply_q: &reply_q,
                                link: &link,
                                client_id,
                            };
                            resend
                                .requeue(target, seq, head, retry, u32::from(ask), db)
                                .await;
                            continue;
                        }
                        if let Some((retry, slot, db)) = take_bounce(&link, seq) {
                            ride_out(
                                &shared,
                                &reply_q,
                                &link,
                                (client_id, seq, slot, db),
                                frame,
                                retry,
                            );
                            continue;
                        }
                        // clients believe the proxy owns every slot: never leak redirects
                        let _ = shared.refresh.send(());
                        frame = Bytes::from_static(ERR_TRYAGAIN);
                    } else if frame.starts_with(CACHING_REFUSED)
                        && let Some((retry, slot, db)) = take_bounce(&link, seq)
                    {
                        // the connection re-tracks itself; the request runs again without the opt-in
                        let topo = shared.topo.load_full();
                        if let Some(idx) = topo.owner(slot) {
                            let resend = Resend {
                                shared: &shared,
                                reply_q: &reply_q,
                                link: &link,
                                client_id,
                            };
                            resend
                                .requeue(&topo.nodes[idx as usize].addr, seq, None, retry, 0, db)
                                .await;
                            continue;
                        }
                        frame = Bytes::from_static(ERR_TRYAGAIN);
                    } else if frame.starts_with(NOSCRIPT)
                        && rerunnable(&link, seq)
                        && let Some((retry, db, target)) = take_reload(&link, seq)
                    {
                        let topo = shared.topo.load_full();
                        let (asked, target) = match &target {
                            Some((asked, t)) => (*asked, Some(t.as_ref())),
                            None => (false, evalsha_target(&topo, &retry.0)),
                        };
                        if let (Some(load), Some(target)) =
                            (shared.scripts.load_frame(&retry.0), target)
                        {
                            let (head, replies) = reload_head(load, asked);
                            let resend = Resend {
                                shared: &shared,
                                reply_q: &reply_q,
                                link: &link,
                                client_id,
                            };
                            resend
                                .requeue(target, seq, Some(head), retry, replies, db)
                                .await;
                            continue;
                        }
                    } else if frame.starts_with(b"-TRYAGAIN")
                        && let Some((req, fill, db)) = take_degrade(&link, seq)
                    {
                        // the singles carry no CACHING opt-in: they cannot fill
                        if let Some(fill) = fill
                            && let Some(cache) = &shared.cache
                        {
                            fill.abandon(cache);
                        }
                        if let Some(plan) = multikey_plan(&req) {
                            let (merge, nkeys, slot) = (plan.merge, plan.nkeys, plan.slot);
                            link.mark_migrating(slot);
                            // otherwise the client retries and this slot takes the gated path
                            if rerunnable(&link, seq) {
                                let shared = shared.clone();
                                let lane = link.lane_with(client_id, db);
                                ride(&link, &reply_q, seq, slot, async move {
                                    // an atomic migration hands the slot over whole: ride it out
                                    // first; split only if the keys really sit on two nodes
                                    let (whole, _) = follow_slot(
                                        &shared,
                                        lane,
                                        slot,
                                        frame,
                                        (req.clone(), 1, None),
                                    )
                                    .await;
                                    let reply = if whole.starts_with(b"-TRYAGAIN") {
                                        let mut singles = Singles::new(merge);
                                        resend_singles(
                                            &shared,
                                            lane,
                                            &req,
                                            nkeys,
                                            0..nkeys,
                                            &mut singles,
                                        )
                                        .await;
                                        singles.merge(nkeys, &[])
                                    } else if parse_redirect(&whole).is_some() {
                                        Bytes::from_static(ERR_TRYAGAIN)
                                    } else {
                                        whole
                                    };
                                    // a fill raced by the late writes goes before the gate lets
                                    // the client read again
                                    if plan.spec.is_write()
                                        && db == 0
                                        && let Some(cache) = &shared.cache
                                    {
                                        write_keys(plan.spec, &req, plan.argc, |k| {
                                            cache.invalidate(k)
                                        });
                                    }
                                    reply
                                });
                                continue;
                            }
                        }
                    }
                    stats::bump(&shared.wstats.errors);
                }
                if link.fills_armed.get() > 0
                    && let Some(fill) = take_fill(&link, seq)
                    && let Some(cache) = &shared.cache
                {
                    fill.complete(cache, &frame);
                }
                if seq == next_emit {
                    link.proto_switches.apply(next_emit, &mut cur_proto);
                    ready.push(convert_nil(frame, cur_proto));
                    next_emit += 1;
                } else {
                    parked.put(seq, frame, false);
                }
            }
            loop {
                if let Some(&(barrier, _)) = held_pushes.front()
                    && barrier < next_emit
                {
                    if let Some((_, frame)) = held_pushes.pop_front() {
                        emit_push(&link, &mut ready, frame, cur_proto);
                    }
                    continue;
                }
                let Some((frame, ack)) = parked.take(next_emit) else {
                    break;
                };
                link.proto_switches.apply(next_emit, &mut cur_proto);
                if ack {
                    push_pubsub_frame(&mut ready, frame, cur_proto);
                } else {
                    ready.push(convert_nil(frame, cur_proto));
                }
                next_emit += 1;
            }
            if pass == 0 {
                if ready.len() < 2 || close_now {
                    break;
                }
                tokio::task::yield_now().await;
                reply_q.pop_into(&mut batch, BATCH);
                if batch.is_empty() {
                    break;
                }
            }
        }
        if next_emit > swept_to {
            let mut inf = link.inflight.borrow_mut();
            while inf.front().is_some_and(|e| e.seq < next_emit) {
                let Some(e) = inf.pop_front() else {
                    break;
                };
                if let Some(fill) = e.fill
                    && let Some(cache) = &shared.cache
                {
                    link.fills_armed.set(link.fills_armed.get() - 1);
                    fill.abandon(cache);
                }
            }
            drop(inf);
            if link.timings_pending.get() {
                let mut timings = link.timings.borrow_mut();
                while timings.front().is_some_and(|t| t.0 < next_emit) {
                    if let Some((_, started_us, frame)) = timings.pop_front() {
                        shared.stats.log_slow(client_id, started_us, frame);
                    }
                }
                if timings.is_empty() {
                    link.timings_pending.set(false);
                    if timings.capacity() > 256 {
                        *timings = VecDeque::new();
                    }
                }
            }
            swept_to = next_emit;
        }
        if !ready.is_empty() {
            let total: usize = ready.iter().map(Bytes::len).sum();
            let held = link.next_seq.get().saturating_sub(link.emitted.get());
            shared
                .inflight
                .set(shared.inflight.get().saturating_sub(held));
            link.writer_blocked.set(true);
            let written = write_frames(&mut write_half, &ready).await;
            link.writer_blocked.set(false);
            let held = link.next_seq.get().saturating_sub(link.emitted.get());
            shared.inflight.set(shared.inflight.get() + held);
            if written.is_err() {
                return;
            }
            stats::add(&shared.wstats.bytes_out, total as u64);
            ready.clear();
        }
        shared.inflight.set(
            shared
                .inflight
                .get()
                .saturating_sub(next_emit.saturating_sub(link.emitted.get())),
        );
        link.emitted.set(next_emit);
        if link.fence_waiters.get() > 0 {
            link.fence_notify.notify_waiters();
        }
        if close_now {
            return;
        }
    }
}

fn entry_at(inflight: &InflightRing, seq: u64) -> Option<RefMut<'_, InFlight>> {
    RefMut::filter_map(inflight.borrow_mut(), |inf| {
        let idx = inf.binary_search_by_key(&seq, |e| e.seq).ok()?;
        inf.get_mut(idx)
    })
    .ok()
}

fn take_fill(link: &WriterLink, seq: u64) -> Option<Fill> {
    let mut entry = entry_at(&link.inflight, seq)?;
    link.detach_fill(&mut entry)
}

// a request may run again only while no later command of the session holds a sequence
fn rerunnable(link: &WriterLink, seq: u64) -> bool {
    seq + 1 == link.next_seq.get()
}

// one script reload per request, at the node a redirect sent it to when one did
fn take_reload(link: &WriterLink, seq: u64) -> Option<(Retry, u8, Option<Hop>)> {
    let mut entry = entry_at(&link.inflight, seq)?;
    if entry.reloaded {
        return None;
    }
    entry.reloaded = true;
    let db = entry.db;
    let fill = link.detach_fill(&mut entry);
    let target = entry.target.take();
    Some(((entry.frame.clone(), entry.expect, fill), db, target))
}

// retryable redirects: single-reply requests always, multi-reply blobs only for MOVED
fn take_retry(link: &WriterLink, seq: u64, ask: bool, target: &str) -> Option<(Retry, u8)> {
    let mut entry = entry_at(&link.inflight, seq)?;
    if entry.retried || (entry.expect > 1 && ask) {
        return None;
    }
    entry.retried = true;
    entry.target = Some((ask, Box::from(target)));
    let db = entry.db;
    let fill = link.detach_fill(&mut entry);
    Some(((entry.frame.clone(), entry.expect, fill), db))
}

// a request that must wait a topology change out takes one ride, while nothing later holds a sequence
fn take_bounce(link: &WriterLink, seq: u64) -> Option<(Retry, u16, u8)> {
    if !rerunnable(link, seq) {
        return None;
    }
    let mut entry = entry_at(&link.inflight, seq)?;
    if entry.waited || entry.expect != 1 {
        return None;
    }
    let slot = request_slot(&entry.frame)?;
    entry.waited = true;
    let db = entry.db;
    let fill = link.detach_fill(&mut entry);
    Some(((entry.frame.clone(), entry.expect, fill), slot, db))
}

// rides a topology change out: the request follows the slot, a script it needs is reloaded
// where it landed, and a redirect never reaches the client
fn ride_out(
    shared: &Rc<Shared>,
    reply_q: &Rc<ReplyQueue>,
    link: &Rc<WriterLink>,
    (client_id, seq, slot, db): (u64, u64, u16, u8),
    reply: Bytes,
    retry: Retry,
) {
    let shared = shared.clone();
    let lane = link.lane_with(client_id, db);
    ride(link, reply_q, seq, slot, async move {
        let req = retry.0.clone();
        let (mut reply, last) = follow_slot(&shared, lane, slot, reply, retry).await;
        if reply.starts_with(NOSCRIPT)
            && let (Some(load), Some((asked, target))) = (shared.scripts.load_frame(&req), last)
        {
            let (head, replies) = reload_head(load, asked);
            let pipe = pipe_for(&shared, &target, lane, false);
            let (staged, rx) = stage_expect(&pipe, Some(head), req, 1 + replies);
            staged.send().await;
            reply = recv_or_lost(rx).await;
        }
        if parse_redirect(&reply).is_some() {
            reply = Bytes::from_static(ERR_TRYAGAIN);
        }
        reply
    });
}

// runs `work` on a detached task gated on `slot`: its answer lands at `seq`, later commands
// of the slot wait for it, and teardown aborts it like a blocking command
fn ride(
    link: &Rc<WriterLink>,
    reply_q: &Rc<ReplyQueue>,
    seq: u64,
    slot: u16,
    work: impl Future<Output = Bytes> + 'static,
) {
    let gate = Rc::new(Notify::new());
    link.gate_slots(&[slot], &gate);
    let task = {
        let (link, reply_q) = (link.clone(), reply_q.clone());
        tokio::task::spawn_local(async move {
            let reply = work.await;
            link.release_gates(&[slot]);
            gate.notify_waiters();
            let _ = reply_q.send(Reply::At(seq, reply));
        })
    };
    link.track(seq, task);
}

// what a reload sends ahead of the rerun on the same pipe, and how many replies that adds:
// the script, then ASKING when the rerun must carry it
fn reload_head(load: Bytes, asked: bool) -> (Bytes, u32) {
    if !asked {
        return (load, 1);
    }
    (Bytes::from([load.as_ref(), ASKING_FRAME].concat()), 2)
}

// each hop waits, then goes where the reply points (a redirect's target) or where the slot
// lives now (TRYAGAIN under an atomic migration); the first real answer stands, with the
// hop that got it, and an error outlasting the hops is the caller's to judge
async fn follow_slot(
    shared: &Rc<Shared>,
    lane: Lane,
    slot: u16,
    mut reply: Bytes,
    (req, _, fill): Retry,
) -> (Bytes, Option<Hop>) {
    if let Some(fill) = fill
        && let Some(cache) = &shared.cache
    {
        fill.abandon(cache);
    }
    let mut last = None;
    let mut wait = REDIRECT_WAIT;
    for _ in 0..REDIRECT_HOPS {
        let (ask, target): Hop = match parse_redirect(&reply) {
            Some((ask, target)) => (ask, Box::from(target)),
            None if reply.starts_with(b"-TRYAGAIN") => match shared.topo.load().owner_addr(slot) {
                Some(addr) => (false, Box::from(addr)),
                None => break,
            },
            None => break,
        };
        stats::bump(&shared.wstats.redirect_waits);
        tokio::time::sleep(wait).await;
        wait *= 2;
        let head = ask.then(|| Bytes::from_static(ASKING_FRAME));
        let rx = scatter_one(shared, &target, lane, head, req.clone()).await;
        reply = recv_or_lost(rx).await;
        last = Some((ask, target));
    }
    (reply, last)
}

// one key-by-key resend per request, whether or not a redirect retry preceded it
fn take_degrade(link: &WriterLink, seq: u64) -> Option<(Bytes, Option<Fill>, u8)> {
    let mut entry = entry_at(&link.inflight, seq)?;
    if entry.degraded || entry.expect > 1 {
        return None;
    }
    // a redirect merged out of the resend must not re-run the whole request
    entry.degraded = true;
    entry.retried = true;
    let db = entry.db;
    let fill = link.detach_fill(&mut entry);
    Some((entry.frame.clone(), fill, db))
}

// the single window-release site: a push frees its slot on emission
fn emit_push(link: &WriterLink, ready: &mut Vec<Bytes>, frame: Bytes, proto: u8) {
    let left = link.oob_budget.get().saturating_sub(1);
    link.oob_budget.set(left);
    if left == PUBSUB_PUSH_WINDOW - 1 {
        link.oob_notify.notify_waiters();
    }
    push_pubsub_frame(ready, frame, proto);
}

// RESP3 push conversion: the leading '*' becomes '>' via a two-segment write
fn push_pubsub_frame(ready: &mut Vec<Bytes>, frame: Bytes, proto: u8) {
    if proto >= 3 && frame.first() == Some(&b'*') {
        ready.push(Bytes::from_static(b">"));
        ready.push(frame.slice(1..));
    } else {
        ready.push(frame);
    }
}

fn convert_nil(frame: Bytes, proto: u8) -> Bytes {
    if proto >= 3 && (frame.as_ref() == resp::NIL_BULK || frame.as_ref() == resp::NIL_ARRAY) {
        Bytes::from_static(resp::NIL_RESP3)
    } else {
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parked_ring_orders_sparse_sequences() {
        let f = |n: u64| Bytes::from(n.to_string());
        let mut ring = ParkedRing::default();
        assert!(ring.is_empty());
        ring.put(5, f(5), false);
        ring.put(7, f(7), true);
        ring.put(4, f(4), false);
        assert!(!ring.is_empty());
        assert_eq!(ring.take(3), None);
        assert_eq!(ring.take(4), Some((f(4), false)));
        assert_eq!(ring.take(5), Some((f(5), false)));
        assert_eq!(ring.take(6), None);
        assert_eq!(ring.take(7), Some((f(7), true)));
        assert!(ring.is_empty());
        ring.put(10, f(10), false);
        assert_eq!(ring.take(10), Some((f(10), false)));
        assert!(ring.is_empty());
        assert_eq!(ring.take(11), None);
    }

    #[test]
    fn converts_top_level_nils_for_resp3() {
        let nil = Bytes::from_static(resp::NIL_BULK);
        assert_eq!(convert_nil(nil.clone(), 3).as_ref(), resp::NIL_RESP3);
        assert_eq!(convert_nil(nil, 2).as_ref(), resp::NIL_BULK);
        let value = Bytes::from_static(b"$1\r\nx\r\n");
        assert_eq!(convert_nil(value.clone(), 3), value);
    }
}

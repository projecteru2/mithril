//! The reply writer: ordered emission, redirects and degrades, cache fills.

use std::cell::RefMut;
use std::collections::VecDeque;
use std::rc::Rc;

use bytes::Bytes;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Notify, oneshot};

use super::fanout::{Singles, multikey_plan, resend_singles, write_keys};
use super::link::{Fill, InFlight, InflightRing, WriterLink, mark_closed};
use super::pipe::{parse_redirect, pipe_for, queue_on};
use super::pubsub::PUBSUB_PUSH_WINDOW;
use super::queue::ReplyQueue;
use super::{ERR_TRYAGAIN, Reply, Shared};
use crate::backend::{ASKING_FRAME, BATCH, ERR_BACKEND_LOST, write_frames};
use crate::resp;
use crate::stats;

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
                        if let Some((req, base_expect, fill)) = take_retry(&link, seq, ask) {
                            stats::bump(&shared.wstats.redirects);
                            // the retry carries no CACHING opt-in: it cannot fill
                            if let Some(fill) = fill
                                && let Some(cache) = &shared.cache
                            {
                                fill.abandon(cache);
                            }
                            let _ = shared.refresh.send(());
                            let head = ask.then(|| Bytes::from_static(ASKING_FRAME));
                            let expect = base_expect + u32::from(ask);
                            let pipe =
                                pipe_for(&shared, target, client_id, false, link.sharded.get());
                            match queue_on(&pipe, &reply_q, seq, head, req, expect) {
                                Ok(Some(cold)) => cold.flush().await,
                                Ok(None) => {}
                                Err(()) => {
                                    let _ = reply_q
                                        .send(Reply::At(seq, Bytes::from_static(ERR_BACKEND_LOST)));
                                }
                            }
                            continue;
                        }
                        // clients believe the proxy owns every slot: never leak redirects
                        frame = Bytes::from_static(ERR_TRYAGAIN);
                    } else if frame.starts_with(b"-TRYAGAIN")
                        && let Some((req, fill)) = take_degrade(&link, seq)
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
                            // any later command already holds a sequence: a re-run would land
                            // out of order, so the client retries and this slot takes the
                            // gated path from now on
                            if seq + 1 == link.next_seq.get() {
                                let gate = Rc::new(Notify::new());
                                link.gate_slots(&[slot], &gate);
                                let (shared, reply_q, link) =
                                    (shared.clone(), reply_q.clone(), link.clone());
                                // detached deliberately: completion is bounded by backend replies
                                tokio::task::spawn_local(async move {
                                    let mut singles = Singles::new(merge);
                                    resend_singles(
                                        &shared,
                                        client_id,
                                        link.sharded.get(),
                                        &req,
                                        nkeys,
                                        0..nkeys,
                                        &mut singles,
                                    )
                                    .await;
                                    // a fill raced by the late writes goes before the gate lets
                                    // the client read again
                                    if plan.spec.is_write()
                                        && let Some(cache) = &shared.cache
                                    {
                                        write_keys(plan.spec, &req, plan.argc, |k| {
                                            cache.invalidate(k)
                                        });
                                    }
                                    link.release_gates(&[slot]);
                                    gate.notify_waiters();
                                    let _ = reply_q.send(Reply::At(seq, singles.merge(nkeys, &[])));
                                });
                                continue;
                            }
                        }
                    }
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

// retryable redirects: single-reply requests always, multi-reply blobs only for MOVED
fn take_retry(link: &WriterLink, seq: u64, ask: bool) -> Option<(Bytes, u32, Option<Fill>)> {
    let mut entry = entry_at(&link.inflight, seq)?;
    if entry.retried || (entry.expect > 1 && ask) {
        return None;
    }
    entry.retried = true;
    let fill = link.detach_fill(&mut entry);
    Some((entry.frame.clone(), entry.expect, fill))
}

// one key-by-key resend per request, whether or not a redirect retry preceded it
fn take_degrade(link: &WriterLink, seq: u64) -> Option<(Bytes, Option<Fill>)> {
    let mut entry = entry_at(&link.inflight, seq)?;
    if entry.degraded || entry.expect > 1 {
        return None;
    }
    // a redirect merged out of the resend must not re-run the whole request
    entry.degraded = true;
    entry.retried = true;
    let fill = link.detach_fill(&mut entry);
    Some((entry.frame.clone(), fill))
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

//! Multi-key commands: per-slot split, scatter, merge, key-by-key resend, SCAN and broadcasts.

use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::{Notify, oneshot};

use super::link::Fill;
use super::local::collect_args;
use super::pipe::{
    ColdSend, Pipe, Staged, parse_redirect, recv_or_lost, scatter_one, scatter_pipe, stage_one,
};
use super::session::{Session, eval_numkeys};
use super::{Cold, ERR_NO_OWNER, Reply, Shared, error_frame};
use crate::backend::{ASKING_FRAME, BATCH, ERR_BACKEND_LOST};
use crate::cache::{CACHING_FRAME, ReplyCache};
use crate::command::{self, Kind, Spec};
use crate::multikey;
use crate::resp;
use crate::stats;
use crate::{crc16, route};

// an MGET with more keys than this is never cached: it would arm one ticket per key
const MGET_CACHE_KEYS: usize = 64;
// reserved per item of a cached MGET reply before the sizes are known
const MGET_ITEM_GUESS: usize = 128;

#[derive(Clone, Copy)]
pub(super) enum Merge {
    Mget,
    Sum,
    Ok,
}

// a whole multi-key request re-issued key by key from the writer loop
#[derive(Clone, Copy)]
pub(super) struct DegradePlan {
    pub(super) spec: &'static Spec,
    pub(super) argc: usize,
    pub(super) merge: Merge,
    pub(super) nkeys: usize,
    pub(super) slot: u16,
}

// what a key-by-key resend accumulates: MGET keeps every item, the rest fold on arrival
pub(super) enum Singles {
    Mget(Vec<(usize, Bytes)>),
    Sum(Result<i64, Bytes>),
    Ok(Result<(), Bytes>),
}

impl Singles {
    pub(super) fn new(merge: Merge) -> Singles {
        match merge {
            Merge::Mget => Singles::Mget(Vec::new()),
            Merge::Sum => Singles::Sum(Ok(0)),
            Merge::Ok => Singles::Ok(Ok(())),
        }
    }

    fn push(&mut self, pos: usize, reply: Bytes) {
        match self {
            Singles::Mget(items) => items.push((pos, reply)),
            Singles::Sum(Ok(total)) => match multikey::parse_int(&reply) {
                Some(n) => *total += n,
                None => *self = Singles::Sum(Err(reply)),
            },
            Singles::Ok(Ok(())) if reply.as_ref() != resp::OK => *self = Singles::Ok(Err(reply)),
            _ => {}
        }
    }

    pub(super) fn merge(self, total: usize, parts: &[(Vec<usize>, Bytes)]) -> Bytes {
        let replies = || parts.iter().map(|(_, r)| r);
        let merged = match self {
            Singles::Mget(items) => multikey::merge_mget(total, parts, &items),
            Singles::Sum(acc) => acc.and_then(|base| multikey::merge_sum(replies(), base)),
            Singles::Ok(acc) => acc.and_then(|()| multikey::merge_ok(replies())),
        };
        merged.unwrap_or_else(|e| e)
    }
}

enum Planned {
    Parts(FanoutPlan),
    /// Every key hashes to one slot: the original frame routes as one command.
    Single(u16),
    Failed,
}

// a fan-out resolved and ready to queue; the frames retain themselves for resends
struct FanoutPlan {
    seq: u64,
    merge: Merge,
    degradable: bool,
    // per part, only for a cacheable MGET; empty otherwise
    cached: Vec<PartCache>,
    parts: Vec<multikey::Part>,
    pipes: Vec<Pipe>,
    total: usize,
    marks: Option<WriteMarks>,
    slots: Vec<u16>,
}

// holds a fan-out write's keys against cache fills until the operation ends
struct WriteMarks {
    cache: Rc<ReplyCache>,
    keys: Vec<Bytes>,
}

impl WriteMarks {
    fn new(cache: &Rc<ReplyCache>, keys: Vec<Bytes>) -> WriteMarks {
        for k in &keys {
            cache.begin_write(k);
        }
        WriteMarks {
            cache: cache.clone(),
            keys,
        }
    }
}

impl Drop for WriteMarks {
    fn drop(&mut self) {
        for k in &self.keys {
            self.cache.end_write(k);
        }
    }
}

// how one MGET-shaped frame meets the cache: served whole, filling, or neither
enum PartCache {
    Backend,
    Hit(Bytes),
    Fill(Vec<Bytes>),
}

impl PartCache {
    fn armed(self, cache: &ReplyCache) -> PartCache {
        match self {
            PartCache::Fill(keys) => cache
                .begin_fills(keys)
                .map_or(PartCache::Backend, PartCache::Fill),
            c => c,
        }
    }
}

impl Session {
    // fast path: no gate pending and every pipe has room, so nothing awaits
    pub(super) fn fan_out(
        &self,
        spec: &'static Spec,
        frame: Bytes,
        argc: usize,
    ) -> Option<Cold<'_>> {
        let seq = self.alloc_seq();
        if spec.kind == Kind::Mset && !(argc - 1).is_multiple_of(2) {
            self.emit_at(
                seq,
                error_frame("ERR wrong number of arguments for 'mset' command"),
            );
            return None;
        }
        let plan = match self.plan_fanout(seq, spec, &frame, argc) {
            Planned::Parts(plan) => plan,
            Planned::Failed => return None,
            Planned::Single(slot) => {
                if spec.is_write()
                    && let Some(cache) = &self.shared.cache
                {
                    write_keys(spec, &frame, argc, |k| cache.invalidate(k));
                }
                // the cache is consulted only once no gate is pending on the slot
                return self.gated(slot, move |s| s.serve_single(seq, slot, spec, frame));
            }
        };
        let cacheable = spec.flags & command::FLAG_CACHE != 0;
        let n = plan.parts.len();
        if self.fanouts_pending() || !plan.pipes.iter().all(|p| p.has_room(n)) {
            return Some(Box::pin(self.fan_out_slow(plan, cacheable)));
        }
        let mut plan = plan;
        if self.cache_parts(&mut plan, cacheable) {
            return None;
        }
        // has_room is a snapshot; a queue another worker filled meanwhile is awaited
        let mut receivers = Vec::with_capacity(n);
        for (i, (part, pipe)) in plan.parts.iter().zip(&plan.pipes).enumerate() {
            let Some(head) = part_head(plan.cached.get(i), &mut receivers) else {
                continue;
            };
            let (staged, rx) = stage_one(pipe, head, part.frame.clone());
            receivers.push(rx);
            if let Err(staged) = staged.try_send() {
                return Some(Box::pin(self.fan_out_resume(plan, receivers, i, staged)));
            }
        }
        self.launch_fanout(plan, receivers);
        None
    }

    pub(super) fn run_scan(&self, frame: Bytes, argc: usize) {
        let cursor = resp::Args::new(&frame, argc)
            .nth(1)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|v| v.parse::<u64>().ok());
        let Some(cursor) = cursor else {
            self.emit_error("ERR invalid cursor");
            return;
        };
        let (master_idx, node_cursor) = multikey::unpack_cursor(cursor);
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let id = self.id;
        // detached deliberately: completion is bounded by backend replies
        let sharded = self.link.sharded.get();
        tokio::task::spawn_local(async move {
            let topo = shared.topo.load_full();
            if master_idx >= topo.masters.len() {
                let done = multikey::rebuild_scan_reply(0, b"*0\r\n");
                let _ = reply_q.send(Reply::At(seq, Bytes::from(done)));
                return;
            }
            let addr = &topo.nodes[topo.masters[master_idx] as usize].addr;
            let mut cursor_buf = [0u8; resp::DEC_BUF];
            let mut sub_args: Vec<&[u8]> =
                vec![b"SCAN", resp::u64_digits(&mut cursor_buf, node_cursor)];
            let args = collect_args(&frame, argc);
            sub_args.extend_from_slice(&args[2..]);
            let mut cmd = Vec::new();
            resp::write_command(&mut cmd, &sub_args);
            let rx = scatter_one(&shared, addr, id, sharded, None, Bytes::from(cmd)).await;
            let reply = recv_or_lost(rx).await;
            let out = match multikey::parse_scan_reply(&reply) {
                Some((next, keys)) => {
                    let n_masters = topo.masters.len();
                    let synth = if next == 0 {
                        if master_idx + 1 < n_masters {
                            multikey::pack_cursor(master_idx + 1, 0)
                        } else {
                            0
                        }
                    } else {
                        multikey::pack_cursor(master_idx, next)
                    };
                    Bytes::from(multikey::rebuild_scan_reply(synth, keys))
                }
                None => reply,
            };
            let _ = reply_q.send(Reply::At(seq, out));
        });
    }

    pub(super) async fn run_broadcast(&self, frame: Bytes, merge: Merge) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let topo = shared.topo.load_full();
        let sharded = self.link.sharded.get();
        let mut receivers = Vec::with_capacity(topo.masters.len());
        for &midx in &topo.masters {
            let addr = &topo.nodes[midx as usize].addr;
            receivers.push(scatter_one(&shared, addr, self.id, sharded, None, frame.clone()).await);
        }
        // detached deliberately: completion is bounded by backend replies
        tokio::task::spawn_local(async move {
            let mut replies: Vec<Bytes> = Vec::with_capacity(receivers.len());
            for rx in receivers {
                replies.push(recv_or_lost(rx).await);
            }
            let merged = match merge {
                Merge::Sum => multikey::merge_sum(replies.iter(), 0),
                _ => multikey::merge_ok(replies.iter()),
            };
            let _ = reply_q.send(Reply::At(seq, merged.unwrap_or_else(|e| e)));
        });
    }

    async fn fan_out_resume(
        &self,
        plan: FanoutPlan,
        receivers: Vec<oneshot::Receiver<Bytes>>,
        at: usize,
        staged: Staged,
    ) {
        staged.send().await;
        self.scatter_rest(plan, receivers, at + 1).await;
    }

    // splits the keys per slot and resolves every pipe; one slot needs no fan-out
    fn plan_fanout(&self, seq: u64, spec: &Spec, frame: &Bytes, argc: usize) -> Planned {
        let readonly = spec.is_readonly();
        let mode = self.shared.cfg.slave_mode;
        let mut pairs = key_pairs(spec, frame, argc);
        let first = pairs.next().map(|(k, _)| crc16::slot(k));
        if let Some(slot) = first
            && pairs.all(|(k, _)| crc16::slot(k) == slot)
            && !self.link.is_migrating(slot)
        {
            return Planned::Single(slot);
        }
        let nkeys = key_indices(spec, argc).len();
        let mut keys: Vec<&[u8]> = Vec::with_capacity(nkeys);
        let mut slots: Vec<u16> = Vec::with_capacity(nkeys);
        let mut values: Option<Vec<&[u8]>> = (spec.step == 2).then(|| Vec::with_capacity(nkeys));
        for (key, value) in key_pairs(spec, frame, argc) {
            keys.push(key);
            slots.push(crc16::slot(key));
            if let (Some(vals), Some(v)) = (values.as_mut(), value) {
                vals.push(v);
            }
        }
        let total = keys.len();
        let marks = match &self.shared.cache {
            Some(cache) if spec.is_write() => Some(WriteMarks::new(
                cache,
                keys.iter().map(|k| frame.slice_ref(k)).collect(),
            )),
            _ => None,
        };
        let topo = self.topo();
        let mut part_slots = Vec::new();
        let parts = self.with_rng(|rng| {
            multikey::split(
                spec.name.as_bytes(),
                &keys,
                &slots,
                values.as_deref(),
                |slot| {
                    part_slots.push(slot);
                    route::pick(&topo, slot, readonly, mode, rng)
                },
            )
        });
        let parts = match parts {
            Ok(p) => p,
            Err(e) => {
                self.emit_at(seq, error_frame(&format!("CLUSTERDOWN {e}")));
                return Planned::Failed;
            }
        };
        let pipes = parts
            .iter()
            .map(|part| Some(self.cached_pipe(&topo, part.node, part.readonly)?.clone()))
            .collect();
        let Some(pipes) = pipes else {
            self.emit_at(seq, Bytes::from_static(ERR_BACKEND_LOST));
            return Planned::Failed;
        };
        Planned::Parts(FanoutPlan {
            seq,
            merge: merge_for(spec.kind).unwrap_or(Merge::Sum),
            degradable: degradable(spec),
            cached: Vec::new(),
            parts,
            pipes,
            total,
            marks,
            slots: part_slots,
        })
    }

    async fn fan_out_slow(&self, mut plan: FanoutPlan, cacheable: bool) {
        if self.fanouts_pending() && !self.wait_fanouts(&plan.slots).await {
            self.closing.set(true);
            return;
        }
        if self.cache_parts(&mut plan, cacheable) {
            return;
        }
        let receivers = Vec::with_capacity(plan.parts.len());
        self.scatter_rest(plan, receivers, 0).await;
    }

    // a same-slot multi-key command sent as one request; a cacheable one meets the cache here
    fn serve_single(
        &self,
        seq: u64,
        slot: u16,
        spec: &'static Spec,
        frame: Bytes,
    ) -> Option<Box<ColdSend>> {
        let mut fill = None;
        if spec.flags & command::FLAG_CACHE != 0
            && let Some(cache) = &self.shared.cache
        {
            match mget_cache(cache, &frame, self.may_fill()) {
                PartCache::Hit(reply) => {
                    stats::bump(&self.shared.wstats.cache_hits);
                    self.emit_at(seq, reply);
                    return None;
                }
                PartCache::Fill(keys) => fill = Some(Fill::Many(keys)),
                PartCache::Backend => {}
            }
            stats::bump(&self.shared.wstats.cache_misses);
        }
        self.route_single(seq, slot, spec.is_readonly(), frame, 1, fill)
    }

    // meets the cache per part once no gate is pending; true when every part hit and
    // the merged reply went out. Replica-routed parts never fill: those connections
    // carry no tracking
    fn cache_parts(&self, plan: &mut FanoutPlan, cacheable: bool) -> bool {
        let Some(cache) = &self.shared.cache else {
            return false;
        };
        if !cacheable || plan.total > MGET_CACHE_KEYS {
            return false;
        }
        let may_fill = self.may_fill();
        plan.cached = plan
            .parts
            .iter()
            .map(|p| mget_cache(cache, &p.frame, may_fill && !p.readonly).armed(cache))
            .collect();
        if !plan.cached.iter().all(|c| matches!(c, PartCache::Hit(_))) {
            stats::bump(&self.shared.wstats.cache_misses);
            return false;
        }
        stats::bump(&self.shared.wstats.cache_hits);
        let parts = std::mem::take(&mut plan.parts);
        let cached = std::mem::take(&mut plan.cached);
        let hits: Vec<(Vec<usize>, Bytes)> = parts
            .into_iter()
            .zip(cached)
            .map(|(part, c)| match c {
                PartCache::Hit(reply) => (part.positions, reply),
                _ => (part.positions, Bytes::new()),
            })
            .collect();
        let merged = multikey::merge_mget(plan.total, &hits, &[]).unwrap_or_else(|e| e);
        self.emit_at(plan.seq, merged);
        true
    }

    async fn scatter_rest(
        &self,
        plan: FanoutPlan,
        mut receivers: Vec<oneshot::Receiver<Bytes>>,
        from: usize,
    ) {
        for (i, (part, pipe)) in plan.parts.iter().zip(&plan.pipes).enumerate().skip(from) {
            let Some(head) = part_head(plan.cached.get(i), &mut receivers) else {
                continue;
            };
            receivers.push(scatter_pipe(pipe, head, part.frame.clone()).await);
        }
        self.launch_fanout(plan, receivers);
    }

    // every part is queued by now, so later commands stay behind this one
    fn launch_fanout(&self, plan: FanoutPlan, receivers: Vec<oneshot::Receiver<Bytes>>) {
        let FanoutPlan {
            seq,
            merge,
            degradable,
            cached,
            parts,
            pipes: _,
            total,
            marks,
            slots,
        } = plan;
        let gate = Rc::new(Notify::new());
        self.link.gate_slots(&slots, &gate);
        let link = self.link.clone();
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let id = self.id;
        // detached deliberately: completion is bounded by backend replies
        tokio::task::spawn_local(async move {
            let _marks = marks;
            let sharded = link.sharded.get();
            let mut results: Vec<(Vec<usize>, Bytes)> = Vec::with_capacity(parts.len());
            let mut retries: Vec<(multikey::Part, oneshot::Receiver<Bytes>)> = Vec::new();
            let mut singles = Singles::new(merge);
            let mut cached = cached.into_iter();
            for (part, rx) in parts.into_iter().zip(receivers) {
                let reply = recv_or_lost(rx).await;
                if let Some(PartCache::Fill(keys)) = cached.next()
                    && let Some(cache) = &shared.cache
                {
                    cache.complete_fills(&keys, &reply);
                }
                if reply.first() != Some(&b'-') {
                    results.push((part.positions, reply));
                    continue;
                }
                // a redirected part executed nothing: one resend is idempotent
                match parse_redirect(&reply) {
                    Some((ask, target)) => {
                        stats::bump(&shared.wstats.redirects);
                        let _ = shared.refresh.send(());
                        let head = ask.then(|| Bytes::from_static(ASKING_FRAME));
                        let frame = part.frame.clone();
                        let rx = scatter_one(&shared, target, id, sharded, head, frame).await;
                        retries.push((part, rx));
                    }
                    None if degradable && reply.starts_with(b"-TRYAGAIN") => {
                        // boxed: the resend's state must not widen every fan-out's future
                        Box::pin(resend_singles(
                            &shared,
                            id,
                            sharded,
                            &part.frame,
                            part.positions.len(),
                            part.positions.iter().copied(),
                            &mut singles,
                        ))
                        .await;
                    }
                    None => results.push((part.positions, reply)),
                }
            }
            for (part, rx) in retries {
                let reply = recv_or_lost(rx).await;
                if degradable && reply.starts_with(b"-TRYAGAIN") {
                    Box::pin(resend_singles(
                        &shared,
                        id,
                        sharded,
                        &part.frame,
                        part.positions.len(),
                        part.positions.iter().copied(),
                        &mut singles,
                    ))
                    .await;
                } else {
                    results.push((part.positions, reply));
                }
            }
            link.release_gates(&slots);
            gate.notify_waiters();
            let _ = reply_q.send(Reply::At(seq, singles.merge(total, &results)));
        });
    }
}

// keys a write mutates: the declared range, STORE destinations, script keys
pub(super) fn write_keys(spec: &Spec, frame: &Bytes, argc: usize, mut f: impl FnMut(&[u8])) {
    let mut args = resp::Args::new(frame, argc);
    if spec.kind == Kind::Eval {
        let numkeys = eval_numkeys(&mut args).map_or(0, |n| n.max(0) as usize);
        for key in args.take(numkeys) {
            f(key);
        }
        return;
    }
    let mut cur = 0;
    for want in key_indices(spec, argc) {
        let Some(key) = args.nth(want - cur) else {
            return;
        };
        f(key);
        cur = want + 1;
    }
    if spec.flags & command::FLAG_STORE != 0 {
        let mut dest_next = false;
        for a in args {
            if dest_next {
                f(a);
            }
            dest_next = a.eq_ignore_ascii_case(b"store") || a.eq_ignore_ascii_case(b"storedist");
        }
    }
}

pub(super) fn key_pairs<'a>(
    spec: &Spec,
    frame: &'a Bytes,
    argc: usize,
) -> impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)> {
    let mut args = resp::Args::new(frame, argc);
    let mut cur = 0;
    let paired = spec.step == 2;
    key_indices(spec, argc).map_while(move |want| {
        let key = args.nth(want - cur)?;
        cur = want + 1;
        let value = if paired { args.next() } else { None };
        cur += usize::from(value.is_some());
        Some((key, value))
    })
}

// a migrating slot answers a multi-key request with TRYAGAIN; one key at a time follows ASK
pub(super) async fn resend_singles(
    shared: &Rc<Shared>,
    id: u64,
    sharded: bool,
    frame: &[u8],
    count: usize,
    positions: impl Iterator<Item = usize>,
    out: &mut Singles,
) {
    let topo = shared.topo.load_full();
    let mut singles = multikey::singles(frame, count, positions);
    let mut pending = Vec::with_capacity(count.min(BATCH));
    let mut followed = Vec::new();
    let mut refreshed = false;
    loop {
        let mut more = false;
        for (pos, slot, frame) in singles.by_ref().take(BATCH) {
            more = true;
            let Some(addr) = topo.owner_addr(slot) else {
                out.push(pos, Bytes::from_static(ERR_NO_OWNER));
                continue;
            };
            let rx = scatter_one(shared, addr, id, sharded, None, frame.clone()).await;
            pending.push((pos, frame, rx));
        }
        for (pos, frame, rx) in pending.drain(..) {
            let reply = recv_or_lost(rx).await;
            match parse_redirect(&reply) {
                Some((ask, target)) => {
                    stats::bump(&shared.wstats.redirects);
                    if !std::mem::replace(&mut refreshed, true) {
                        let _ = shared.refresh.send(());
                    }
                    let head = ask.then(|| Bytes::from_static(ASKING_FRAME));
                    let rx = scatter_one(shared, target, id, sharded, head, frame).await;
                    followed.push((pos, rx));
                }
                None => out.push(pos, reply),
            }
        }
        for (pos, rx) in followed.drain(..) {
            out.push(pos, recv_or_lost(rx).await);
        }
        if !more {
            break;
        }
    }
}

pub(super) fn multikey_plan(frame: &Bytes) -> Option<DegradePlan> {
    let (argc, _) = resp::scan_int_line(frame, 1)?;
    let argc = usize::try_from(argc).ok()?;
    let mut args = resp::Args::new(frame, argc);
    let spec = command::lookup(args.next()?)?;
    let merge = merge_for(spec.kind).filter(|_| degradable(spec))?;
    let slot = crc16::slot(args.nth(spec.first_key as usize - 1)?);
    Some(DegradePlan {
        spec,
        argc,
        merge,
        nkeys: key_indices(spec, argc).len(),
        slot,
    })
}

// argument indices holding keys, per the spec's first/last/step triple
fn key_indices(spec: &Spec, argc: usize) -> impl ExactSizeIterator<Item = usize> {
    let first = spec.first_key as usize;
    let last = if spec.last_key < 0 {
        (argc as i64 + i64::from(spec.last_key)).max(0) as usize
    } else {
        spec.last_key as usize
    };
    let end = if first == 0 {
        0
    } else {
        last.min(argc.saturating_sub(1)) + 1
    };
    (first..end).step_by((spec.step as usize).max(1))
}

// the head a part is sent with; None for a part the cache already answered
fn part_head(
    cached: Option<&PartCache>,
    receivers: &mut Vec<oneshot::Receiver<Bytes>>,
) -> Option<Option<Bytes>> {
    match cached {
        Some(PartCache::Hit(reply)) => {
            receivers.push(resolved(reply.clone()));
            None
        }
        Some(PartCache::Fill(_)) => Some(Some(Bytes::from_static(CACHING_FRAME))),
        _ => Some(None),
    }
}

// a part served from the cache answers through the same channel as a fetched one
fn resolved(reply: Bytes) -> oneshot::Receiver<Bytes> {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(reply);
    rx
}

// serves an MGET-shaped frame whole when every key hits, else names the keys to fill
fn mget_cache(cache: &ReplyCache, frame: &Bytes, may_fill: bool) -> PartCache {
    let Some((argc, _)) = resp::scan_int_line(frame, 1) else {
        return PartCache::Backend;
    };
    let nkeys = argc.max(1) as usize - 1;
    if nkeys == 0 || nkeys > MGET_CACHE_KEYS {
        return PartCache::Backend;
    }
    let keys = || resp::Args::new(frame, argc as usize).skip(1);
    cache.sync();
    let mut out = Vec::with_capacity(16 + nkeys * MGET_ITEM_GUESS);
    resp::array_header(&mut out, nkeys);
    let found = keys()
        .take_while(|k| cache.lookup_into(k, &mut out))
        .count();
    let hit = found == nkeys;
    cache.note(nkeys as u32, hit);
    if hit {
        return PartCache::Hit(Bytes::from(out));
    }
    if !may_fill || !cache.admit_fill(nkeys as u32) {
        return PartCache::Backend;
    }
    PartCache::Fill(keys().map(|k| frame.slice_ref(k)).collect())
}

fn degradable(spec: &Spec) -> bool {
    spec.flags & command::FLAG_UNION == 0
}

fn merge_for(kind: Kind) -> Option<Merge> {
    match kind {
        Kind::Mget => Some(Merge::Mget),
        Kind::Mset => Some(Merge::Ok),
        Kind::MultiSum => Some(Merge::Sum),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str) -> &'static Spec {
        command::lookup(name.as_bytes()).unwrap()
    }

    #[test]
    fn singles_fold_sums_and_oks_and_keep_mget_items() {
        let mut s = Singles::new(Merge::Sum);
        s.push(0, Bytes::from_static(b":2\r\n"));
        s.push(1, Bytes::from_static(b":3\r\n"));
        assert_eq!(s.merge(2, &[]).as_ref(), b":5\r\n");
        let mut s = Singles::new(Merge::Ok);
        s.push(0, Bytes::from_static(b"+OK\r\n"));
        s.push(1, Bytes::from_static(b"-ERR x\r\n"));
        assert_eq!(s.merge(2, &[]).as_ref(), b"-ERR x\r\n");
        let mut s = Singles::new(Merge::Mget);
        s.push(1, Bytes::from_static(b"*1\r\n$1\r\nb\r\n"));
        s.push(0, Bytes::from_static(b"*1\r\n$1\r\na\r\n"));
        assert_eq!(s.merge(2, &[]).as_ref(), b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    }

    #[test]
    fn multikey_plan_covers_the_fanned_out_kinds() {
        let frame = |args: &[&[u8]]| {
            let mut f = Vec::new();
            resp::write_command(&mut f, args);
            Bytes::from(f)
        };
        let slot = crc16::slot(b"a");
        let plan = |args: &[&[u8]]| multikey_plan(&frame(args)).map(|p| (p.merge, p.nkeys, p.slot));
        assert!(matches!(plan(&[b"MGET", b"a", b"b"]), Some((Merge::Mget, 2, s)) if s == slot));
        assert!(matches!(
            plan(&[b"mset", b"a", b"1", b"b", b"2"]),
            Some((Merge::Ok, 2, s)) if s == slot
        ));
        assert!(matches!(plan(&[b"DEL", b"a", b"b", b"c"]), Some((Merge::Sum, 3, s)) if s == slot));
        assert!(multikey_plan(&frame(&[b"GET", b"a"])).is_none());
        assert!(multikey_plan(&frame(&[b"PFCOUNT", b"a", b"b"])).is_none());
        assert!(multikey_plan(&frame(&[b"SINTER", b"a", b"b"])).is_none());
    }

    #[test]
    fn key_indices_honor_first_last_step() {
        let set: Vec<usize> = key_indices(spec("set"), 3).collect();
        assert_eq!(set, vec![1]);
        let mset: Vec<usize> = key_indices(spec("mset"), 5).collect();
        assert_eq!(mset, vec![1, 3]);
        let del: Vec<usize> = key_indices(spec("del"), 4).collect();
        assert_eq!(del, vec![1, 2, 3]);
        let rename: Vec<usize> = key_indices(spec("rename"), 3).collect();
        assert_eq!(rename, vec![1, 2]);
        let ping: Vec<usize> = key_indices(spec("ping"), 1).collect();
        assert!(ping.is_empty());
    }

    #[test]
    fn write_keys_cover_ranges_store_targets_and_scripts() {
        let keys = |cmd: &[&str]| {
            let args: Vec<&[u8]> = cmd.iter().map(|a| a.as_bytes()).collect();
            let mut raw = Vec::new();
            resp::write_command(&mut raw, &args);
            let frame = Bytes::from(raw);
            let mut out: Vec<String> = Vec::new();
            write_keys(spec(cmd[0]), &frame, cmd.len(), |k| {
                out.push(String::from_utf8_lossy(k).into_owned())
            });
            out
        };
        assert_eq!(keys(&["set", "k", "v"]), ["k"]);
        assert_eq!(keys(&["mset", "a", "1", "b", "2"]), ["a", "b"]);
        assert_eq!(keys(&["rename", "a", "b"]), ["a", "b"]);
        assert_eq!(
            keys(&["sort", "src", "alpha", "STORE", "dst"]),
            ["src", "dst"]
        );
        assert_eq!(
            keys(&["georadius", "g", "0", "0", "1", "km", "storedist", "d"]),
            ["g", "d"]
        );
        assert_eq!(keys(&["eval", "return 1", "2", "a", "b", "c"]), ["a", "b"]);
        assert_eq!(keys(&["eval", "return 1", "0"]), Vec::<String>::new());
        assert_eq!(keys(&["eval", "return 1", "9", "a"]), ["a"]);
    }
}

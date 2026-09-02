//! One client session: the read loop, command dispatch and single-key routing.

use std::cell::{Cell, Ref, RefCell};
use std::net::SocketAddr;
use std::ops::Range;
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::{Notify, oneshot};

use super::fanout::{Merge, write_keys};
use super::link::{Fill, InFlight, WriterLink};
use super::local::{MultiState, display_name};
use super::pipe::{ColdSend, Pipe, pipe_for, queue_on};
use super::pubsub::{PubsubHandle, PubsubSim, pubsub_allowed};
use super::queue::ReplyQueue;
use super::tuner::PIPELINED_LOCAL;
use super::writer::write_loop;
use super::{Cold, ERR_NO_OWNER, ERR_NOAUTH, MAX_INFLIGHT, Reply, Shared, error_frame};
use crate::backend::{ERR_BACKEND_LOST, ensure_read_room};
use crate::cache::CACHING_FRAME;
use crate::command::{self, Kind, Spec};
use crate::config::Sharding;
use crate::resp::{self, ReqScan};
use crate::server::topo_epoch;
use crate::stats::{self, Stats};
use crate::topology::Topology;
use crate::{crc16, route};

const GATE_PROBE: Duration = Duration::from_millis(100);

pub(super) struct Session {
    pub(super) shared: Rc<Shared>,
    pub(super) id: u64,
    pub(super) reply_q: Rc<ReplyQueue>,
    pub(super) link: Rc<WriterLink>,
    pub(super) proto: Cell<u8>,
    pub(super) authed: Cell<bool>,
    rng: Cell<u64>,
    pub(super) multi: RefCell<Option<MultiState>>,
    pub(super) in_multi: Cell<bool>,
    pub(super) pubsub: RefCell<Option<PubsubHandle>>,
    pub(super) subs: RefCell<PubsubSim>,
    pub(super) blocking: RefCell<Vec<(u64, tokio::task::JoinHandle<()>)>>,
    pub(super) closing: Cell<bool>,
    // the worker wants this session on the shared pipes: reading pauses until it drains
    pub(super) switch_pending: Cell<bool>,
    // a live relay can close the session while the reader is parked reading
    pub(super) has_relay: Cell<bool>,
    auto: bool,
    pub(super) pipelined: Cell<u8>,
    pub(super) conns: RefCell<ConnCache>,
    topo_cache: RefCell<Arc<Topology>>,
}

impl Session {
    // one relaxed epoch load replaces the arc-swap hazard load on the hot path
    pub(super) fn topo(&self) -> Ref<'_, Arc<Topology>> {
        let cached = self.topo_cache.borrow();
        if topo_epoch() == cached.epoch {
            return cached;
        }
        let old = Arc::clone(&cached);
        drop(cached);
        let fresh = self.shared.topo.load_full();
        // a migration mark outlives refreshes until the slot's owner changes
        self.link
            .retain_migrating(|slot| old.owner_addr(slot) == fresh.owner_addr(slot));
        *self.topo_cache.borrow_mut() = fresh;
        self.topo_cache.borrow()
    }

    pub(super) fn key_slot(&self, frame: &Bytes, argc: usize, key_index: usize) -> Option<u16> {
        let mut it = resp::Args::new(frame, argc);
        it.nth(key_index).map(crc16::slot)
    }

    // one borrow and index per request once warm; re-resolves on epoch or death
    pub(super) fn cached_pipe(
        &self,
        topo: &Topology,
        idx: u16,
        is_replica: bool,
    ) -> Option<Ref<'_, Pipe>> {
        let i = idx as usize;
        if let Ok(pipe) = Ref::filter_map(self.conns.borrow(), |c| c.live(topo.epoch, i)) {
            return Some(pipe);
        }
        self.resolve_pipe(topo, i, is_replica);
        Ref::filter_map(self.conns.borrow(), |c| c.live(topo.epoch, i)).ok()
    }

    pub(super) fn any_master_addr(&self) -> Option<String> {
        let topo = self.topo();
        let idx = self.with_rng(|r| route::any_master(&topo, r))?;
        Some(topo.nodes[idx as usize].addr.clone())
    }

    pub(super) fn with_rng<R>(&self, f: impl FnOnce(&mut u64) -> R) -> R {
        let mut rng = self.rng.get();
        let out = f(&mut rng);
        self.rng.set(rng);
        out
    }

    pub(super) fn route_single(
        &self,
        seq: u64,
        slot: u16,
        readonly: bool,
        frame: Bytes,
        expect: u32,
        fill: Option<Fill>,
    ) -> Option<Box<ColdSend>> {
        let (pipe, is_replica) = {
            let topo = self.topo();
            let picked = self
                .with_rng(|r| route::pick(&topo, slot, readonly, self.shared.cfg.slave_mode, r));
            let routed = picked.and_then(|(idx, ro)| Some((self.cached_pipe(&topo, idx, ro)?, ro)));
            let Some(routed) = routed else {
                let err = if picked.is_some() {
                    ERR_BACKEND_LOST
                } else {
                    ERR_NO_OWNER
                };
                self.emit_at(seq, Bytes::from_static(err));
                return None;
            };
            routed
        };
        // replica connections are untracked: their replies never fill
        let fill = match (&self.shared.cache, fill) {
            (Some(cache), Some(fill)) if !is_replica => fill.arm(cache),
            _ => None,
        };
        let head = fill.is_some().then(|| Bytes::from_static(CACHING_FRAME));
        let sent = expect + u32::from(head.is_some());
        self.track_inflight(seq, &frame, expect, fill);
        self.queue_at(&pipe, seq, head, frame, sent)
    }

    // one slot-routed request; only a pending fan-out gate puts it on the boxed path
    pub(super) fn send_single(
        &self,
        seq: u64,
        slot: u16,
        readonly: bool,
        frame: Bytes,
    ) -> Option<Cold<'_>> {
        self.gated(slot, move |s| {
            s.route_single(seq, slot, readonly, frame, 1, None)
        })
    }

    pub(super) fn gated<'a>(
        &'a self,
        slot: u16,
        act: impl FnOnce(&'a Session) -> Option<Box<ColdSend>> + 'a,
    ) -> Option<Cold<'a>> {
        if self.fanouts_pending() {
            return Some(Box::pin(async move {
                if !self.wait_fanouts(&[slot]).await {
                    self.closing.set(true);
                    return;
                }
                if let Some(cold) = act(self) {
                    cold.flush().await;
                }
            }));
        }
        let cold = act(self)?;
        Some(Box::pin(cold.flush()))
    }

    // under auto sharding only the shared pipes carry tracking: a session on its
    // worker-local connections reads the cache but never fills it
    pub(super) fn may_fill(&self) -> bool {
        !self.auto || self.link.sharded.get()
    }

    pub(super) fn fanouts_pending(&self) -> bool {
        self.link.fanouts_any.get()
    }

    // false once the session closed: the caller drops the command instead of sending it
    pub(super) async fn wait_fanouts(&self, slots: &[u16]) -> bool {
        for slot in slots {
            loop {
                if self.link.closed.get() {
                    return false;
                }
                let Some(gate) = self.link.fanouts.borrow().get(slot).cloned() else {
                    break;
                };
                if !self.notified_or_closed(&gate).await {
                    return false;
                }
            }
        }
        true
    }

    pub(super) async fn notified_or_closed(&self, n: &Notify) -> bool {
        tokio::select! {
            _ = n.notified() => true,
            _ = self.link.closed_notify.notified() => false,
        }
    }

    pub(super) fn outstanding(&self) -> u64 {
        self.link
            .next_seq
            .get()
            .saturating_sub(self.link.emitted.get())
    }

    pub(super) fn alloc_seq(&self) -> u64 {
        let seq = self.link.next_seq.get();
        self.link.next_seq.set(seq + 1);
        if !self.link.writer_blocked.get() {
            self.shared.inflight.set(self.shared.inflight.get() + 1);
        }
        seq
    }

    pub(super) fn emit_local(&self, bytes: impl Into<Bytes>) {
        let seq = self.alloc_seq();
        self.emit_at(seq, bytes.into());
    }

    pub(super) fn emit_error(&self, msg: &str) {
        self.emit_error_frame(error_frame(msg));
    }

    pub(super) fn emit_error_frame(&self, frame: Bytes) {
        stats::bump(&self.shared.wstats.errors);
        self.emit_local(frame);
    }

    pub(super) fn emit_at(&self, seq: u64, frame: Bytes) {
        let _ = self.reply_q.send(Reply::At(seq, frame));
    }

    async fn dispatch(&self, frame: Bytes, argc: usize) {
        // a command must not commit effects its client can never observe
        if self.link.closed.get() {
            self.closing.set(true);
            return;
        }
        stats::bump(&self.shared.wstats.commands);
        if argc == 0 {
            return;
        }
        let mut it = resp::Args::new(&frame, argc);
        let spec = {
            let Some(name) = it.next() else {
                return;
            };
            match command::lookup(name) {
                Some(spec) => spec,
                None => {
                    let name = display_name(name);
                    self.abort_multi();
                    self.emit_error(&format!("ERR unknown command '{name}'"));
                    return;
                }
            }
        };
        if !spec.arity_ok(argc) {
            self.abort_multi();
            self.emit_error(&format!(
                "ERR wrong number of arguments for '{}' command",
                spec.name
            ));
            return;
        }
        if !self.authed.get() && spec.flags & command::FLAG_NO_AUTH == 0 {
            self.emit_error_frame(Bytes::from_static(ERR_NOAUTH));
            return;
        }
        if self.auto {
            self.adapt_pipes();
        }
        if self.has_relay.get() {
            if !self.exit_pubsub_if_done().await {
                let passthrough = self.proto.get() >= 3 && !pubsub_allowed(spec);
                if !passthrough {
                    // a pipelined QUIT must not backfill confirmations still in flight
                    if spec.name == "quit" {
                        self.drain_acks().await;
                    }
                    self.dispatch_pubsub(spec, frame, argc);
                    return;
                }
            }
            // the ack-drain wait can observe the relay dying: re-check
            if self.link.closed.get() {
                self.closing.set(true);
                return;
            }
        }
        if self.in_multi.get() && spec.flags & command::FLAG_TXN_CTRL == 0 {
            self.queue_multi(spec, frame, argc);
            return;
        }
        // writes drop their keys before they are queued: read-your-writes
        if spec.is_write()
            && let Some(cache) = &self.shared.cache
        {
            match spec.kind {
                Kind::Single | Kind::MultiSum | Kind::Mset => {}
                Kind::Flushall => cache.clear(),
                _ => write_keys(spec, &frame, argc, |k| cache.invalidate(k)),
            }
        }
        match spec.kind {
            Kind::Single => {
                let Some(key) = it.nth(spec.first_key as usize - 1) else {
                    self.emit_error("ERR missing key");
                    return;
                };
                let at = span(&frame, key);
                if let Some(cold) = self.serve_key(spec, frame, at, argc) {
                    cold.await;
                }
            }
            Kind::Local => self.handle_local(spec, frame, argc),
            Kind::Exec => {
                if let Some(cold) = self.handle_exec() {
                    cold.await;
                }
            }
            Kind::AnyMaster => self.forward_any_master(frame).await,
            Kind::MultiSum | Kind::Mget | Kind::Mset => {
                if let Some(cold) = self.fan_out(spec, frame, argc) {
                    cold.await;
                }
            }
            Kind::Blocking => {
                if let Some(cold) = self.forward_blocking(spec, frame, argc) {
                    cold.await;
                }
            }
            Kind::Eval => {
                if let Some(cold) = self.forward_eval(frame, argc) {
                    cold.await;
                }
            }
            Kind::Xread => {
                if let Some(cold) = self.forward_xread(spec, frame, argc) {
                    cold.await;
                }
            }
            Kind::Subscribe => self.enter_pubsub(spec, frame, argc),
            Kind::Scan => {
                if Box::pin(self.gates_clear()).await {
                    self.run_scan(frame, argc);
                }
            }
            Kind::Dbsize => {
                if Box::pin(self.gates_clear()).await {
                    Box::pin(self.run_broadcast(frame, Merge::Sum)).await;
                }
            }
            Kind::Flushall => {
                if Box::pin(self.gates_clear()).await {
                    Box::pin(self.run_broadcast(frame, Merge::Ok)).await;
                }
            }
        }
    }

    fn resolve_pipe(&self, topo: &Topology, i: usize, is_replica: bool) {
        let mut cache = self.conns.borrow_mut();
        if cache.epoch != topo.epoch {
            cache.epoch = topo.epoch;
            cache.by_node.clear();
        }
        if cache.by_node.len() < topo.nodes.len() {
            cache.by_node.resize(topo.nodes.len(), None);
        }
        cache.by_node[i] = Some(pipe_for(
            &self.shared,
            &topo.nodes[i].addr,
            self.id,
            is_replica,
            self.link.sharded.get(),
        ));
    }

    fn queue_at(
        &self,
        pipe: &Pipe,
        seq: u64,
        head: Option<Bytes>,
        frame: Bytes,
        expect: u32,
    ) -> Option<Box<ColdSend>> {
        match queue_on(pipe, &self.reply_q, seq, head, frame, expect) {
            Ok(cold) => cold,
            Err(()) => {
                self.emit_at(seq, Bytes::from_static(ERR_BACKEND_LOST));
                None
            }
        }
    }

    async fn forward_any_master(&self, frame: Bytes) {
        let seq = self.alloc_seq();
        let pipe = {
            let topo = self.topo();
            let picked = self.with_rng(|r| route::any_master(&topo, r));
            let Some(idx) = picked else {
                self.emit_at(seq, Bytes::from_static(ERR_NO_OWNER));
                return;
            };
            let Some(pipe) = self.cached_pipe(&topo, idx, false) else {
                self.emit_at(seq, Bytes::from_static(ERR_BACKEND_LOST));
                return;
            };
            pipe.clone()
        };
        self.send_at(&pipe, seq, frame, 1).await;
    }

    async fn send_at(&self, pipe: &Pipe, seq: u64, frame: Bytes, expect: u32) {
        if let Some(cold) = self.queue_at(pipe, seq, None, frame, expect) {
            cold.flush().await;
        }
    }

    fn track_inflight(&self, seq: u64, frame: &Bytes, expect: u32, fill: Option<Fill>) {
        if fill.is_some() {
            self.link.fills_armed.set(self.link.fills_armed.get() + 1);
        }
        self.link.inflight.borrow_mut().push_back(InFlight {
            seq,
            frame: frame.clone(),
            expect,
            retried: false,
            degraded: false,
            fill,
        });
    }

    fn forward_eval(&self, frame: Bytes, argc: usize) -> Option<Cold<'_>> {
        let (numkeys, slot) = {
            let mut args = resp::Args::new(&frame, argc);
            let numkeys = eval_numkeys(&mut args).unwrap_or(-1);
            (numkeys, args.next().map(crc16::slot))
        };
        if numkeys < 0 || 3 + numkeys as usize > argc {
            self.emit_error("ERR Number of keys can't be negative");
            return None;
        }
        if numkeys == 0 {
            return Some(Box::pin(self.forward_any_master(frame)));
        }
        let Some(slot) = slot else {
            self.emit_error("ERR missing key");
            return None;
        };
        let seq = self.alloc_seq();
        self.send_single(seq, slot, false, frame)
    }

    fn serve_key(
        &self,
        spec: &'static Spec,
        frame: Bytes,
        at: Range<usize>,
        argc: usize,
    ) -> Option<Cold<'_>> {
        let slot = crc16::slot(&frame[at.clone()]);
        let seq = self.alloc_seq();
        self.gated(slot, move |s| s.route_key(seq, slot, spec, frame, at, argc))
    }

    fn route_key(
        &self,
        seq: u64,
        slot: u16,
        spec: &'static Spec,
        frame: Bytes,
        at: Range<usize>,
        argc: usize,
    ) -> Option<Box<ColdSend>> {
        let mut fill = None;
        if let Some(cache) = &self.shared.cache {
            let key = &frame[at];
            if spec.is_write() {
                if spec.first_key == 1
                    && spec.last_key == 1
                    && spec.flags & command::FLAG_STORE == 0
                {
                    cache.invalidate(key);
                } else {
                    write_keys(spec, &frame, argc, |k| cache.invalidate(k));
                }
            } else if spec.flags & command::FLAG_CACHE != 0 {
                if let Some(hit) = cache.lookup(key) {
                    stats::bump(&self.shared.wstats.cache_hits);
                    cache.note(1, true);
                    self.emit_at(seq, hit);
                    return None;
                }
                stats::bump(&self.shared.wstats.cache_misses);
                cache.note(1, false);
                if self.may_fill() && cache.admit_fill(1) {
                    fill = Some(Fill::One(frame.slice_ref(key)));
                }
            }
        }
        self.route_single(seq, slot, spec.is_readonly(), frame, 1, fill)
    }

    // cluster-wide commands wait until no fan-out gate is pending on any slot
    async fn gates_clear(&self) -> bool {
        loop {
            let slots: Vec<u16> = self.link.fanouts.borrow().keys().copied().collect();
            if slots.is_empty() {
                return true;
            }
            if !self.wait_fanouts(&slots).await {
                self.closing.set(true);
                return false;
            }
        }
    }

    fn window_full(&self) -> bool {
        let outstanding = self.outstanding();
        outstanding >= MAX_INFLIGHT as u64 || (self.switch_pending.get() && outstanding > 0)
    }
}

// per-session resolved connections, valid for one topology epoch
pub(super) struct ConnCache {
    epoch: u64,
    pub(super) by_node: Vec<Option<Pipe>>,
}

impl ConnCache {
    fn live(&self, epoch: u64, i: usize) -> Option<&Pipe> {
        (self.epoch == epoch)
            .then(|| self.by_node.get(i)?.as_ref().filter(|p| !p.is_dead()))
            .flatten()
    }
}

// CLIENT LIST membership for the session's lifetime
struct Listed<'a> {
    stats: &'a Stats,
    id: u64,
}

impl<'a> Listed<'a> {
    fn new(stats: &'a Stats, id: u64, addr: SocketAddr, fd: i32) -> Listed<'a> {
        stats.registry().insert(
            id,
            stats::ClientInfo {
                addr,
                fd,
                name: Box::from(""),
                since: Instant::now(),
            },
        );
        Listed { stats, id }
    }
}

impl Drop for Listed<'_> {
    fn drop(&mut self) {
        self.stats.registry().remove(&self.id);
    }
}

/// Serves one client connection to completion.
pub async fn serve(shared: Rc<Shared>, stream: TcpStream, addr: SocketAddr, id: u64) {
    if stream.set_nodelay(true).is_err() {
        return;
    }
    let _listed = Listed::new(&shared.stats, id, addr, stream.as_raw_fd());
    let (mut read_half, write_half) = stream.into_split();
    let reply_q = ReplyQueue::new(shared.fabric.is_some());
    let link: Rc<WriterLink> = Rc::new(WriterLink::default());
    link.sharded
        .set(shared.cfg.backend_sharding == Sharding::On);

    let session = Session {
        shared: shared.clone(),
        id,
        reply_q: reply_q.clone(),
        link: link.clone(),
        proto: Cell::new(2),
        authed: Cell::new(shared.cfg.requirepass.is_empty()),
        rng: Cell::new(id | 1),
        multi: RefCell::new(None),
        in_multi: Cell::new(false),
        pubsub: RefCell::new(None),
        subs: RefCell::new(PubsubSim::default()),
        blocking: RefCell::new(Vec::new()),
        closing: Cell::new(false),
        switch_pending: Cell::new(false),
        has_relay: Cell::new(false),
        auto: shared.cfg.backend_sharding == Sharding::Auto,
        pipelined: Cell::new(PIPELINED_LOCAL),
        conns: RefCell::new(ConnCache {
            epoch: 0,
            by_node: Vec::new(),
        }),
        topo_cache: RefCell::new(shared.topo.load_full()),
    };

    let (close_tx, close_rx) = oneshot::channel::<u64>();
    let writer = tokio::task::spawn_local(write_loop(
        shared.clone(),
        write_half,
        reply_q.clone(),
        close_rx,
        link.clone(),
        id,
    ));

    let mut buf = BytesMut::with_capacity(crate::backend::READ_INIT);
    let mut cur = resp::Cursor::default();
    'main: loop {
        // a closing session must not let a half-open client keep executing writes
        if link.closed.get() {
            break;
        }
        loop {
            if session.window_full() {
                break;
            }
            match resp::scan_request_at(&buf, &mut cur) {
                ReqScan::Complete { len, argc } => {
                    let frame = buf.split_to(len).freeze();
                    session.dispatch(frame, argc).await;
                }
                ReqScan::Inline { len } => {
                    let line = buf.split_to(len);
                    let Some(args) = resp::split_inline(&line) else {
                        session.emit_error("ERR Protocol error: unbalanced quotes in request");
                        break 'main;
                    };
                    let argc = args.len();
                    if argc > 0 {
                        let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
                        let mut rebuilt = Vec::new();
                        resp::write_command(&mut rebuilt, &refs);
                        session.dispatch(Bytes::from(rebuilt), argc).await;
                    }
                }
                ReqScan::Invalid(e) => {
                    session.emit_error(&format!("ERR Protocol error: {e}"));
                    break 'main;
                }
                ReqScan::Incomplete => break,
            }
            if session.closing.get() {
                break 'main;
            }
        }
        if session.window_full() {
            while session.window_full() {
                if link.closed.get() {
                    break 'main;
                }
                ensure_read_room(&mut buf);
                match tokio::time::timeout(GATE_PROBE, read_half.read_buf(&mut buf)).await {
                    Ok(Ok(0)) | Ok(Err(_)) => break 'main,
                    Ok(Ok(n)) => {
                        stats::add(&shared.wstats.bytes_in, n as u64);
                        if buf.len() > shared.cfg.query_buffer_limit {
                            session.emit_error("ERR query buffer exceeds limit");
                            break 'main;
                        }
                    }
                    _ => {}
                }
            }
            continue 'main;
        }
        if buf.len() > shared.cfg.query_buffer_limit {
            session.emit_error("ERR query buffer exceeds limit");
            break;
        }
        ensure_read_room(&mut buf);
        // only a relay can close the session while the read is parked
        let read = if session.has_relay.get() {
            tokio::select! {
                _ = link.closed_notify.notified() => break,
                r = read_half.read_buf(&mut buf) => r,
            }
        } else {
            read_half.read_buf(&mut buf).await
        };
        match read {
            Ok(0) | Err(_) => break,
            Ok(n) => stats::add(&shared.wstats.bytes_in, n as u64),
        }
    }
    stats::bump(&shared.wstats.readers_exited);
    session.stop_pubsub();
    for (seq, task) in session.blocking.borrow_mut().drain(..) {
        if task.is_finished() {
            continue;
        }
        task.abort();
        let _ = reply_q.send(Reply::At(seq, Bytes::from_static(ERR_BACKEND_LOST)));
    }
    let final_seq = session.link.next_seq.get();
    drop(session);
    drop(reply_q);
    let _ = close_tx.send(final_seq);
    let _ = writer.await;
    shared.inflight.set(
        shared
            .inflight
            .get()
            .saturating_sub(final_seq.saturating_sub(link.emitted.get())),
    );
    stats::bump(&shared.wstats.sessions_closed);
}

pub(super) fn eval_numkeys(args: &mut resp::Args<'_>) -> Option<i64> {
    args.nth(2)
        .and_then(|a| std::str::from_utf8(a).ok())
        .and_then(|s| s.parse().ok())
}

// the byte range of an argument borrowed from its frame
fn span(frame: &[u8], arg: &[u8]) -> Range<usize> {
    let start = arg.as_ptr() as usize - frame.as_ptr() as usize;
    start..start + arg.len()
}

//! Client sessions: dispatch, ordered replies, MULTI, pubsub, redirects.

use std::cell::{Cell, RefCell};
use std::collections::{HashSet, VecDeque};
use std::io::IoSlice;
use std::rc::Rc;

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, oneshot};

use crate::backend::{ASKING_FRAME, Backends, ERR_BACKEND_LOST, Outbound, Sink, ensure_read_room};
use crate::command::{self, Kind, Spec};
use crate::log_debug;
use crate::multikey;
use crate::resp::{self, ReqScan};
use crate::stats::{self, Stats};
use crate::topology::Topology;
use crate::{admin, crc16, route};

const MAX_INFLIGHT: usize = 65536;
const SUBS_LIMIT: usize = 32768;
const PUBSUB_FORWARD_QUEUE: usize = 64;
const PUBSUB_PUSH_WINDOW: usize = 4096;
const GATE_PROBE: std::time::Duration = std::time::Duration::from_millis(100);

const ERR_NOAUTH: &[u8] = b"-NOAUTH Authentication required.\r\n";
const ERR_CROSSSLOT: &[u8] = b"-CROSSSLOT Keys in request don't hash to the same slot\r\n";
const ERR_NO_OWNER: &[u8] = b"-CLUSTERDOWN Hash slot not served\r\n";
const ERR_TRYAGAIN: &[u8] = b"-TRYAGAIN slot is migrating, retry later\r\n";

/// Everything a session needs from its worker.
pub struct Shared {
    pub cfg: Rc<crate::config::Config>,
    pub topo: std::sync::Arc<ArcSwap<Topology>>,
    pub backends: Rc<Backends>,
    pub stats: std::sync::Arc<Stats>,
    pub worker: usize,
    pub refresh: mpsc::UnboundedSender<()>,
    pub started: u64,
}

/// One frame travelling to the client writer.
pub enum Reply {
    /// Ordered reply at its sequence.
    At(u64, Bytes),
    /// Pubsub confirmation at its sequence, emitted as a push frame.
    Ack(u64, Bytes),
    /// Out-of-band push, never emitted before the ack it followed.
    Push { after: Option<u64>, frame: Bytes },
    /// Closes the client connection once the pending batch is flushed.
    Close,
}

pub type ReplyTx = mpsc::UnboundedSender<Reply>;

struct InFlight {
    seq: u64,
    frame: Bytes,
    expect: u32,
    retried: bool,
}

// sequences are allocated monotonically, so the ring stays sorted
type InflightRing = RefCell<VecDeque<InFlight>>;

// state shared between a session's reader, writer, and pubsub relay
#[derive(Default)]
struct WriterLink {
    inflight: InflightRing,
    emitted: Cell<u64>,
    // set when no reply can ever be written again; reader must stop dispatching
    closed: Cell<bool>,
    closed_notify: tokio::sync::Notify,
    // a live relay can close the session while the reader is parked reading
    has_relay: Cell<bool>,
    proto_switches: ProtoSwitchQueue,
    oob_budget: Cell<usize>,
    oob_notify: tokio::sync::Notify,
    // pre-allocated sequences for pending pubsub confirmations, in order
    ack_seqs: RefCell<VecDeque<u64>>,
    acks_drained: tokio::sync::Notify,
}
// pending protocol flips; `armed` keeps the hot path off the RefCell
#[derive(Default)]
struct ProtoSwitchQueue {
    armed: Cell<usize>,
    queue: RefCell<VecDeque<(u64, u8)>>,
}

impl ProtoSwitchQueue {
    fn push(&self, at: u64, proto: u8) {
        self.queue.borrow_mut().push_back((at, proto));
        self.armed.set(self.armed.get() + 1);
    }

    fn apply(&self, seq: u64, cur: &mut u8) {
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

struct Session {
    shared: Rc<Shared>,
    id: u64,
    reply_tx: Rc<ReplyTx>,
    link: Rc<WriterLink>,
    proto: Cell<u8>,
    authed: Cell<bool>,
    name: RefCell<String>,
    next_seq: Cell<u64>,
    rng: Cell<u64>,
    multi: RefCell<Option<MultiState>>,
    pubsub: RefCell<Option<PubsubHandle>>,
    subs: RefCell<PubsubSim>,
    blocking: RefCell<Vec<(u64, tokio::task::JoinHandle<()>)>>,
    closing: Cell<bool>,
    conns: RefCell<ConnCache>,
    topo_cache: RefCell<std::sync::Arc<Topology>>,
}

impl Session {
    async fn dispatch(&self, frame: Bytes, argc: usize) {
        // a command must not commit effects its client can never observe
        if self.link.closed.get() {
            self.closing.set(true);
            return;
        }
        stats::bump(&self.shared.stats.workers[self.shared.worker].commands);
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
        if self.pubsub.borrow().is_some() {
            if !self.exit_pubsub_if_done().await {
                let passthrough = self.proto.get() >= 3 && !pubsub_allowed(spec);
                if !passthrough {
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
        if self.multi.borrow().is_some() && spec.flags & command::FLAG_TXN_CTRL == 0 {
            self.queue_multi(spec, frame, argc);
            return;
        }
        match spec.kind {
            Kind::Single => match it.nth(spec.first_key as usize - 1).map(crc16::slot) {
                Some(slot) => {
                    if let Some(cold) = self.route_single(slot, spec.is_readonly(), frame) {
                        let (conn, out) = *cold;
                        conn.send_wait(out).await;
                    }
                }
                None => self.emit_error("ERR missing key"),
            },
            Kind::Local => self.handle_local(spec, frame, argc),
            Kind::Exec => Box::pin(self.handle_exec()).await,
            Kind::AnyMaster => self.forward_any_master(frame).await,
            Kind::MultiSum | Kind::Mget | Kind::Mset => self.fan_out(spec, &frame, argc),
            Kind::Blocking => self.forward_blocking(spec, frame, argc),
            Kind::Eval => Box::pin(self.forward_eval(frame, argc)).await,
            Kind::Xread => Box::pin(self.forward_xread(spec, frame, argc)).await,
            Kind::Subscribe => self.enter_pubsub(spec, frame, argc),
            Kind::Scan => self.run_scan(frame, argc),
            Kind::Dbsize => self.run_broadcast(frame, true),
            Kind::Flushall => self.run_broadcast(frame, false),
        }
    }

    // one relaxed epoch load replaces the arc-swap hazard load on the hot path
    fn topo(&self) -> std::cell::Ref<'_, std::sync::Arc<Topology>> {
        if crate::server::topo_epoch() != self.topo_cache.borrow().epoch {
            *self.topo_cache.borrow_mut() = self.shared.topo.load_full();
        }
        self.topo_cache.borrow()
    }

    fn key_slot(&self, frame: &Bytes, argc: usize, key_index: usize) -> Option<u16> {
        let mut it = resp::Args::new(frame, argc);
        it.nth(key_index).map(crc16::slot)
    }

    // one Vec index per request once warm; re-resolves on epoch or death
    fn cached_conn(&self, topo: &Topology, idx: u16, is_replica: bool) -> Rc<crate::backend::Conn> {
        let mut cache = self.conns.borrow_mut();
        if cache.epoch != topo.epoch {
            cache.epoch = topo.epoch;
            cache.by_node.clear();
        }
        if cache.by_node.len() < topo.nodes.len() {
            cache.by_node.resize(topo.nodes.len(), None);
        }
        let entry = &mut cache.by_node[idx as usize];
        if let Some(conn) = entry
            && !conn.is_dead()
        {
            return conn.clone();
        }
        let conn = self
            .shared
            .backends
            .shared(&topo.nodes[idx as usize].addr, self.id, is_replica);
        *entry = Some(conn.clone());
        conn
    }

    async fn forward_any_master(&self, frame: Bytes) {
        let seq = self.alloc_seq();
        let conn = {
            let topo = self.topo();
            let picked = self.with_rng(|r| route::any_master(&topo, r));
            let Some(idx) = picked else {
                self.emit_at(seq, Bytes::from_static(ERR_NO_OWNER));
                return;
            };
            self.cached_conn(&topo, idx, false)
        };
        conn.send(self.client_out(seq, frame)).await;
    }

    fn any_master_addr(&self) -> Option<String> {
        let topo = self.topo();
        let idx = self.with_rng(|r| route::any_master(&topo, r))?;
        Some(topo.nodes[idx as usize].addr.clone())
    }

    fn with_rng<R>(&self, f: impl FnOnce(&mut u64) -> R) -> R {
        let mut rng = self.rng.get();
        let out = f(&mut rng);
        self.rng.set(rng);
        out
    }

    // resolves a slot's master connection, emitting ERR_NO_OWNER at seq if unowned
    fn owner_conn(&self, seq: u64, slot: Option<u16>) -> Option<Rc<crate::backend::Conn>> {
        let topo = self.topo();
        let Some(idx) = slot.and_then(|sl| topo.owner(sl)) else {
            self.emit_at(seq, Bytes::from_static(ERR_NO_OWNER));
            return None;
        };
        Some(self.cached_conn(&topo, idx, false))
    }

    // sync fast path: the returned pair is the rare full-queue leftover to await
    fn route_single(
        &self,
        slot: u16,
        readonly: bool,
        frame: Bytes,
    ) -> Option<Box<(Rc<crate::backend::Conn>, Outbound)>> {
        let seq = self.alloc_seq();
        let conn = {
            let topo = self.topo();
            let picked = self
                .with_rng(|r| route::pick(&topo, slot, readonly, self.shared.cfg.slave_mode, r));
            let Some((idx, is_replica)) = picked else {
                self.emit_at(seq, Bytes::from_static(ERR_NO_OWNER));
                return None;
            };
            self.cached_conn(&topo, idx, is_replica)
        };
        self.track_inflight(seq, &frame, 1);
        match conn.try_send(self.client_out(seq, frame)) {
            Ok(()) => None,
            Err(out) => Some(Box::new((conn, out))),
        }
    }

    fn client_out(&self, seq: u64, frame: Bytes) -> Outbound {
        Outbound {
            head: None,
            frame,
            expect: 1,
            sink: Sink::Client(self.reply_tx.clone(), seq),
        }
    }

    fn track_inflight(&self, seq: u64, frame: &Bytes, expect: u32) {
        self.link.inflight.borrow_mut().push_back(InFlight {
            seq,
            frame: frame.clone(),
            expect,
            retried: false,
        });
    }

    async fn forward_eval(&self, frame: Bytes, argc: usize) {
        let (numkeys, slot) = {
            let args = collect_args(&frame, argc);
            let numkeys: i64 = std::str::from_utf8(args[2])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(-1);
            let slot = if numkeys >= 1 && argc > 3 {
                Some(crc16::slot(args[3]))
            } else {
                None
            };
            (numkeys, slot)
        };
        if numkeys < 0 || 3 + numkeys as usize > argc {
            self.emit_error("ERR Number of keys can't be negative");
            return;
        }
        if numkeys == 0 {
            self.forward_any_master(frame).await;
            return;
        }
        let seq = self.alloc_seq();
        let Some(conn) = self.owner_conn(seq, slot) else {
            return;
        };
        self.track_inflight(seq, &frame, 1);
        conn.send(self.client_out(seq, frame)).await;
    }

    async fn forward_xread(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let parsed = {
            let args = collect_args(&frame, argc);
            let pos = args.iter().position(|a| a.eq_ignore_ascii_case(b"streams"));
            match pos {
                None => None,
                Some(p) => {
                    let tail = argc - p - 1;
                    if tail == 0 || !tail.is_multiple_of(2) {
                        None
                    } else {
                        let blocking = args
                            .iter()
                            .take(p)
                            .any(|a| a.eq_ignore_ascii_case(b"block"));
                        Some((crc16::slot(args[p + 1]), blocking))
                    }
                }
            }
        };
        let Some((slot, blocking)) = parsed else {
            self.emit_error("ERR Unbalanced XREAD list of streams");
            return;
        };
        if blocking {
            self.spawn_blocking(slot, frame);
            return;
        }
        if let Some(cold) = self.route_single(slot, spec.is_readonly(), frame) {
            let (conn, out) = *cold;
            conn.send_wait(out).await;
        }
    }

    fn forward_blocking(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let Some(slot) = self.key_slot(&frame, argc, spec.first_key as usize) else {
            self.emit_error("ERR missing key");
            return;
        };
        self.spawn_blocking(slot, frame);
    }

    fn spawn_blocking(&self, slot: u16, frame: Bytes) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let task = tokio::task::spawn_local(async move {
            let reply = blocking_round(&shared, slot, frame, None, false).await;
            let _ = reply_tx.send(Reply::At(seq, reply));
        });
        let mut blocking = self.blocking.borrow_mut();
        blocking.retain(|(_, t)| !t.is_finished());
        blocking.push((seq, task));
    }

    fn fan_out(&self, spec: &Spec, frame: &Bytes, argc: usize) {
        let seq = self.alloc_seq();
        let readonly = spec.is_readonly();
        let mode = self.shared.cfg.slave_mode;
        if spec.kind == Kind::Mset && !(argc - 1).is_multiple_of(2) {
            self.emit_at(
                seq,
                error_frame("ERR wrong number of arguments for 'mset' command"),
            );
            return;
        }
        let split = {
            let args = collect_args(frame, argc);
            let mut keys: Vec<&[u8]> = Vec::with_capacity(argc);
            let mut values: Option<Vec<&[u8]>> =
                (spec.step == 2).then(|| Vec::with_capacity(argc / 2));
            for i in key_indices(spec, argc) {
                keys.push(args[i]);
                if let Some(vals) = values.as_mut() {
                    vals.push(args[i + 1]);
                }
            }
            let total = keys.len();
            let topo = self.shared.topo.load_full();
            let parts = self.with_rng(|rng| {
                multikey::split(spec.name.as_bytes(), &keys, values.as_deref(), |slot| {
                    route::pick(&topo, slot, readonly, mode, rng)
                })
            });
            parts.map(|p| (p, total, topo))
        };
        let (parts, total, topo) = match split {
            Ok(v) => v,
            Err(e) => {
                self.emit_at(seq, error_frame(&format!("CLUSTERDOWN {e}")));
                return;
            }
        };
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let id = self.id;
        let merge = match spec.kind {
            Kind::Mget => Merge::Mget,
            Kind::Mset => Merge::Ok,
            _ => Merge::Sum,
        };
        // detached deliberately: completion is bounded by backend replies.
        tokio::task::spawn_local(async move {
            let mut receivers = Vec::with_capacity(parts.len());
            for part in &parts {
                let addr = &topo.nodes[part.node as usize].addr;
                // the clone retains the frame for a possible redirect resend
                let rx =
                    scatter_one(&shared, addr, id, part.readonly, None, part.frame.clone()).await;
                receivers.push(rx);
            }
            let mut results: Vec<(Vec<usize>, Bytes)> = Vec::with_capacity(parts.len());
            let mut redirected: Vec<(multikey::Part, bool, String)> = Vec::new();
            for (part, rx) in parts.into_iter().zip(receivers) {
                let reply = recv_or_lost(rx).await;
                // a redirected part executed nothing: one resend is idempotent
                match parse_redirect(&reply) {
                    Some((ask, target)) => redirected.push((part, ask, target)),
                    None => results.push((part.positions, reply)),
                }
            }
            if !redirected.is_empty() {
                let _ = shared.refresh.send(());
                let mut retries = Vec::with_capacity(redirected.len());
                for (part, ask, target) in &mut redirected {
                    let head = ask.then(|| Bytes::from_static(ASKING_FRAME));
                    let frame = std::mem::take(&mut part.frame);
                    let rx = scatter_one(&shared, target, id, false, head, frame).await;
                    retries.push(rx);
                }
                for ((part, _, _), rx) in redirected.into_iter().zip(retries) {
                    let reply = recv_or_lost(rx).await;
                    results.push((part.positions, reply));
                }
            }
            let merged = match merge {
                Merge::Mget => multikey::merge_mget(total, &results),
                Merge::Ok => multikey::merge_ok(results.iter().map(|(_, r)| r)),
                Merge::Sum => multikey::merge_sum(results.iter().map(|(_, r)| r)),
            };
            let _ = reply_tx.send(Reply::At(seq, merged.unwrap_or_else(|e| e)));
        });
    }

    fn run_scan(&self, frame: Bytes, argc: usize) {
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
        let reply_tx = self.reply_tx.clone();
        let id = self.id;
        // detached deliberately: completion is bounded by backend replies.
        tokio::task::spawn_local(async move {
            let topo = shared.topo.load_full();
            if master_idx >= topo.masters.len() {
                let done = multikey::rebuild_scan_reply(0, b"*0\r\n");
                let _ = reply_tx.send(Reply::At(seq, Bytes::from(done)));
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
            let rx = scatter_one(&shared, addr, id, false, None, Bytes::from(cmd)).await;
            let reply = recv_or_lost(rx).await;
            let out = match multikey::parse_scan_reply(&reply) {
                Some((next, keys)) => {
                    let n_masters = shared.topo.load().masters.len();
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
            let _ = reply_tx.send(Reply::At(seq, out));
        });
    }

    fn run_broadcast(&self, frame: Bytes, sum: bool) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let id = self.id;
        // detached deliberately: completion is bounded by backend replies.
        tokio::task::spawn_local(async move {
            let topo = shared.topo.load_full();
            let mut receivers = Vec::with_capacity(topo.masters.len());
            for &midx in &topo.masters {
                let addr = &topo.nodes[midx as usize].addr;
                let rx = scatter_one(&shared, addr, id, false, None, frame.clone()).await;
                receivers.push(rx);
            }
            let mut replies: Vec<Bytes> = Vec::with_capacity(receivers.len());
            for rx in receivers {
                replies.push(recv_or_lost(rx).await);
            }
            let merged = if sum {
                multikey::merge_sum(replies.iter())
            } else {
                multikey::merge_ok(replies.iter())
            };
            let _ = reply_tx.send(Reply::At(seq, merged.unwrap_or_else(|e| e)));
        });
    }

    fn queue_multi(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let queueable = matches!(
            spec.kind,
            Kind::Single | Kind::MultiSum | Kind::Mget | Kind::Mset
        );
        if !queueable {
            if let Some(state) = self.multi.borrow_mut().as_mut() {
                state.aborted = true;
            }
            self.emit_error(&format!("ERR {} is not allowed in transactions", spec.name));
            return;
        }
        let new_slot = {
            let args = collect_args(&frame, argc);
            let current = self.multi.borrow().as_ref().and_then(|s| s.slot);
            let mut slot = current;
            let mut conflict = false;
            for idx in key_indices(spec, argc) {
                let s = crc16::slot(args[idx]);
                match slot {
                    None => slot = Some(s),
                    Some(prev) if prev != s => {
                        conflict = true;
                        break;
                    }
                    Some(_) => {}
                }
            }
            if conflict { None } else { slot }
        };
        let mut guard = self.multi.borrow_mut();
        let Some(state) = guard.as_mut() else {
            return;
        };
        match new_slot {
            None => {
                state.aborted = true;
                drop(guard);
                self.emit_error_frame(Bytes::from_static(ERR_CROSSSLOT));
            }
            Some(_) if state.bytes + frame.len() > self.shared.cfg.query_buffer_limit => {
                state.aborted = true;
                drop(guard);
                self.emit_error("ERR transaction exceeds query buffer limit");
            }
            Some(slot) => {
                state.slot = Some(slot);
                state.bytes += frame.len();
                state.frames.push(frame);
                drop(guard);
                self.emit_local(Bytes::from_static(b"+QUEUED\r\n"));
            }
        }
    }

    fn handle_local(&self, spec: &Spec, frame: Bytes, argc: usize) {
        if spec.name == "ping" && argc == 1 {
            self.emit_local(Bytes::from_static(b"+PONG\r\n"));
            return;
        }
        let reply: Option<Bytes> = {
            let args = collect_args(&frame, argc);
            match spec.name {
                "ping" => Some(Bytes::from(admin::ping(&args))),
                "echo" => Some(Bytes::from(admin::echo(&args))),
                "select" => Some(Bytes::from(admin::select(&args))),
                "time" => Some(Bytes::from(admin::time())),
                "info" => Some(Bytes::from(admin::info(
                    &self.shared.cfg,
                    &self.shared.stats,
                    self.shared.started,
                ))),
                "config" => Some(Bytes::from(admin::config_cmd(&args, &self.shared.cfg))),
                "cluster" => Some(Bytes::from(admin::cluster(
                    &args,
                    &self.shared.cfg,
                    self.proto.get(),
                ))),
                "command" => Some(Bytes::from(admin::command_reply(
                    &args,
                    command::table(),
                    self.proto.get(),
                ))),
                "auth" => {
                    self.handle_auth(&args);
                    None
                }
                "hello" => {
                    self.handle_hello(&args);
                    None
                }
                "acl" => match args.get(1).map(|s| s.to_ascii_lowercase()).as_deref() {
                    Some(b"whoami") => {
                        let mut out = Vec::new();
                        admin::bulk(&mut out, b"default");
                        Some(Bytes::from(out))
                    }
                    _ => Some(error_frame("ERR unsupported ACL subcommand")),
                },
                "client" => {
                    self.handle_client_cmd(&args);
                    None
                }
                "quit" => {
                    self.closing.set(true);
                    Some(Bytes::from_static(admin::OK))
                }
                "reset" => {
                    self.do_reset();
                    Some(Bytes::from_static(b"+RESET\r\n"))
                }
                "multi" => {
                    if self.multi.borrow().is_some() {
                        Some(error_frame("ERR MULTI calls can not be nested"))
                    } else {
                        *self.multi.borrow_mut() = Some(MultiState {
                            slot: None,
                            frames: Vec::new(),
                            bytes: 0,
                            aborted: false,
                        });
                        Some(Bytes::from_static(admin::OK))
                    }
                }
                "discard" => {
                    if self.multi.borrow_mut().take().is_some() {
                        Some(Bytes::from_static(admin::OK))
                    } else {
                        Some(error_frame("ERR DISCARD without MULTI"))
                    }
                }
                _ => Some(error_frame("ERR unsupported command")),
            }
        };
        if let Some(bytes) = reply {
            self.emit_local(bytes);
        }
    }

    async fn handle_exec(&self) {
        let state = self.multi.borrow_mut().take();
        let Some(state) = state else {
            self.emit_error("ERR EXEC without MULTI");
            return;
        };
        if state.aborted {
            self.emit_error("EXECABORT Transaction discarded because of previous errors.");
            return;
        }
        if state.frames.is_empty() {
            self.emit_local(Bytes::from_static(b"*0\r\n"));
            return;
        }
        let seq = self.alloc_seq();
        let Some(conn) = self.owner_conn(seq, state.slot) else {
            return;
        };
        let body: usize = state.frames.iter().map(Bytes::len).sum();
        let mut blob = Vec::with_capacity(body + 32);
        blob.extend_from_slice(b"*1\r\n$5\r\nMULTI\r\n");
        for f in &state.frames {
            blob.extend_from_slice(f);
        }
        blob.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
        let expect = state.frames.len() as u32 + 2;
        let blob = Bytes::from(blob);
        self.track_inflight(seq, &blob, expect);
        conn.send(Outbound {
            head: None,
            frame: blob,
            expect,
            sink: Sink::Client(self.reply_tx.clone(), seq),
        })
        .await;
    }

    fn handle_auth(&self, args: &[&[u8]]) {
        let pass = self.shared.cfg.requirepass.as_bytes();
        if pass.is_empty() {
            self.emit_error("ERR Client sent AUTH, but no password is set");
            return;
        }
        let given = match args.len() {
            2 => Some(args[1]),
            3 if args[1] == b"default" => Some(args[2]),
            _ => None,
        };
        if given == Some(pass) {
            self.authed.set(true);
            self.emit_local(Bytes::from_static(admin::OK));
        } else {
            self.emit_error("WRONGPASS invalid username-password pair or user is disabled.");
        }
    }

    fn handle_hello(&self, args: &[&[u8]]) {
        let mut proto = self.proto.get();
        let mut i = 1;
        if let Some(ver) = args.get(1)
            && !ver.eq_ignore_ascii_case(b"auth")
            && !ver.eq_ignore_ascii_case(b"setname")
        {
            match *ver {
                b"2" => proto = 2,
                b"3" => proto = 3,
                _ => {
                    self.emit_error("NOPROTO unsupported protocol version");
                    return;
                }
            }
            i = 2;
        }
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"auth") {
                if i + 2 >= args.len() {
                    self.emit_error("ERR syntax error in HELLO");
                    return;
                }
                let (user, pass) = (args[i + 1], args[i + 2]);
                let expected = self.shared.cfg.requirepass.as_bytes();
                if expected.is_empty() || (user == b"default" && pass == expected) {
                    self.authed.set(true);
                } else {
                    self.emit_error(
                        "WRONGPASS invalid username-password pair or user is disabled.",
                    );
                    return;
                }
                i += 3;
            } else if args[i].eq_ignore_ascii_case(b"setname") && i + 1 < args.len() {
                *self.name.borrow_mut() = String::from_utf8_lossy(args[i + 1]).into_owned();
                i += 2;
            } else {
                self.emit_error("ERR syntax error in HELLO");
                return;
            }
        }
        if !self.authed.get() {
            self.emit_error(
                "NOAUTH HELLO must be called with the client already authenticated, \
                 otherwise the HELLO <proto> AUTH <user> <pass> option can be used",
            );
            return;
        }
        self.proto.set(proto);
        self.link.proto_switches.push(self.next_seq.get(), proto);
        let mut out = Vec::new();
        if proto >= 3 {
            out.extend_from_slice(b"%7\r\n");
        } else {
            out.extend_from_slice(b"*14\r\n");
        }
        admin::bulk(&mut out, b"server");
        admin::bulk(&mut out, b"redis");
        admin::bulk(&mut out, b"version");
        admin::bulk(&mut out, admin::SERVER_VERSION.as_bytes());
        admin::bulk(&mut out, b"proto");
        admin::integer(&mut out, i64::from(proto));
        admin::bulk(&mut out, b"id");
        admin::integer(&mut out, self.id as i64);
        admin::bulk(&mut out, b"mode");
        admin::bulk(&mut out, b"cluster");
        admin::bulk(&mut out, b"role");
        admin::bulk(&mut out, b"master");
        admin::bulk(&mut out, b"modules");
        out.extend_from_slice(b"*0\r\n");
        self.emit_local(out);
    }

    fn handle_client_cmd(&self, args: &[&[u8]]) {
        match args.get(1).map(|s| s.to_ascii_lowercase()).as_deref() {
            Some(b"id") => {
                let mut out = Vec::new();
                admin::integer(&mut out, self.id as i64);
                self.emit_local(out);
            }
            Some(b"setname") if args.len() == 3 => {
                *self.name.borrow_mut() = String::from_utf8_lossy(args[2]).into_owned();
                self.emit_local(Bytes::from_static(admin::OK));
            }
            Some(b"getname") => {
                let mut out = Vec::new();
                admin::bulk(&mut out, self.name.borrow().as_bytes());
                self.emit_local(out);
            }
            _ => self.emit_error("ERR unsupported CLIENT subcommand"),
        }
    }

    // drains already-promised confirmations before dropping the relay
    async fn exit_pubsub_if_done(&self) -> bool {
        if !self.relay_dead() {
            if !self.subs.borrow().is_empty() {
                return false;
            }
            while !self.link.ack_seqs.borrow().is_empty() {
                if self.relay_dead() || self.link.closed.get() {
                    break;
                }
                tokio::select! {
                    _ = self.link.acks_drained.notified() => {}
                    _ = self.reply_tx.closed() => break,
                }
            }
        }
        self.stop_pubsub();
        true
    }

    fn relay_dead(&self) -> bool {
        self.pubsub
            .borrow()
            .as_ref()
            .is_none_or(|ps| ps.task.is_finished())
    }

    fn stop_pubsub(&self) {
        self.link.has_relay.set(false);
        if let Some(ps) = self.pubsub.borrow_mut().take() {
            ps.task.abort();
        }
        *self.subs.borrow_mut() = PubsubSim::default();
        backfill_acks(&self.link, &self.reply_tx);
    }

    fn dispatch_pubsub(&self, spec: &Spec, frame: Bytes, argc: usize) {
        match spec.name {
            "quit" => {
                self.closing.set(true);
                self.emit_local(Bytes::from_static(admin::OK));
                return;
            }
            "reset" => {
                self.stop_pubsub();
                self.do_reset();
                self.emit_local(Bytes::from_static(b"+RESET\r\n"));
                return;
            }
            _ if self.pubsub_overflow(spec, &frame, argc) => {
                self.emit_error("ERR pubsub confirmation backlog exceeds limit");
                return;
            }
            "ping" => self.promise_acks(1),
            _ if spec.kind == Kind::Subscribe => self.promise_subscription(&frame, argc),
            _ => {
                self.emit_error(
                    "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET \
                     are allowed in this context",
                );
                return;
            }
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

    // promised confirmations occupy the reply window until emitted: bound them
    fn pubsub_overflow(&self, spec: &Spec, frame: &Bytes, argc: usize) -> bool {
        let subs = self.subs.borrow();
        let expected = subs.ack_count(spec.name.as_bytes(), argc);
        let outstanding = self.next_seq.get().saturating_sub(self.link.emitted.get()) as usize;
        if outstanding + expected > MAX_INFLIGHT {
            return true;
        }
        if !matches!(spec.name, "subscribe" | "psubscribe") {
            return false;
        }
        let target = if spec.name == "psubscribe" {
            &subs.patterns
        } else {
            &subs.channels
        };
        let mut grown = subs.channels.len() + subs.patterns.len();
        let mut fresh: HashSet<&[u8]> = HashSet::new();
        for a in resp::Args::new(frame, argc).skip(1) {
            if !target.contains(a) && fresh.insert(a) {
                grown += 1;
            }
        }
        grown > SUBS_LIMIT
    }

    fn promise_subscription(&self, frame: &Bytes, argc: usize) {
        let acks = {
            let args = collect_args(frame, argc);
            self.subs.borrow_mut().apply(args[0], &args)
        };
        self.promise_acks(acks);
    }

    fn promise_acks(&self, n: usize) {
        let mut seqs = self.link.ack_seqs.borrow_mut();
        for _ in 0..n {
            seqs.push_back(self.alloc_seq());
        }
    }

    fn enter_pubsub(&self, spec: &Spec, first_frame: Bytes, argc: usize) {
        if self.pubsub_overflow(spec, &first_frame, argc) {
            self.emit_error("ERR pubsub confirmation backlog exceeds limit");
            return;
        }
        let Some(addr) = self.any_master_addr() else {
            self.emit_error_frame(Bytes::from_static(ERR_NO_OWNER));
            return;
        };
        self.promise_subscription(&first_frame, argc);
        self.link.has_relay.set(true);
        let (tx, rx) = mpsc::channel::<Bytes>(PUBSUB_FORWARD_QUEUE);
        let _ = tx.try_send(first_frame);
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let link = self.link.clone();
        let task = tokio::task::spawn_local(async move {
            pubsub_relay(shared, addr, rx, reply_tx, link).await;
        });
        *self.pubsub.borrow_mut() = Some(PubsubHandle { tx, task });
    }

    fn do_reset(&self) {
        *self.multi.borrow_mut() = None;
        self.proto.set(2);
        self.link.proto_switches.push(self.next_seq.get(), 2);
        self.authed.set(self.shared.cfg.requirepass.is_empty());
    }

    fn abort_multi(&self) {
        if let Some(state) = self.multi.borrow_mut().as_mut() {
            state.aborted = true;
        }
    }

    fn window_full(&self) -> bool {
        self.next_seq.get().saturating_sub(self.link.emitted.get()) > MAX_INFLIGHT as u64
    }

    fn alloc_seq(&self) -> u64 {
        let seq = self.next_seq.get();
        self.next_seq.set(seq + 1);
        seq
    }

    fn emit_local(&self, bytes: impl Into<Bytes>) {
        let seq = self.alloc_seq();
        self.emit_at(seq, bytes.into());
    }

    fn emit_error(&self, msg: &str) {
        self.emit_error_frame(error_frame(msg));
    }

    fn emit_error_frame(&self, frame: Bytes) {
        stats::bump(&self.shared.stats.workers[self.shared.worker].errors);
        self.emit_local(frame);
    }

    fn emit_at(&self, seq: u64, frame: Bytes) {
        let _ = self.reply_tx.send(Reply::At(seq, frame));
    }
}

// per-session resolved connections, valid for one topology epoch
struct ConnCache {
    epoch: u64,
    by_node: Vec<Option<Rc<crate::backend::Conn>>>,
}

struct MultiState {
    slot: Option<u16>,
    frames: Vec<Bytes>,
    bytes: usize,
    aborted: bool,
}

struct PubsubHandle {
    tx: mpsc::Sender<Bytes>,
    task: tokio::task::JoinHandle<()>,
}

// reader-side subscription mirror; confirmation counts derive from it
#[derive(Default)]
struct PubsubSim {
    channels: HashSet<Vec<u8>>,
    patterns: HashSet<Vec<u8>>,
}

impl PubsubSim {
    fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty()
    }

    // acks per command: named channels, or the matching set for a bare unsubscribe
    fn ack_count(&self, name: &[u8], argc: usize) -> usize {
        if argc > 1 {
            argc - 1
        } else if name.eq_ignore_ascii_case(b"unsubscribe") {
            self.channels.len().max(1)
        } else if name.eq_ignore_ascii_case(b"punsubscribe") {
            self.patterns.len().max(1)
        } else {
            1
        }
    }

    fn apply(&mut self, name: &[u8], args: &[&[u8]]) -> usize {
        let acks = self.ack_count(name, args.len());
        let target = if name.eq_ignore_ascii_case(b"psubscribe")
            || name.eq_ignore_ascii_case(b"punsubscribe")
        {
            &mut self.patterns
        } else {
            &mut self.channels
        };
        if name.eq_ignore_ascii_case(b"subscribe") || name.eq_ignore_ascii_case(b"psubscribe") {
            for a in &args[1..] {
                target.insert(a.to_vec());
            }
        } else if args.len() > 1 {
            for a in &args[1..] {
                target.remove(*a);
            }
        } else {
            target.clear();
        }
        acks
    }
}

enum Merge {
    Mget,
    Sum,
    Ok,
}

/// Serves one client connection to completion.
pub async fn serve(shared: Rc<Shared>, stream: TcpStream, id: u64) {
    if stream.set_nodelay(true).is_err() {
        return;
    }
    let (mut read_half, write_half) = stream.into_split();
    let (reply_tx, reply_rx) = mpsc::unbounded_channel();
    let reply_tx = Rc::new(reply_tx);
    let link: Rc<WriterLink> = Rc::new(WriterLink::default());

    let session = Session {
        shared: shared.clone(),
        id,
        reply_tx: reply_tx.clone(),
        link: link.clone(),
        proto: Cell::new(2),
        authed: Cell::new(shared.cfg.requirepass.is_empty()),
        name: RefCell::new(String::new()),
        next_seq: Cell::new(0),
        rng: Cell::new(id | 1),
        multi: RefCell::new(None),
        pubsub: RefCell::new(None),
        subs: RefCell::new(PubsubSim::default()),
        blocking: RefCell::new(Vec::new()),
        closing: Cell::new(false),
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
        reply_rx,
        reply_tx.clone(),
        close_rx,
        link.clone(),
        id,
    ));

    let mut buf = BytesMut::with_capacity(crate::backend::READ_INIT);
    'main: loop {
        // a closing session must not let a half-open client keep executing writes
        if link.closed.get() {
            break;
        }
        loop {
            if session.window_full() {
                break;
            }
            match resp::scan_request(&buf) {
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
                        stats::add(&shared.stats.workers[shared.worker].bytes_in, n as u64);
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
        // only a relay can close the session while the read is parked; a dead
        // writer without one implies a dead socket the read observes itself
        let read = if link.has_relay.get() {
            tokio::select! {
                _ = link.closed_notify.notified() => break,
                r = read_half.read_buf(&mut buf) => r,
            }
        } else {
            read_half.read_buf(&mut buf).await
        };
        match read {
            Ok(0) | Err(_) => break,
            Ok(n) => stats::add(&shared.stats.workers[shared.worker].bytes_in, n as u64),
        }
    }
    stats::bump(&shared.stats.workers[shared.worker].readers_exited);
    session.stop_pubsub();
    for (seq, task) in session.blocking.borrow_mut().drain(..) {
        if task.is_finished() {
            continue;
        }
        task.abort();
        let _ = reply_tx.send(Reply::At(seq, Bytes::from_static(ERR_BACKEND_LOST)));
    }
    let final_seq = session.next_seq.get();
    drop(session);
    drop(reply_tx);
    let _ = close_tx.send(final_seq);
    let _ = writer.await;
    stats::bump(&shared.stats.workers[shared.worker].sessions_closed);
}

fn collect_args(frame: &Bytes, argc: usize) -> Vec<&[u8]> {
    resp::Args::new(frame, argc).collect()
}

// argument indices holding keys, per the spec's first/last/step triple
fn key_indices(spec: &Spec, argc: usize) -> impl Iterator<Item = usize> {
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

async fn scatter_one(
    shared: &Rc<Shared>,
    addr: &str,
    id: u64,
    readonly: bool,
    head: Option<Bytes>,
    frame: Bytes,
) -> oneshot::Receiver<Bytes> {
    let (tx, rx) = oneshot::channel();
    let expect = 1 + u32::from(head.is_some());
    let conn = shared.backends.shared(addr, id, readonly);
    conn.send(Outbound {
        head,
        frame,
        expect,
        sink: Sink::One(tx),
    })
    .await;
    rx
}

async fn recv_or_lost(rx: oneshot::Receiver<Bytes>) -> Bytes {
    rx.await
        .unwrap_or_else(|_| Bytes::from_static(ERR_BACKEND_LOST))
}

// echoed names are CR/LF-stripped and capped so they cannot forge a second frame
fn display_name(raw: &[u8]) -> String {
    const CAP: usize = 128;
    let mut out = String::with_capacity(raw.len().min(CAP));
    for &b in raw.iter().take(CAP) {
        out.push(if b == b'\r' || b == b'\n' {
            ' '
        } else {
            b as char
        });
    }
    out
}

fn error_frame(msg: &str) -> Bytes {
    let mut out = Vec::new();
    resp::write_error(&mut out, msg);
    Bytes::from(out)
}

async fn blocking_round(
    shared: &Rc<Shared>,
    slot: u16,
    frame: Bytes,
    redirect: Option<(bool, String)>,
    retried: bool,
) -> Bytes {
    let topo = shared.topo.load_full();
    let (addr, asking) = match &redirect {
        Some((ask, target)) => (Some(target.as_str()), *ask),
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

fn parse_redirect(frame: &[u8]) -> Option<(bool, String)> {
    let ask = if frame.starts_with(b"-MOVED ") {
        false
    } else if frame.starts_with(b"-ASK ") {
        true
    } else {
        return None;
    };
    let text = std::str::from_utf8(frame).ok()?;
    let addr = text.trim_end().rsplit(' ').next()?;
    if !addr.contains(':') {
        return None;
    }
    Some((ask, addr.to_string()))
}

async fn pubsub_relay(
    shared: Rc<Shared>,
    addr: String,
    mut rx: mpsc::Receiver<Bytes>,
    reply_tx: Rc<ReplyTx>,
    link: Rc<WriterLink>,
) {
    let stream = match crate::backend::dial_raw(&addr, &shared.cfg).await {
        Ok(s) => s,
        Err(e) => {
            log_debug!("pubsub dial {addr}: {e}");
            backfill_acks(&link, &reply_tx);
            return;
        }
    };
    let (mut read_half, mut write_half) = stream.into_split();
    // an aborted relay must not detach a child blocked in write_all
    struct AbortOnDrop(tokio::task::JoinHandle<()>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _writer = AbortOnDrop(tokio::task::spawn_local(async move {
        use tokio::io::AsyncWriteExt;
        while let Some(frame) = rx.recv().await {
            if write_half.write_all(&frame).await.is_err() {
                return;
            }
        }
    }));
    let mut buf = BytesMut::with_capacity(crate::backend::READ_INIT);
    let mut last_ack: Option<u64> = None;
    'io: loop {
        loop {
            match resp::scan_value(&buf) {
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
                    if reply_tx.send(reply).is_err() {
                        break 'io;
                    }
                }
                resp::Scan::Invalid(_) => break 'io,
                resp::Scan::Incomplete => break,
            }
        }
        ensure_read_room(&mut buf);
        match read_half.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
    backfill_acks(&link, &reply_tx);
    // an idle subscriber sends nothing: the parked reader needs a wakeup
    mark_closed(&link);
    let _ = reply_tx.send(Reply::Close);
}

fn mark_closed(link: &WriterLink) {
    link.closed.set(true);
    link.closed_notify.notify_waiters();
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
fn backfill_acks(link: &Rc<WriterLink>, reply_tx: &Rc<ReplyTx>) {
    let drained: Vec<u64> = link.ack_seqs.borrow_mut().drain(..).collect();
    for seq in drained {
        let _ = reply_tx.send(Reply::Ack(seq, Bytes::from_static(ERR_BACKEND_LOST)));
    }
    link.acks_drained.notify_one();
}

// out-of-order replies indexed by sequence distance; O(1) park and drain
// (the back slot is always Some, so emptiness needs no live counter)
#[derive(Default)]
struct ParkedRing {
    base: u64,
    slots: VecDeque<Option<Bytes>>,
}

impl ParkedRing {
    fn put(&mut self, seq: u64, frame: Bytes) {
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
        self.slots[idx] = Some(frame);
    }

    fn take(&mut self, seq: u64) -> Option<Bytes> {
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

async fn write_loop(
    shared: Rc<Shared>,
    mut write_half: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Reply>,
    reply_tx: Rc<ReplyTx>,
    mut close_rx: oneshot::Receiver<u64>,
    link: Rc<WriterLink>,
    client_id: u64,
) {
    struct ExitBump<'a> {
        shared: &'a Shared,
        link: &'a WriterLink,
    }
    impl Drop for ExitBump<'_> {
        fn drop(&mut self) {
            mark_closed(self.link);
            stats::bump(&self.shared.stats.workers[self.shared.worker].writers_exited);
            self.link.oob_notify.notify_waiters();
        }
    }
    let _exit = ExitBump {
        shared: &shared,
        link: &link,
    };
    let mut next_emit: u64 = 0;
    let mut swept_to: u64 = 0;
    // protocol flips apply at the HELLO reply's sequence, not before
    let mut cur_proto: u8 = 2;
    // reader's final sequence; draining to it lets a departed client close
    let mut close_at: Option<u64> = None;
    let mut close_now = false;
    let mut parked = ParkedRing::default();
    let mut parked_acks = ParkedRing::default();
    let mut held_pushes: VecDeque<(u64, Bytes)> = VecDeque::new();
    let mut batch: Vec<Reply> = Vec::with_capacity(crate::backend::BATCH);
    let mut ready: Vec<Bytes> = Vec::with_capacity(crate::backend::BATCH);
    loop {
        if let Some(n) = close_at
            && next_emit >= n
            && parked.is_empty()
            && parked_acks.is_empty()
            && held_pushes.is_empty()
        {
            return;
        }
        tokio::select! {
            n = rx.recv_many(&mut batch, crate::backend::BATCH) => {
                if n == 0 {
                    return;
                }
            }
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
        // pass 1 processes the woken batch; one yield then coalesces every
        // ready backend's deliveries into the same flush
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
                            parked_acks.put(seq, frame);
                        }
                        continue;
                    }
                    Reply::At(seq, frame) => (seq, frame),
                };
                if seq < next_emit {
                    continue;
                }
                if frame.first() == Some(&b'-')
                    && (frame.starts_with(b"-MOVED ") || frame.starts_with(b"-ASK "))
                {
                    if let Some((ask, target)) = parse_redirect(&frame)
                        && let Some((req, base_expect)) = take_retry_frame(&link.inflight, seq, ask)
                    {
                        stats::bump(&shared.stats.workers[shared.worker].redirects);
                        let _ = shared.refresh.send(());
                        let conn = shared.backends.shared(&target, client_id, false);
                        conn.send(Outbound {
                            head: ask.then(|| Bytes::from_static(ASKING_FRAME)),
                            frame: req,
                            expect: base_expect + u32::from(ask),
                            sink: Sink::Client(reply_tx.clone(), seq),
                        })
                        .await;
                        continue;
                    }
                    // clients believe the proxy owns every slot: never leak redirects
                    frame = Bytes::from_static(ERR_TRYAGAIN);
                }
                if seq == next_emit {
                    link.proto_switches.apply(next_emit, &mut cur_proto);
                    ready.push(convert_nil(frame, cur_proto));
                    next_emit += 1;
                } else {
                    parked.put(seq, frame);
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
                if let Some(frame) = parked.take(next_emit) {
                    link.proto_switches.apply(next_emit, &mut cur_proto);
                    ready.push(convert_nil(frame, cur_proto));
                    next_emit += 1;
                } else if let Some(frame) = parked_acks.take(next_emit) {
                    link.proto_switches.apply(next_emit, &mut cur_proto);
                    push_pubsub_frame(&mut ready, frame, cur_proto);
                    next_emit += 1;
                } else {
                    break;
                }
            }
            if pass == 0 {
                if ready.len() < 2 || close_now {
                    break;
                }
                tokio::task::yield_now().await;
                while batch.len() < crate::backend::BATCH {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }
                if batch.is_empty() {
                    break;
                }
            }
        }
        if next_emit > swept_to {
            let mut inf = link.inflight.borrow_mut();
            while inf.front().is_some_and(|e| e.seq < next_emit) {
                inf.pop_front();
            }
            swept_to = next_emit;
        }
        if !ready.is_empty() {
            let mut total = 0usize;
            let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(ready.len());
            for f in &ready {
                total += f.len();
                slices.push(IoSlice::new(f));
            }
            if crate::backend::write_slices(&mut write_half, &mut slices)
                .await
                .is_err()
            {
                return;
            }
            drop(slices);
            stats::add(&shared.stats.workers[shared.worker].bytes_out, total as u64);
            ready.clear();
        }
        link.emitted.set(next_emit);
        if close_now {
            return;
        }
    }
}

// retryable redirects: single-reply requests always, multi-reply blobs only for MOVED
fn take_retry_frame(inflight: &InflightRing, seq: u64, ask: bool) -> Option<(Bytes, u32)> {
    let mut inf = inflight.borrow_mut();
    let idx = inf.binary_search_by_key(&seq, |e| e.seq).ok()?;
    let entry = &mut inf[idx];
    if entry.retried || (entry.expect > 1 && ask) {
        return None;
    }
    entry.retried = true;
    Some((entry.frame.clone(), entry.expect))
}

// true for publications; every other pubsub frame consumes a promised sequence
fn is_publication(frame: &[u8]) -> bool {
    if frame.first() != Some(&b'*') {
        return false;
    }
    let Some((n, _)) = resp::scan_int_line(frame, 1) else {
        return false;
    };
    if n < 3 {
        return false;
    }
    let Some(kind) = resp::Args::new(frame, 1).next() else {
        return false;
    };
    kind.eq_ignore_ascii_case(b"message") || kind.eq_ignore_ascii_case(b"pmessage")
}

fn pubsub_allowed(spec: &Spec) -> bool {
    spec.kind == Kind::Subscribe || matches!(spec.name, "ping" | "quit" | "reset")
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

    fn spec(name: &str) -> &'static Spec {
        command::lookup(name.as_bytes()).unwrap()
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
    fn parked_ring_orders_sparse_sequences() {
        let f = |n: u64| Bytes::from(n.to_string());
        let mut ring = ParkedRing::default();
        assert!(ring.is_empty());
        ring.put(5, f(5));
        ring.put(7, f(7));
        ring.put(4, f(4));
        assert!(!ring.is_empty());
        assert_eq!(ring.take(3), None);
        assert_eq!(ring.take(4), Some(f(4)));
        assert_eq!(ring.take(5), Some(f(5)));
        assert_eq!(ring.take(6), None);
        assert_eq!(ring.take(7), Some(f(7)));
        assert!(ring.is_empty());
        ring.put(10, f(10));
        assert_eq!(ring.take(10), Some(f(10)));
        assert!(ring.is_empty());
        assert_eq!(ring.take(11), None);
    }

    #[test]
    fn parses_redirects() {
        assert_eq!(
            parse_redirect(b"-MOVED 3999 10.0.0.2:7002\r\n"),
            Some((false, "10.0.0.2:7002".to_string()))
        );
        assert_eq!(
            parse_redirect(b"-ASK 42 10.0.0.3:7003\r\n"),
            Some((true, "10.0.0.3:7003".to_string()))
        );
        assert_eq!(parse_redirect(b"-ERR nope\r\n"), None);
        assert_eq!(parse_redirect(b"-MOVED garbage\r\n"), None);
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

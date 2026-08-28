//! Client session: request parsing, dispatch, ordered reply emission, MULTI,
//! blocking commands, pubsub relay, and MOVED/ASK retries.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::io::IoSlice;
use std::rc::Rc;

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, oneshot};

use crate::backend::{ASKING_FRAME, Backends, ERR_BACKEND_LOST, Outbound, Sink};
use crate::command::{self, Kind, Spec};
use crate::config::SlaveMode;
use crate::log_debug;
use crate::multikey;
use crate::resp::{self, ReqScan};
use crate::stats::{self, Stats};
use crate::topology::Topology;
use crate::{admin, crc16, route};

pub const MAX_INFLIGHT: usize = 65536;
pub const PUBSUB_FORWARD_QUEUE: usize = 64;
pub const PUBSUB_PUSH_WINDOW: usize = 4096;
const GATE_PROBE: std::time::Duration = std::time::Duration::from_millis(100);
const UNSUB_SYNC: std::time::Duration = std::time::Duration::from_millis(100);
pub const READ_CHUNK: usize = crate::backend::READ_CHUNK;
/// Sequence for out-of-band frames (pubsub pushes) that bypass ordering.
pub const SEQ_OOB: u64 = u64::MAX;

const ERR_NOAUTH: &str = "NOAUTH Authentication required.";
const ERR_CROSSSLOT: &str = "CROSSSLOT Keys in request don't hash to the same slot";
const ERR_NO_OWNER: &str = "CLUSTERDOWN Hash slot not served";
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

struct InFlight {
    frame: Bytes,
    expect: u32,
    retried: bool,
}

type InflightMap = Rc<RefCell<HashMap<u64, InFlight>>>;
type ReplyTx = mpsc::UnboundedSender<(u64, Bytes)>;
type Emitted = Rc<Cell<u64>>;
type ProtoSwitches = Rc<ProtoSwitchQueue>;

/// Pending protocol flips; `armed` keeps the hot path off the RefCell.
#[derive(Default)]
pub struct ProtoSwitchQueue {
    armed: Cell<usize>,
    queue: RefCell<std::collections::VecDeque<(u64, u8)>>,
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
    reply_tx: ReplyTx,
    inflight: InflightMap,
    proto: Rc<Cell<u8>>,
    authed: Cell<bool>,
    name: RefCell<String>,
    next_seq: Cell<u64>,
    rng: Cell<u64>,
    multi: RefCell<Option<MultiState>>,
    pubsub: RefCell<Option<PubsubHandle>>,
    pubsub_done: Rc<Cell<bool>>,
    unsub_inflight: Cell<bool>,
    oob_pending: Rc<Cell<usize>>,
    blocking: RefCell<Vec<(u64, tokio::task::JoinHandle<()>)>>,
    closing: Cell<bool>,
    proto_switches: ProtoSwitches,
    conns: RefCell<ConnCache>,
}

impl Session {
    async fn dispatch(&self, frame: Bytes, argc: usize) {
        stats::bump(&self.shared.stats.workers[self.shared.worker].commands);
        if argc == 0 {
            return;
        }
        if self.pubsub.borrow().is_some() && !self.exit_pubsub_if_done().await {
            let passthrough = self.proto.get() >= 3 && !is_pubsub_command(&frame, argc);
            if !passthrough {
                self.dispatch_pubsub(frame, argc);
                return;
            }
        }
        let spec = {
            let mut it = resp::Args::new(&frame, argc);
            let Some(name) = it.next() else {
                return;
            };
            match command::lookup(name) {
                Some(spec) => spec,
                None => {
                    let name = String::from_utf8_lossy(name).into_owned();
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
            self.emit_error(ERR_NOAUTH);
            return;
        }
        if self.multi.borrow().is_some()
            && !matches!(spec.name, "exec" | "discard" | "multi" | "quit")
        {
            self.queue_multi(spec, frame, argc);
            return;
        }
        if spec.name == "exec" {
            self.handle_exec().await;
            return;
        }
        match spec.kind {
            Kind::Single => self.forward_single(spec, frame, argc).await,
            Kind::Local => self.handle_local(spec, frame, argc).await,
            Kind::AnyMaster => self.forward_any_master(frame).await,
            Kind::MultiSum | Kind::Mget | Kind::Mset => self.fan_out(spec, &frame, argc),
            Kind::Blocking => self.forward_blocking(spec, frame, argc),
            Kind::Eval => self.forward_eval(frame, argc).await,
            Kind::Xread => self.forward_xread(spec, frame, argc).await,
            Kind::Subscribe => self.enter_pubsub(frame).await,
            Kind::Scan => self.run_scan(&frame, argc),
            Kind::Dbsize => self.run_broadcast_frame(frame, Merge::Sum),
            Kind::Flushall => self.run_broadcast_frame(frame, Merge::Ok),
        }
    }

    fn key_slot(&self, frame: &Bytes, argc: usize, key_index: usize) -> Option<u16> {
        let mut it = resp::Args::new(frame, argc);
        it.nth(key_index).map(crc16::slot)
    }

    async fn forward_single(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let Some(slot) = self.key_slot(&frame, argc, spec.first_key as usize) else {
            self.emit_error("ERR missing key");
            return;
        };
        let seq = self.alloc_seq();
        let conn = {
            let topo = self.shared.topo.load();
            let picked = self.with_rng(|r| {
                route::pick(
                    &topo,
                    slot,
                    spec.is_readonly(),
                    self.shared.cfg.slave_mode,
                    r,
                )
            });
            let Some((idx, is_replica)) = picked else {
                self.emit_at(seq, error_frame(ERR_NO_OWNER));
                return;
            };
            self.cached_conn(&topo, idx, is_replica)
        };
        self.track_inflight_entry(seq, &frame);
        self.send_single(conn, seq, frame).await;
    }

    // one Vec index per request once warm; re-resolves on epoch or death.
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
            let topo = self.shared.topo.load();
            let picked = self.with_rng(|r| route::any_master(&topo, r));
            let Some(idx) = picked else {
                self.emit_at(seq, error_frame(ERR_NO_OWNER));
                return;
            };
            self.cached_conn(&topo, idx, false)
        };
        self.send_single(conn, seq, frame).await;
    }

    fn any_master_addr(&self) -> Option<String> {
        let topo = self.shared.topo.load();
        let idx = self.with_rng(|r| route::any_master(&topo, r))?;
        Some(topo.nodes[idx as usize].addr.clone())
    }

    fn with_rng<R>(&self, f: impl FnOnce(&mut u64) -> R) -> R {
        let mut rng = self.rng.get();
        let out = f(&mut rng);
        self.rng.set(rng);
        out
    }

    async fn send_single(&self, conn: Rc<crate::backend::Conn>, seq: u64, frame: Bytes) {
        conn.send(Outbound {
            head: None,
            frame,
            expect: 1,
            seq,
            sink: Sink::Client(self.reply_tx.clone()),
        })
        .await;
    }

    fn track_inflight_entry(&self, seq: u64, frame: &Bytes) {
        self.track_inflight_expect(seq, frame, 1);
    }

    fn track_inflight_expect(&self, seq: u64, frame: &Bytes, expect: u32) {
        self.inflight.borrow_mut().insert(
            seq,
            InFlight {
                frame: frame.clone(),
                expect,
                retried: false,
            },
        );
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
        let conn = {
            let topo = self.shared.topo.load();
            let Some(idx) = slot.and_then(|sl| topo.owner(sl)) else {
                self.emit_at(seq, error_frame(ERR_NO_OWNER));
                return;
            };
            self.cached_conn(&topo, idx, false)
        };
        self.track_inflight_entry(seq, &frame);
        self.send_single(conn, seq, frame).await;
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
        let seq = self.alloc_seq();
        let conn = {
            let topo = self.shared.topo.load();
            let picked = self.with_rng(|r| {
                route::pick(
                    &topo,
                    slot,
                    spec.is_readonly(),
                    self.shared.cfg.slave_mode,
                    r,
                )
            });
            let Some((idx, is_replica)) = picked else {
                self.emit_at(seq, error_frame(ERR_NO_OWNER));
                return;
            };
            self.cached_conn(&topo, idx, is_replica)
        };
        self.track_inflight_entry(seq, &frame);
        self.send_single(conn, seq, frame).await;
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
            let _ = reply_tx.send((seq, reply));
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
            let keys: Vec<&[u8]> = key_indices(spec, argc).map(|i| args[i]).collect();
            let values: Option<Vec<&[u8]>> =
                (spec.step == 2).then(|| key_indices(spec, argc).map(|i| args[i + 1]).collect());
            let total = keys.len();
            let topo = self.shared.topo.load_full();
            let parts = self.with_rng(|rng| {
                multikey::split(spec.name.as_bytes(), &keys, values.as_deref(), |slot| {
                    route::pick(&topo, slot, readonly, mode, rng).map(|(i, _)| i)
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
        let kind = spec.kind;
        let flag_readonly = readonly && mode != SlaveMode::Off;
        tokio::task::spawn_local(async move {
            let mut receivers = Vec::with_capacity(parts.len());
            for part in &parts {
                let (tx, rx) = oneshot::channel();
                let conn =
                    shared
                        .backends
                        .shared(&topo.nodes[part.node as usize].addr, id, flag_readonly);
                conn.send(Outbound {
                    head: None,
                    frame: part.frame.clone(),
                    expect: 1,
                    seq: 0,
                    sink: Sink::One(tx),
                })
                .await;
                receivers.push(rx);
            }
            let mut results: Vec<(Vec<usize>, Bytes)> = Vec::with_capacity(parts.len());
            let mut redirected: Vec<(multikey::Part, bool, String)> = Vec::new();
            for (part, rx) in parts.into_iter().zip(receivers) {
                let reply = recv_or_lost(rx).await;
                // a redirected part executed nothing: one resend is idempotent.
                match parse_redirect(&reply) {
                    Some((ask, target)) => redirected.push((part, ask, target)),
                    None => results.push((part.positions, reply)),
                }
            }
            if !redirected.is_empty() {
                let _ = shared.refresh.send(());
                let mut retries = Vec::with_capacity(redirected.len());
                for (part, ask, target) in &redirected {
                    let (tx, rx) = oneshot::channel();
                    let conn = shared.backends.shared(target, id, false);
                    conn.send(Outbound {
                        head: ask.then(|| Bytes::from_static(ASKING_FRAME)),
                        frame: part.frame.clone(),
                        expect: if *ask { 2 } else { 1 },
                        seq: 0,
                        sink: Sink::One(tx),
                    })
                    .await;
                    retries.push(rx);
                }
                for ((part, _, _), rx) in redirected.into_iter().zip(retries) {
                    let reply = recv_or_lost(rx).await;
                    results.push((part.positions, reply));
                }
            }
            let merged = match kind {
                Kind::Mget => multikey::merge_mget(total, &results),
                Kind::Mset => multikey::merge_ok(&results),
                Kind::MultiSum => multikey::merge_sum(&results),
                _ => unreachable!("fan_out only handles multi-key kinds"),
            };
            let out = match merged {
                Ok(bytes) => Bytes::from(bytes),
                Err(err_frame) => err_frame,
            };
            let _ = reply_tx.send((seq, out));
        });
    }

    fn run_scan(&self, frame: &Bytes, argc: usize) {
        let cursor = {
            let args = collect_args(frame, argc);
            std::str::from_utf8(args[1])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
        };
        let Some(cursor) = cursor else {
            self.emit_error("ERR invalid cursor");
            return;
        };
        let frame = frame.clone();
        let (master_idx, node_cursor) = multikey::unpack_cursor(cursor);
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let id = self.id;
        tokio::task::spawn_local(async move {
            let topo = shared.topo.load_full();
            if master_idx >= topo.masters.len() {
                let done = multikey::rebuild_scan_reply(0, b"*0\r\n");
                let _ = reply_tx.send((seq, Bytes::from(done)));
                return;
            }
            let addr = &topo.nodes[topo.masters[master_idx] as usize].addr;
            let cursor_str = node_cursor.to_string();
            let mut sub_args: Vec<&[u8]> = vec![b"SCAN", cursor_str.as_bytes()];
            let args = collect_args(&frame, argc);
            sub_args.extend_from_slice(&args[2..]);
            let mut cmd = Vec::new();
            resp::write_command(&mut cmd, &sub_args);
            let (tx, rx) = oneshot::channel();
            let conn = shared.backends.shared(addr, id, false);
            conn.send(Outbound {
                head: None,
                frame: Bytes::from(cmd),
                expect: 1,
                seq: 0,
                sink: Sink::One(tx),
            })
            .await;
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
            let _ = reply_tx.send((seq, out));
        });
    }

    fn run_broadcast_frame(&self, frame: Bytes, merge: Merge) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let id = self.id;
        tokio::task::spawn_local(async move {
            let topo = shared.topo.load_full();
            let mut receivers = Vec::with_capacity(topo.masters.len());
            for &midx in &topo.masters {
                let (tx, rx) = oneshot::channel();
                let conn = shared
                    .backends
                    .shared(&topo.nodes[midx as usize].addr, id, false);
                conn.send(Outbound {
                    head: None,
                    frame: frame.clone(),
                    expect: 1,
                    seq: 0,
                    sink: Sink::One(tx),
                })
                .await;
                receivers.push(rx);
            }
            let mut replies: Vec<(Vec<usize>, Bytes)> = Vec::with_capacity(receivers.len());
            for rx in receivers {
                replies.push((Vec::new(), recv_or_lost(rx).await));
            }
            let merged = match merge {
                Merge::Sum => multikey::merge_sum(&replies),
                Merge::Ok => multikey::merge_ok(&replies),
            };
            let out = match merged {
                Ok(b) => Bytes::from(b),
                Err(e) => e,
            };
            let _ = reply_tx.send((seq, out));
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
                self.emit_error(ERR_CROSSSLOT);
            }
            Some(slot) => {
                state.slot = Some(slot);
                state.frames.push(frame);
                drop(guard);
                self.emit_local(b"+QUEUED\r\n".to_vec());
            }
        }
    }

    async fn handle_local(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let reply = {
            let args = collect_args(&frame, argc);
            match spec.name {
                "ping" => Some(admin::ping(&args)),
                "echo" => Some(admin::echo(&args)),
                "select" => Some(admin::select(&args)),
                "time" => Some(admin::time()),
                "info" => Some(admin::info(
                    &self.shared.cfg,
                    &self.shared.stats,
                    self.shared.started,
                )),
                "config" => Some(admin::config_cmd(&args, &self.shared.cfg)),
                "cluster" => Some(admin::cluster(&args, &self.shared.cfg, self.proto.get())),
                "command" => Some(admin::command_reply(
                    &args,
                    command::table(),
                    self.proto.get(),
                )),
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
                        Some(out)
                    }
                    _ => Some(error_frame_vec("ERR unsupported ACL subcommand")),
                },
                "client" => {
                    self.handle_client_cmd(&args);
                    None
                }
                "quit" => {
                    self.closing.set(true);
                    Some(admin::OK.to_vec())
                }
                "reset" => {
                    *self.multi.borrow_mut() = None;
                    self.proto.set(2);
                    self.proto_switches.push(self.next_seq.get(), 2);
                    self.authed.set(self.shared.cfg.requirepass.is_empty());
                    Some(b"+RESET\r\n".to_vec())
                }
                "multi" => {
                    if self.multi.borrow().is_some() {
                        Some(error_frame_vec("ERR MULTI calls can not be nested"))
                    } else {
                        *self.multi.borrow_mut() = Some(MultiState {
                            slot: None,
                            frames: Vec::new(),
                            aborted: false,
                        });
                        Some(admin::OK.to_vec())
                    }
                }
                "discard" => {
                    if self.multi.borrow_mut().take().is_some() {
                        Some(admin::OK.to_vec())
                    } else {
                        Some(error_frame_vec("ERR DISCARD without MULTI"))
                    }
                }
                _ => Some(error_frame_vec("ERR unsupported command")),
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
            self.emit_local(b"*0\r\n".to_vec());
            return;
        }
        let seq = self.alloc_seq();
        let conn = {
            let topo = self.shared.topo.load();
            let Some(idx) = state.slot.and_then(|sl| topo.owner(sl)) else {
                self.emit_at(seq, error_frame(ERR_NO_OWNER));
                return;
            };
            self.cached_conn(&topo, idx, false)
        };
        let mut blob = Vec::new();
        blob.extend_from_slice(b"*1\r\n$5\r\nMULTI\r\n");
        for f in &state.frames {
            blob.extend_from_slice(f);
        }
        blob.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
        let expect = state.frames.len() as u32 + 2;
        let blob = Bytes::from(blob);
        self.track_inflight_expect(seq, &blob, expect);
        conn.send(Outbound {
            head: None,
            frame: blob,
            expect,
            seq,
            sink: Sink::Client(self.reply_tx.clone()),
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
            self.emit_local(admin::OK.to_vec());
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
        self.proto_switches.push(self.next_seq.get(), proto);
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
                self.emit_local(admin::OK.to_vec());
            }
            Some(b"getname") => {
                let mut out = Vec::new();
                admin::bulk(&mut out, self.name.borrow().as_bytes());
                self.emit_local(out);
            }
            _ => self.emit_error("ERR unsupported CLIENT subcommand"),
        }
    }

    // waits briefly for an in-flight unsubscribe confirmation before deciding.
    async fn exit_pubsub_if_done(&self) -> bool {
        if !self.pubsub_done.get() && self.unsub_inflight.get() {
            let deadline = tokio::time::Instant::now() + UNSUB_SYNC;
            while !self.pubsub_done.get() && tokio::time::Instant::now() < deadline {
                tokio::task::yield_now().await;
            }
            self.unsub_inflight.set(false);
        }
        if self.pubsub_done.get() {
            if let Some(ps) = self.pubsub.borrow_mut().take() {
                ps.task.abort();
            }
            self.pubsub_done.set(false);
            return true;
        }
        false
    }

    fn dispatch_pubsub(&self, frame: Bytes, argc: usize) {
        let forward = {
            let args = collect_args(&frame, argc);
            let lower = args
                .first()
                .map(|n| n.to_ascii_lowercase())
                .unwrap_or_default();
            match lower.as_slice() {
                b"unsubscribe" | b"punsubscribe" => {
                    self.unsub_inflight.set(true);
                    true
                }
                b"subscribe" | b"psubscribe" | b"ping" => true,
                b"quit" => {
                    self.closing.set(true);
                    false
                }
                b"reset" => {
                    if let Some(ps) = self.pubsub.borrow_mut().take() {
                        ps.task.abort();
                    }
                    self.proto.set(2);
                    self.proto_switches.push(self.next_seq.get(), 2);
                    self.emit_local(b"+RESET\r\n".to_vec());
                    return;
                }
                _ => {
                    self.emit_error(
                        "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET \
                         are allowed in this context",
                    );
                    return;
                }
            }
        };
        if forward {
            let sent = self
                .pubsub
                .borrow()
                .as_ref()
                .is_some_and(|ps| ps.tx.try_send(frame).is_ok());
            if !sent {
                if let Some(ps) = self.pubsub.borrow_mut().take() {
                    ps.task.abort();
                }
                self.emit_error("ERR pubsub backend connection lost");
            }
        } else {
            self.emit_local(admin::OK.to_vec());
        }
    }

    async fn enter_pubsub(&self, first_frame: Bytes) {
        let Some(addr) = self.any_master_addr() else {
            self.emit_error(ERR_NO_OWNER);
            return;
        };
        let first_seq = self.alloc_seq();
        let (tx, rx) = mpsc::channel::<Bytes>(PUBSUB_FORWARD_QUEUE);
        let _ = tx.try_send(first_frame);
        self.pubsub_done.set(false);
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let proto = self.proto.clone();
        let done = self.pubsub_done.clone();
        let budget = self.oob_pending.clone();
        let task = tokio::task::spawn_local(async move {
            pubsub_relay(shared, addr, rx, reply_tx, proto, done, budget, first_seq).await;
        });
        *self.pubsub.borrow_mut() = Some(PubsubHandle { tx, task });
    }

    fn abort_multi(&self) {
        if let Some(state) = self.multi.borrow_mut().as_mut() {
            state.aborted = true;
        }
    }

    fn alloc_seq(&self) -> u64 {
        let seq = self.next_seq.get();
        self.next_seq.set(seq + 1);
        seq
    }

    fn emit_local(&self, bytes: Vec<u8>) {
        let seq = self.alloc_seq();
        self.emit_at(seq, Bytes::from(bytes));
    }

    fn emit_error(&self, msg: &str) {
        stats::bump(&self.shared.stats.workers[self.shared.worker].errors);
        self.emit_local(error_frame_vec(msg));
    }

    fn emit_at(&self, seq: u64, frame: Bytes) {
        let _ = self.reply_tx.send((seq, frame));
    }
}

/// Serves one client connection to completion.
pub async fn serve(shared: Rc<Shared>, stream: TcpStream, id: u64) {
    if stream.set_nodelay(true).is_err() {
        return;
    }
    let (mut read_half, write_half) = stream.into_split();
    let (reply_tx, reply_rx) = mpsc::unbounded_channel();
    let inflight: InflightMap = Rc::new(RefCell::new(HashMap::new()));
    let proto = Rc::new(Cell::new(2u8));
    let emitted: Emitted = Rc::new(Cell::new(0));
    let proto_switches: ProtoSwitches = Rc::new(ProtoSwitchQueue::default());
    let oob_budget: Rc<Cell<usize>> = Rc::new(Cell::new(0));

    let session = Session {
        shared: shared.clone(),
        id,
        reply_tx: reply_tx.clone(),
        inflight: inflight.clone(),
        proto: proto.clone(),
        authed: Cell::new(shared.cfg.requirepass.is_empty()),
        name: RefCell::new(String::new()),
        next_seq: Cell::new(0),
        rng: Cell::new(id | 1),
        multi: RefCell::new(None),
        pubsub: RefCell::new(None),
        pubsub_done: Rc::new(Cell::new(false)),
        unsub_inflight: Cell::new(false),
        oob_pending: oob_budget.clone(),
        blocking: RefCell::new(Vec::new()),
        closing: Cell::new(false),
        proto_switches: proto_switches.clone(),
        conns: RefCell::new(ConnCache {
            epoch: 0,
            by_node: Vec::new(),
        }),
    };

    let (close_tx, close_rx) = oneshot::channel::<u64>();
    let writer = tokio::task::spawn_local(write_loop(
        shared.clone(),
        write_half,
        reply_rx,
        reply_tx.clone(),
        close_rx,
        inflight,
        emitted.clone(),
        proto_switches.clone(),
        oob_budget,
        proto,
        id,
    ));

    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    'main: loop {
        loop {
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
                        drop(refs);
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
        if buf.len() > shared.cfg.query_buffer_limit {
            session.emit_error("ERR query buffer exceeds limit");
            break;
        }
        while session.next_seq.get().saturating_sub(emitted.get()) > MAX_INFLIGHT as u64 {
            if reply_tx.is_closed() {
                break 'main;
            }
            let mut probe = [0u8; 1];
            match tokio::time::timeout(GATE_PROBE, read_half.peek(&mut probe)).await {
                Ok(Ok(0)) | Ok(Err(_)) => break 'main,
                _ => {}
            }
        }
        match read_half.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => stats::add(&shared.stats.workers[shared.worker].bytes_in, n as u64),
        }
    }
    stats::bump(&shared.stats.workers[shared.worker].readers_exited);
    if let Some(ps) = session.pubsub.borrow_mut().take() {
        ps.task.abort();
    }
    for (seq, task) in session.blocking.borrow_mut().drain(..) {
        if task.is_finished() {
            continue;
        }
        task.abort();
        let _ = reply_tx.send((seq, Bytes::from_static(ERR_BACKEND_LOST)));
    }
    let final_seq = session.next_seq.get();
    drop(session);
    drop(reply_tx);
    let _ = close_tx.send(final_seq);
    let _ = writer.await;
    stats::bump(&shared.stats.workers[shared.worker].sessions_closed);
}
/// Per-session resolved connections, valid for one topology epoch.
struct ConnCache {
    epoch: u64,
    by_node: Vec<Option<Rc<crate::backend::Conn>>>,
}

struct MultiState {
    slot: Option<u16>,
    frames: Vec<Bytes>,
    aborted: bool,
}

struct PubsubHandle {
    tx: mpsc::Sender<Bytes>,
    task: tokio::task::JoinHandle<()>,
}

enum Merge {
    Sum,
    Ok,
}

fn is_pubsub_command(frame: &Bytes, argc: usize) -> bool {
    let mut it = resp::Args::new(frame, argc);
    it.next().and_then(command::lookup).is_some_and(|spec| {
        spec.kind == Kind::Subscribe || matches!(spec.name, "ping" | "quit" | "reset")
    })
}

fn collect_args(frame: &Bytes, argc: usize) -> Vec<&[u8]> {
    resp::Args::new(frame, argc).collect()
}

/// Argument indices holding keys, per the spec's first/last/step triple.
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

async fn recv_or_lost(rx: oneshot::Receiver<Bytes>) -> Bytes {
    rx.await
        .unwrap_or_else(|_| Bytes::from_static(ERR_BACKEND_LOST))
}

fn error_frame(msg: &str) -> Bytes {
    Bytes::from(error_frame_vec(msg))
}

fn error_frame_vec(msg: &str) -> Vec<u8> {
    let mut out = Vec::new();
    resp::write_error(&mut out, msg);
    out
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
        return error_frame(ERR_NO_OWNER);
    };
    let Some(lease) = shared.backends.take_exclusive(addr, false) else {
        return error_frame("ERR too many blocking connections");
    };
    let (tx, rx) = oneshot::channel();
    lease
        .conn()
        .send(Outbound {
            head: asking.then(|| Bytes::from_static(ASKING_FRAME)),
            frame: frame.clone(),
            expect: if asking { 2 } else { 1 },
            seq: 0,
            sink: Sink::One(tx),
        })
        .await;
    let reply = recv_or_lost(rx).await;
    lease.complete();
    drop(lease);
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

#[allow(clippy::too_many_arguments)]
async fn pubsub_relay(
    shared: Rc<Shared>,
    addr: String,
    mut rx: mpsc::Receiver<Bytes>,
    reply_tx: ReplyTx,
    proto: Rc<Cell<u8>>,
    done: Rc<Cell<bool>>,
    budget: Rc<Cell<usize>>,
    first_seq: u64,
) {
    let mut first_seq = Some(first_seq);
    let stream = match crate::backend::dial_raw(&addr, &shared.cfg).await {
        Ok(s) => s,
        Err(e) => {
            log_debug!("pubsub dial {addr}: {e}");
            if let Some(seq) = first_seq.take() {
                let _ = reply_tx.send((seq, error_frame("ERR pubsub backend unavailable")));
            }
            return;
        }
    };
    let (mut read_half, mut write_half) = stream.into_split();
    let writer = tokio::task::spawn_local(async move {
        use tokio::io::AsyncWriteExt;
        while let Some(frame) = rx.recv().await {
            if write_half.write_all(&frame).await.is_err() {
                return;
            }
        }
    });
    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    loop {
        match resp::scan_value(&buf) {
            resp::Scan::Complete(len) => {
                let mut frame = buf.split_to(len);
                if subscription_count(&frame) == Some(0) {
                    done.set(true);
                }
                if proto.get() >= 3 && frame.first() == Some(&b'*') {
                    frame[0] = b'>';
                }
                while budget.get() >= PUBSUB_PUSH_WINDOW {
                    if reply_tx.is_closed() {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
                // the first confirmation rides normal ordering.
                let tagged = match first_seq.take() {
                    Some(seq) => (seq, frame.freeze()),
                    None => {
                        budget.set(budget.get() + 1);
                        (SEQ_OOB, frame.freeze())
                    }
                };
                if reply_tx.send(tagged).is_err() {
                    break;
                }
                continue;
            }
            resp::Scan::Invalid(_) => break,
            resp::Scan::Incomplete => {}
        }
        match read_half.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
    writer.abort();
}

#[allow(clippy::too_many_arguments)]
async fn write_loop(
    shared: Rc<Shared>,
    mut write_half: OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<(u64, Bytes)>,
    reply_tx: ReplyTx,
    mut close_rx: oneshot::Receiver<u64>,
    inflight: InflightMap,
    emitted: Emitted,
    proto_switches: ProtoSwitches,
    oob_budget: Rc<Cell<usize>>,
    proto: Rc<Cell<u8>>,
    client_id: u64,
) {
    let _ = proto;
    struct ExitBump(Rc<Shared>);
    impl Drop for ExitBump {
        fn drop(&mut self) {
            stats::bump(&self.0.stats.workers[self.0.worker].writers_exited);
        }
    }
    let _exit = ExitBump(shared.clone());
    let mut next_emit: u64 = 0;
    // protocol flips apply at the HELLO reply's sequence, not before.
    let mut cur_proto: u8 = 2;
    // reader's final sequence; draining to it lets a departed client close.
    let mut close_at: Option<u64> = None;
    let mut parked: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut batch: Vec<(u64, Bytes)> = Vec::with_capacity(crate::backend::BATCH);
    let mut ready: Vec<Bytes> = Vec::with_capacity(crate::backend::BATCH);
    loop {
        if let Some(n) = close_at
            && next_emit >= n
            && parked.is_empty()
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
        for (seq, frame) in batch.drain(..) {
            if seq == SEQ_OOB {
                oob_budget.set(oob_budget.get().saturating_sub(1));
                ready.push(convert_nil(frame, cur_proto));
                continue;
            }
            if seq < next_emit {
                continue;
            }
            if frame.first() == Some(&b'-')
                && let Some((req, base_expect)) = take_retry_frame(&inflight, seq, &frame)
                && let Some((ask, target)) = parse_redirect(&frame)
                && (base_expect == 1 || !ask)
            {
                stats::bump(&shared.stats.workers[shared.worker].redirects);
                let _ = shared.refresh.send(());
                let conn = shared.backends.shared(&target, client_id, false);
                conn.send(Outbound {
                    head: ask.then(|| Bytes::from_static(ASKING_FRAME)),
                    frame: req,
                    expect: base_expect + u32::from(ask),
                    seq,
                    sink: Sink::Client(reply_tx.clone()),
                })
                .await;
                continue;
            }
            // clients believe the proxy owns every slot: never leak redirects.
            let frame = if frame.starts_with(b"-MOVED ") || frame.starts_with(b"-ASK ") {
                Bytes::from_static(ERR_TRYAGAIN)
            } else {
                frame
            };
            if seq == next_emit {
                inflight.borrow_mut().remove(&seq);
                proto_switches.apply(next_emit, &mut cur_proto);
                ready.push(convert_nil(frame, cur_proto));
                next_emit += 1;
            } else {
                parked.insert(seq, frame);
            }
        }
        while let Some(frame) = parked.remove(&next_emit) {
            inflight.borrow_mut().remove(&next_emit);
            proto_switches.apply(next_emit, &mut cur_proto);
            ready.push(convert_nil(frame, cur_proto));
            next_emit += 1;
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
        emitted.set(next_emit);
    }
}

fn take_retry_frame(inflight: &InflightMap, seq: u64, frame: &Bytes) -> Option<(Bytes, u32)> {
    if !frame.starts_with(b"-MOVED ") && !frame.starts_with(b"-ASK ") {
        return None;
    }
    let mut inf = inflight.borrow_mut();
    match inf.get_mut(&seq) {
        Some(entry) if !entry.retried => {
            entry.retried = true;
            Some((entry.frame.clone(), entry.expect))
        }
        _ => None,
    }
}

/// Returns the remaining-subscriptions count of a (p|un)subscribe
/// confirmation frame, or None for any other frame.
fn subscription_count(frame: &[u8]) -> Option<i64> {
    let items = multikey::split_array(frame)?;
    if items.len() != 3 {
        return None;
    }
    let kind = resp::bulk_payload(items[0])?;
    let known = [
        b"subscribe".as_ref(),
        b"unsubscribe".as_ref(),
        b"psubscribe".as_ref(),
        b"punsubscribe".as_ref(),
    ];
    if !known.iter().any(|k| kind.eq_ignore_ascii_case(k)) {
        return None;
    }
    multikey::parse_int(items[2])
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

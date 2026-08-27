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
pub const READ_CHUNK: usize = 64 * 1024;
/// Sequence for out-of-band frames (pubsub pushes) that bypass ordering.
pub const SEQ_OOB: u64 = u64::MAX;

const ERR_NOAUTH: &str = "NOAUTH Authentication required.";
const ERR_CROSSSLOT: &str = "CROSSSLOT Keys in request don't hash to the same slot";
const ERR_NO_OWNER: &str = "CLUSTERDOWN Hash slot not served";

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
    retried: bool,
}

type Inflight = Rc<RefCell<HashMap<u64, InFlight>>>;
type ReplyTx = mpsc::UnboundedSender<(u64, Bytes)>;

struct Session {
    shared: Rc<Shared>,
    id: u64,
    reply_tx: ReplyTx,
    inflight: Inflight,
    proto: Rc<Cell<u8>>,
    authed: Cell<bool>,
    name: RefCell<String>,
    next_seq: Cell<u64>,
    rng: Cell<u64>,
    multi: RefCell<Option<MultiState>>,
    pubsub: RefCell<Option<PubsubHandle>>,
    closing: Cell<bool>,
}

struct MultiState {
    slot: Option<u16>,
    frames: Vec<Bytes>,
    aborted: bool,
}

struct PubsubHandle {
    tx: mpsc::UnboundedSender<Bytes>,
    task: tokio::task::JoinHandle<()>,
}

/// Serves one client connection to completion.
pub async fn serve(shared: Rc<Shared>, stream: TcpStream, id: u64) {
    if stream.set_nodelay(true).is_err() {
        return;
    }
    let (mut read_half, write_half) = stream.into_split();
    let (reply_tx, reply_rx) = mpsc::unbounded_channel();
    let inflight: Inflight = Rc::new(RefCell::new(HashMap::new()));
    let proto = Rc::new(Cell::new(2u8));

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
        closing: Cell::new(false),
    };

    let writer = tokio::task::spawn_local(write_loop(
        shared.clone(),
        write_half,
        reply_rx,
        reply_tx,
        inflight,
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
                    let args = resp::split_inline(&line);
                    let argc = args.len();
                    if argc > 0 {
                        let mut rebuilt = Vec::new();
                        resp::write_command(&mut rebuilt, &args);
                        drop(args);
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
        while session.inflight.borrow().len() > MAX_INFLIGHT {
            tokio::task::yield_now().await;
        }
        match read_half.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => stats::add(&shared.stats.workers[shared.worker].bytes_in, n as u64),
        }
    }
    if let Some(ps) = session.pubsub.borrow_mut().take() {
        ps.task.abort();
    }
    drop(session);
    let _ = writer.await;
}

impl Session {
    async fn dispatch(&self, frame: Bytes, argc: usize) {
        stats::bump(&self.shared.stats.workers[self.shared.worker].commands);
        if argc == 0 {
            return;
        }
        if self.pubsub.borrow().is_some() {
            self.dispatch_pubsub(frame, argc);
            return;
        }
        let spec = {
            let mut it = resp::Args::new(&frame, argc);
            match it.next().map(command::lookup) {
                Some(Some(spec)) => spec,
                Some(None) => {
                    let name = {
                        let mut it = resp::Args::new(&frame, argc);
                        it.next().map(|n| String::from_utf8_lossy(n).into_owned())
                    };
                    self.emit_error(&format!(
                        "ERR unknown command '{}'",
                        name.unwrap_or_default()
                    ));
                    return;
                }
                None => return,
            }
        };
        if !spec.arity_ok(argc) {
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
            Kind::Dbsize => self.run_broadcast(b"DBSIZE", Merge::Sum),
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
        let Some((addr, is_replica)) = self.pick_addr(slot, spec.is_readonly()) else {
            self.emit_at(seq, error_frame(ERR_NO_OWNER));
            return;
        };
        self.inflight.borrow_mut().insert(
            seq,
            InFlight {
                frame: frame.clone(),
                retried: false,
            },
        );
        let conn = self.shared.backends.shared(&addr, self.id, is_replica);
        conn.send(Outbound {
            head: None,
            frame,
            expect: 1,
            seq,
            sink: Sink::Client(self.reply_tx.clone()),
        })
        .await;
    }

    fn pick_addr(&self, slot: u16, readonly: bool) -> Option<(String, bool)> {
        let topo = self.shared.topo.load();
        let mut rng = self.rng.get();
        let picked = route::pick(&topo, slot, readonly, self.shared.cfg.slave_mode, &mut rng)
            .map(|(a, r)| (a.to_string(), r));
        self.rng.set(rng);
        picked
    }

    fn master_addr(&self, slot: u16) -> Option<String> {
        let topo = self.shared.topo.load();
        topo.owner(slot)
            .map(|i| topo.nodes[i as usize].addr.clone())
    }

    async fn forward_any_master(&self, frame: Bytes) {
        let seq = self.alloc_seq();
        let addr = {
            let topo = self.shared.topo.load();
            let mut rng = self.rng.get();
            let a = route::any_master(&topo, &mut rng).map(str::to_string);
            self.rng.set(rng);
            a
        };
        let Some(addr) = addr else {
            self.emit_at(seq, error_frame(ERR_NO_OWNER));
            return;
        };
        let conn = self.shared.backends.shared(&addr, self.id, false);
        conn.send(Outbound {
            head: None,
            frame,
            expect: 1,
            seq,
            sink: Sink::Client(self.reply_tx.clone()),
        })
        .await;
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
        let Some(addr) = slot.and_then(|s| self.master_addr(s)) else {
            self.emit_at(seq, error_frame(ERR_NO_OWNER));
            return;
        };
        self.inflight.borrow_mut().insert(
            seq,
            InFlight {
                frame: frame.clone(),
                retried: false,
            },
        );
        let conn = self.shared.backends.shared(&addr, self.id, false);
        conn.send(Outbound {
            head: None,
            frame,
            expect: 1,
            seq,
            sink: Sink::Client(self.reply_tx.clone()),
        })
        .await;
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
        let Some((addr, is_replica)) = self.pick_addr(slot, spec.is_readonly()) else {
            self.emit_at(seq, error_frame(ERR_NO_OWNER));
            return;
        };
        self.inflight.borrow_mut().insert(
            seq,
            InFlight {
                frame: frame.clone(),
                retried: false,
            },
        );
        let conn = self.shared.backends.shared(&addr, self.id, is_replica);
        conn.send(Outbound {
            head: None,
            frame,
            expect: 1,
            seq,
            sink: Sink::Client(self.reply_tx.clone()),
        })
        .await;
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
        tokio::task::spawn_local(async move {
            let reply = blocking_round(&shared, slot, frame, None, false).await;
            let _ = reply_tx.send((seq, reply));
        });
    }

    fn fan_out(&self, spec: &Spec, frame: &Bytes, argc: usize) {
        let seq = self.alloc_seq();
        let readonly = spec.is_readonly();
        let mode = self.shared.cfg.slave_mode;
        let split = {
            let args = collect_args(frame, argc);
            let (keys, values): (Vec<&[u8]>, Option<Vec<&[u8]>>) = if spec.kind == Kind::Mset {
                let ks = args[1..].iter().step_by(2).copied().collect();
                let vs = args[2..].iter().step_by(2).copied().collect();
                (ks, Some(vs))
            } else {
                (args[1..].to_vec(), None)
            };
            if spec.kind == Kind::Mset && !(argc - 1).is_multiple_of(2) {
                self.emit_at(
                    seq,
                    error_frame("ERR wrong number of arguments for 'mset' command"),
                );
                return;
            }
            let total = keys.len();
            let mut rng = self.rng.get();
            let name_upper = spec.name.to_ascii_uppercase();
            let topo = self.shared.topo.load();
            let parts = multikey::split(name_upper.as_bytes(), &keys, values.as_deref(), |k| {
                route::pick(&topo, crc16::slot(k), readonly, mode, &mut rng)
                    .map(|(a, _)| a.to_string())
            });
            self.rng.set(rng);
            parts.map(|p| (p, total))
        };
        let (parts, total) = match split {
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
                let conn = shared.backends.shared(&part.addr, id, flag_readonly);
                conn.send(Outbound {
                    head: None,
                    frame: Bytes::from(part.frame.clone()),
                    expect: 1,
                    seq: 0,
                    sink: Sink::One(tx),
                })
                .await;
                receivers.push(rx);
            }
            let mut results: Vec<(Vec<usize>, Bytes)> = Vec::with_capacity(parts.len());
            for (part, rx) in parts.into_iter().zip(receivers) {
                let reply = rx
                    .await
                    .unwrap_or_else(|_| Bytes::from_static(ERR_BACKEND_LOST));
                results.push((part.positions, reply));
            }
            let merged = match kind {
                Kind::Mget => multikey::merge_mget(total, &results),
                Kind::Mset => {
                    multikey::merge_ok(&results.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>())
                }
                _ => {
                    multikey::merge_sum(&results.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>())
                }
            };
            let out = match merged {
                Ok(bytes) => Bytes::from(bytes),
                Err(err_frame) => err_frame,
            };
            let _ = reply_tx.send((seq, out));
        });
    }

    fn run_scan(&self, frame: &Bytes, argc: usize) {
        let (cursor, tail) = {
            let args = collect_args(frame, argc);
            let cursor = std::str::from_utf8(args[1])
                .ok()
                .and_then(|s| s.parse::<u64>().ok());
            let tail: Vec<Vec<u8>> = args[2..].iter().map(|a| a.to_vec()).collect();
            (cursor, tail)
        };
        let Some(cursor) = cursor else {
            self.emit_error("ERR invalid cursor");
            return;
        };
        let (master_idx, node_cursor) = multikey::unpack_cursor(cursor);
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let id = self.id;
        tokio::task::spawn_local(async move {
            let addr = {
                let topo = shared.topo.load();
                if master_idx >= topo.masters.len() {
                    let done = multikey::rebuild_scan_reply(0, b"*0\r\n");
                    let _ = reply_tx.send((seq, Bytes::from(done)));
                    return;
                }
                topo.nodes[topo.masters[master_idx] as usize].addr.clone()
            };
            let cursor_str = node_cursor.to_string();
            let mut sub_args: Vec<&[u8]> = vec![b"SCAN", cursor_str.as_bytes()];
            for t in &tail {
                sub_args.push(t);
            }
            let mut cmd = Vec::new();
            resp::write_command(&mut cmd, &sub_args);
            let (tx, rx) = oneshot::channel();
            let conn = shared.backends.shared(&addr, id, false);
            conn.send(Outbound {
                head: None,
                frame: Bytes::from(cmd),
                expect: 1,
                seq: 0,
                sink: Sink::One(tx),
            })
            .await;
            let reply = rx
                .await
                .unwrap_or_else(|_| Bytes::from_static(ERR_BACKEND_LOST));
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

    fn run_broadcast(&self, name: &'static [u8], merge: Merge) {
        let mut cmd = Vec::new();
        resp::write_command(&mut cmd, &[name]);
        self.run_broadcast_frame(Bytes::from(cmd), merge);
    }

    fn run_broadcast_frame(&self, frame: Bytes, merge: Merge) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let id = self.id;
        tokio::task::spawn_local(async move {
            let addrs: Vec<String> = {
                let topo = shared.topo.load();
                topo.masters
                    .iter()
                    .map(|&i| topo.nodes[i as usize].addr.clone())
                    .collect()
            };
            let mut receivers = Vec::with_capacity(addrs.len());
            for addr in &addrs {
                let (tx, rx) = oneshot::channel();
                let conn = shared.backends.shared(addr, id, false);
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
            let mut replies = Vec::with_capacity(receivers.len());
            for rx in receivers {
                replies.push(
                    rx.await
                        .unwrap_or_else(|_| Bytes::from_static(ERR_BACKEND_LOST)),
                );
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
                "exec" => None,
                _ => Some(error_frame_vec("ERR unsupported command")),
            }
        };
        match reply {
            Some(bytes) => self.emit_local(bytes),
            None if spec.name == "exec" => self.handle_exec().await,
            None => {}
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
        let addr = state.slot.and_then(|s| self.master_addr(s));
        let Some(addr) = addr else {
            self.emit_at(seq, error_frame(ERR_NO_OWNER));
            return;
        };
        let mut blob = Vec::new();
        blob.extend_from_slice(b"*1\r\n$5\r\nMULTI\r\n");
        for f in &state.frames {
            blob.extend_from_slice(f);
        }
        blob.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
        let expect = state.frames.len() as u32 + 2;
        let conn = self.shared.backends.shared(&addr, self.id, false);
        conn.send(Outbound {
            head: None,
            frame: Bytes::from(blob),
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

    fn dispatch_pubsub(&self, frame: Bytes, argc: usize) {
        let forward = {
            let args = collect_args(&frame, argc);
            let lower = args
                .first()
                .map(|n| n.to_ascii_lowercase())
                .unwrap_or_default();
            match lower.as_slice() {
                b"subscribe" | b"unsubscribe" | b"psubscribe" | b"punsubscribe" | b"ping" => true,
                b"quit" => {
                    self.closing.set(true);
                    false
                }
                b"reset" => {
                    if let Some(ps) = self.pubsub.borrow_mut().take() {
                        ps.task.abort();
                    }
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
            if let Some(ps) = self.pubsub.borrow().as_ref() {
                let _ = ps.tx.send(frame);
            }
        } else {
            self.emit_local(admin::OK.to_vec());
        }
    }

    async fn enter_pubsub(&self, first_frame: Bytes) {
        let addr = {
            let topo = self.shared.topo.load();
            let mut rng = self.rng.get();
            let a = route::any_master(&topo, &mut rng).map(str::to_string);
            self.rng.set(rng);
            a
        };
        let Some(addr) = addr else {
            self.emit_error(ERR_NO_OWNER);
            return;
        };
        let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
        let _ = tx.send(first_frame);
        let shared = self.shared.clone();
        let reply_tx = self.reply_tx.clone();
        let proto = self.proto.clone();
        let task = tokio::task::spawn_local(async move {
            pubsub_relay(shared, addr, rx, reply_tx, proto).await;
        });
        *self.pubsub.borrow_mut() = Some(PubsubHandle { tx, task });
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

enum Merge {
    Sum,
    Ok,
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
    let (addr, asking) = match redirect {
        Some((ask, target)) => (Some(target), ask),
        None => {
            let topo = shared.topo.load();
            (
                topo.owner(slot)
                    .map(|i| topo.nodes[i as usize].addr.clone()),
                false,
            )
        }
    };
    let Some(addr) = addr else {
        return error_frame(ERR_NO_OWNER);
    };
    let Some(conn) = shared.backends.take_exclusive(&addr, false) else {
        return error_frame("ERR too many blocking connections");
    };
    let (tx, rx) = oneshot::channel();
    conn.send(Outbound {
        head: asking.then(|| Bytes::from_static(ASKING_FRAME)),
        frame: frame.clone(),
        expect: if asking { 2 } else { 1 },
        seq: 0,
        sink: Sink::One(tx),
    })
    .await;
    let reply = rx
        .await
        .unwrap_or_else(|_| Bytes::from_static(ERR_BACKEND_LOST));
    shared.backends.put_exclusive(&addr, conn);
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
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    reply_tx: ReplyTx,
    proto: Rc<Cell<u8>>,
) {
    let stream = match crate::backend::dial_raw(&addr, &shared.cfg).await {
        Ok(s) => s,
        Err(e) => {
            log_debug!("pubsub dial {addr}: {e}");
            let _ = reply_tx.send((SEQ_OOB, error_frame("ERR pubsub backend unavailable")));
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
                if proto.get() >= 3 && frame.first() == Some(&b'*') {
                    frame[0] = b'>';
                }
                if reply_tx.send((SEQ_OOB, frame.freeze())).is_err() {
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
    inflight: Inflight,
    proto: Rc<Cell<u8>>,
    client_id: u64,
) {
    let mut next_emit: u64 = 0;
    let mut parked: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut batch: Vec<(u64, Bytes)> = Vec::with_capacity(64);
    let mut ready: Vec<Bytes> = Vec::with_capacity(64);
    loop {
        let n = rx.recv_many(&mut batch, 64).await;
        if n == 0 {
            return;
        }
        for (seq, frame) in batch.drain(..) {
            if seq == SEQ_OOB {
                ready.push(convert_nil(frame, proto.get()));
                continue;
            }
            if frame.first() == Some(&b'-')
                && let Some(req) = retryable(&inflight, seq, &frame)
                && let Some((ask, target)) = parse_redirect(&frame)
            {
                stats::bump(&shared.stats.workers[shared.worker].redirects);
                let _ = shared.refresh.send(());
                let conn = shared.backends.shared(&target, client_id, false);
                conn.send(Outbound {
                    head: ask.then(|| Bytes::from_static(ASKING_FRAME)),
                    frame: req,
                    expect: if ask { 2 } else { 1 },
                    seq,
                    sink: Sink::Client(reply_tx.clone()),
                })
                .await;
                continue;
            }
            parked.insert(seq, frame);
        }
        while let Some(frame) = parked.remove(&next_emit) {
            inflight.borrow_mut().remove(&next_emit);
            ready.push(convert_nil(frame, proto.get()));
            next_emit += 1;
        }
        if !ready.is_empty() {
            let total: usize = ready.iter().map(|f| f.len()).sum();
            let mut slices: Vec<IoSlice<'_>> = ready.iter().map(|f| IoSlice::new(f)).collect();
            if crate::backend::write_slices(&mut write_half, &mut slices)
                .await
                .is_err()
            {
                return;
            }
            stats::add(&shared.stats.workers[shared.worker].bytes_out, total as u64);
            ready.clear();
        }
    }
}

fn retryable(inflight: &Inflight, seq: u64, frame: &Bytes) -> Option<Bytes> {
    if !frame.starts_with(b"-MOVED ") && !frame.starts_with(b"-ASK ") {
        return None;
    }
    let mut inf = inflight.borrow_mut();
    match inf.get_mut(&seq) {
        Some(entry) if !entry.retried => {
            entry.retried = true;
            Some(entry.frame.clone())
        }
        _ => None,
    }
}

fn convert_nil(frame: Bytes, proto: u8) -> Bytes {
    if proto >= 3 && (frame.as_ref() == resp::NIL_BULK || frame.as_ref() == resp::NIL_ARRAY) {
        Bytes::from_static(resp::NIL_RESP3)
    } else {
        frame
    }
}

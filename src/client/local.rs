//! Commands the proxy answers itself, and MULTI/EXEC.

use bytes::Bytes;

use super::fanout::write_keys;
use super::pipe::{recv_or_lost, scatter_one};
use super::session::Session;
use super::{Cold, ERR_CROSSSLOT, ERR_NO_OWNER, error_frame};
use crate::command::{Kind, Spec};
use crate::resp;
use crate::{admin, crc16};

pub(super) struct MultiState {
    slot: Option<u16>,
    frames: Vec<Bytes>,
    write_keys: Vec<Bytes>,
    bytes: usize,
    aborted: bool,
}

impl MultiState {
    pub(super) fn queued(&self) -> usize {
        self.frames.len()
    }
}

const CLUSTER_DATABASES: &[u8] = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$17\r\ncluster-databases\r\n";

impl Session {
    pub(super) fn queue_multi(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let queueable = matches!(
            spec.kind,
            Kind::Single | Kind::MultiSum | Kind::Mget | Kind::Mset
        );
        if !queueable {
            self.abort_multi();
            self.emit_error(&format!(
                "ERR '{}' in MULTI / EXEC, only support keyed single-slot commands",
                spec.name
            ));
            return;
        }
        let current = self
            .multi
            .borrow()
            .as_ref()
            .and_then(|s| s.slot)
            .or_else(|| self.guarding_slot());
        let new_slot = spec
            .all_keys(resp::Args::new(&frame, argc).skip(1), argc)
            .map(crc16::slot)
            .try_fold(current, |acc, s| match acc {
                Some(prev) if prev != s => None,
                _ => Some(Some(s)),
            })
            .flatten();
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
                if self.cache().is_some() && spec.is_write() {
                    write_keys(spec, &frame, argc, |k| {
                        state.write_keys.push(frame.slice_ref(k))
                    });
                }
                state.frames.push(frame);
                drop(guard);
                self.emit_local(Bytes::from_static(b"+QUEUED\r\n"));
            }
        }
    }

    pub(super) fn handle_local(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let reply = match spec.name {
            "ping" if argc == 1 => Some(Bytes::from_static(resp::PONG)),
            "time" => Some(Bytes::from(admin::time())),
            "info" => Some(Bytes::from(admin::info(
                &self.shared.cfg,
                &self.shared.stats,
                self.shared.started,
            ))),
            "quit" => {
                self.closing.set(true);
                Some(Bytes::from_static(resp::OK))
            }
            "reset" => {
                if self.do_reset() {
                    return;
                }
                Some(Bytes::from_static(b"+RESET\r\n"))
            }
            "multi" => {
                if self.in_multi.get() {
                    Some(error_frame("ERR MULTI calls can not be nested"))
                } else {
                    *self.multi.borrow_mut() = Some(MultiState {
                        slot: None,
                        frames: Vec::new(),
                        write_keys: Vec::new(),
                        bytes: 0,
                        aborted: false,
                    });
                    self.in_multi.set(true);
                    Some(Bytes::from_static(resp::OK))
                }
            }
            "discard" => {
                if self.take_multi().is_some() {
                    if self.release_watch(Bytes::from_static(resp::OK)) {
                        return;
                    }
                    Some(Bytes::from_static(resp::OK))
                } else {
                    Some(error_frame("ERR DISCARD without MULTI"))
                }
            }
            "unwatch" => {
                if self.release_watch(Bytes::from_static(resp::OK)) {
                    return;
                }
                Some(Bytes::from_static(resp::OK))
            }
            _ => self.handle_local_args(spec, &collect_args(&frame, argc)),
        };
        if let Some(bytes) = reply {
            self.emit_local(bytes);
        }
    }

    pub(super) fn handle_exec(&self) -> Option<Cold<'_>> {
        let Some(state) = self.take_multi() else {
            self.emit_error("ERR EXEC without MULTI");
            return None;
        };
        // a guarding watch owns the transaction; otherwise the slot's newest generation, still
        // draining, keeps it ordered behind what that connection accepted
        let watched = {
            let watch = self.link.watch.borrow();
            match watch.as_ref() {
                Some(w) if w.guarding() && w.db == self.link.db.get() => Some(w.clone()),
                _ => state.slot.and_then(|s| self.link.generation_in_db(s)),
            }
        };
        if state.aborted {
            let abort = error_frame("EXECABORT Transaction discarded because of previous errors.");
            if !self.release_watch(abort.clone()) {
                self.emit_local(abort);
            }
            return None;
        }
        let Some(slot) = state.slot.or(watched.as_ref().map(|w| w.slot)) else {
            self.emit_local(Bytes::from_static(b"*0\r\n"));
            return None;
        };
        if let Some(cache) = &self.shared.cache {
            cache.invalidate_all(state.write_keys.iter().map(|k| &k[..]));
        }
        let seq = self.alloc_seq();
        let expect = state.frames.len() as u32 + 2;
        let mut blob = Vec::with_capacity(state.bytes + 32);
        blob.extend_from_slice(b"*1\r\n$5\r\nMULTI\r\n");
        for f in &state.frames {
            blob.extend_from_slice(f);
        }
        blob.extend_from_slice(b"*1\r\n$4\r\nEXEC\r\n");
        let blob = Bytes::from(blob);
        if let Some(watched) = watched {
            if !self.fanouts_pending() {
                self.exec_watched(watched, seq, blob, expect);
                return None;
            }
            return Some(Box::pin(async move {
                if self.wait_fanouts(&[slot]).await {
                    self.exec_watched(watched, seq, blob, expect);
                } else {
                    self.closing.set(true);
                }
            }));
        }
        self.gated(slot, move |s| {
            s.route_single(seq, slot, false, blob, expect, None)
        })
    }

    /// SELECT: database 0 is the proxy's own answer; any other index is checked against the
    /// cluster's `cluster-databases` before the session switches to it.
    pub(super) async fn handle_select(&self, frame: Bytes, argc: usize) {
        let index = resp::Args::new(&frame, argc)
            .nth(1)
            .and_then(|a| std::str::from_utf8(a).ok()?.parse::<i64>().ok());
        let Some(index) = index else {
            self.emit_error("ERR invalid DB index");
            return;
        };
        if index == 0 {
            let changed = self.link.db.get() != 0;
            self.set_db(0);
            let seq = self.alloc_seq();
            self.deliver_select(seq, Bytes::from_static(resp::OK), changed);
            return;
        }
        // re-selecting the current database is already validated: answer without a probe
        if index == i64::from(self.link.db.get()) {
            let seq = self.alloc_seq();
            self.emit_at(seq, Bytes::from_static(resp::OK));
            return;
        }
        let seq = self.alloc_seq();
        let Some(addr) = self.any_master_addr() else {
            self.emit_at(seq, Bytes::from_static(ERR_NO_OWNER));
            return;
        };
        let probe = Bytes::from_static(CLUSTER_DATABASES);
        let reply =
            recv_or_lost(scatter_one(&self.shared, &addr, self.lane(), None, probe).await).await;
        let reply = match databases(&reply) {
            None if reply.first() == Some(&b'-') => reply,
            None => error_frame("ERR SELECT is not allowed in cluster mode"),
            Some(count) => match u8::try_from(index) {
                Ok(db) if i64::from(db) < count => {
                    let changed = self.link.db.get() != db;
                    self.set_db(db);
                    return self.deliver_select(seq, Bytes::from_static(resp::OK), changed);
                }
                _ => error_frame("ERR DB index is out of range"),
            },
        };
        self.emit_at(seq, reply);
    }

    // ends any active WATCH when the database changes, so later commands reach the new one
    fn deliver_select(&self, seq: u64, reply: Bytes, changed: bool) {
        if changed && self.end_watch_at(seq, reply.clone()) {
            return;
        }
        self.emit_at(seq, reply);
    }

    /// Resets the session; true when the RESET reply follows the watch release instead.
    pub(super) fn do_reset(&self) -> bool {
        self.set_db(0);
        self.take_multi();
        let delivered = self.release_watch(Bytes::from_static(b"+RESET\r\n"));
        self.store_name("");
        self.proto.set(2);
        self.link.proto_switches.push(self.link.next_seq.get(), 2);
        let generation = self.shared.acl.generation();
        let user = self.shared.acl.default_user();
        self.authed.set(user.enabled && user.nopass);
        self.adopt(user, generation);
        delivered
    }

    pub(super) fn abort_multi(&self) {
        if let Some(state) = self.multi.borrow_mut().as_mut() {
            state.aborted = true;
        }
    }

    fn handle_local_args(&self, spec: &Spec, args: &[&[u8]]) -> Option<Bytes> {
        match spec.name {
            "ping" => Some(Bytes::from(admin::ping(args))),
            "echo" => Some(Bytes::from(admin::echo(args))),
            "config" => Some(Bytes::from(admin::config_cmd(
                args,
                &self.shared.cfg,
                &self.shared.acl,
                &self.shared.stats,
            ))),
            "cluster" => Some(Bytes::from(admin::cluster(
                args,
                &self.shared.cfg,
                self.proto.get(),
            ))),
            "command" => Some(Bytes::from(admin::command_reply(args, self.proto.get()))),
            "auth" => {
                self.handle_auth(args);
                None
            }
            "hello" => {
                self.handle_hello(args);
                None
            }
            "acl" => Some(self.handle_acl(args)),
            "client" => {
                self.handle_client_cmd(args);
                None
            }
            _ => Some(error_frame("ERR unsupported command")),
        }
    }

    fn handle_auth(&self, args: &[&[u8]]) {
        let (name, password) = match args {
            [_, _] if self.shared.acl.default_user().nopass => {
                self.emit_error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
                return;
            }
            [_, password] => (&b"default"[..], *password),
            [_, name, password] => (*name, *password),
            _ => {
                self.emit_error("ERR wrong number of arguments for 'auth' command");
                return;
            }
        };
        if self.login("auth", name, password) {
            self.emit_local(Bytes::from_static(resp::OK));
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
                if !self.login("hello", args[i + 1], args[i + 2]) {
                    self.emit_error(
                        "WRONGPASS invalid username-password pair or user is disabled.",
                    );
                    return;
                }
                i += 3;
            } else if args[i].eq_ignore_ascii_case(b"setname") && i + 1 < args.len() {
                if let Err(e) = self.set_name(args[i + 1]) {
                    self.emit_error(e);
                    return;
                }
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
        self.link
            .proto_switches
            .push(self.link.next_seq.get(), proto);
        let mut out = Vec::new();
        if proto >= 3 {
            out.extend_from_slice(b"%7\r\n");
        } else {
            out.extend_from_slice(b"*14\r\n");
        }
        resp::bulk(&mut out, b"server");
        resp::bulk(&mut out, b"redis");
        resp::bulk(&mut out, b"version");
        resp::bulk(&mut out, admin::SERVER_VERSION.as_bytes());
        resp::bulk(&mut out, b"proto");
        resp::integer(&mut out, i64::from(proto));
        resp::bulk(&mut out, b"id");
        resp::integer(&mut out, self.id as i64);
        resp::bulk(&mut out, b"mode");
        resp::bulk(&mut out, b"cluster");
        resp::bulk(&mut out, b"role");
        resp::bulk(&mut out, b"master");
        resp::bulk(&mut out, b"modules");
        out.extend_from_slice(b"*0\r\n");
        self.emit_local(out);
    }

    fn handle_client_cmd(&self, args: &[&[u8]]) {
        let sub = |name: &[u8]| args.get(1).is_some_and(|s| s.eq_ignore_ascii_case(name));
        if sub(b"id") {
            let mut out = Vec::new();
            resp::integer(&mut out, self.id as i64);
            self.emit_local(out);
        } else if sub(b"setname") && args.len() == 3 {
            match self.set_name(args[2]) {
                Ok(()) => self.emit_local(Bytes::from_static(resp::OK)),
                Err(e) => self.emit_error(e),
            }
        } else if sub(b"list") {
            self.emit_local(admin::client_list(&self.shared.stats));
        } else if sub(b"getname") {
            let reply = match self.shared.stats.registry().get(&self.id) {
                Some(c) if !c.name.is_empty() => {
                    let mut out = Vec::new();
                    resp::bulk(&mut out, c.name.as_bytes());
                    Bytes::from(out)
                }
                _ => Bytes::from_static(resp::NIL_BULK),
            };
            self.emit_local(reply);
        } else {
            self.emit_error("ERR unsupported CLIENT subcommand");
        }
    }

    fn set_name(&self, name: &[u8]) -> Result<(), &'static str> {
        match std::str::from_utf8(name) {
            Ok(name) if name.bytes().all(|b| (b'!'..=b'~').contains(&b)) => {
                self.store_name(name);
                Ok(())
            }
            _ => Err("ERR Client names cannot contain spaces, newlines or special characters."),
        }
    }

    fn store_name(&self, name: &str) {
        if let Some(c) = self.shared.stats.registry().get_mut(&self.id) {
            c.name = Box::from(name);
        }
    }

    fn take_multi(&self) -> Option<MultiState> {
        self.in_multi.set(false);
        self.multi.borrow_mut().take()
    }
}

pub(super) fn collect_args(frame: &Bytes, argc: usize) -> Vec<&[u8]> {
    resp::Args::new(frame, argc).collect()
}

// echoed names are CR/LF-stripped and capped so they cannot forge a second frame
pub(super) fn display_name(raw: &[u8]) -> String {
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

// the value of a CONFIG GET reply; an empty array (an unknown config: Redis) is None
fn databases(reply: &[u8]) -> Option<i64> {
    let (n, _) = resp::scan_int_line(reply, 1)?;
    let mut args = resp::Args::new(reply, usize::try_from(n).ok()?);
    std::str::from_utf8(args.nth(1)?).ok()?.parse().ok()
}

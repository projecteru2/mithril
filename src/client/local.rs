//! Commands the proxy answers itself, and MULTI/EXEC.

use bytes::Bytes;

use super::fanout::{key_pairs, write_keys};
use super::session::Session;
use super::{Cold, ERR_CROSSSLOT, error_frame};
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
        let current = self.multi.borrow().as_ref().and_then(|s| s.slot);
        let new_slot = key_pairs(spec, &frame, argc)
            .map(|(k, _)| crc16::slot(k))
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
                if self.shared.cache.is_some() && spec.is_write() {
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
                self.do_reset();
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
                    Some(Bytes::from_static(resp::OK))
                } else {
                    Some(error_frame("ERR DISCARD without MULTI"))
                }
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
        if state.aborted {
            self.emit_error("EXECABORT Transaction discarded because of previous errors.");
            return None;
        }
        let Some(slot) = state.slot else {
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
        self.gated(slot, move |s| {
            s.route_single(seq, slot, false, blob, expect, None)
        })
    }

    pub(super) fn do_reset(&self) {
        self.take_multi();
        self.store_name("");
        self.proto.set(2);
        self.link.proto_switches.push(self.link.next_seq.get(), 2);
        self.authed.set(self.shared.cfg.requirepass.is_empty());
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
            "select" => Some(Bytes::from(admin::select(args))),
            "config" => Some(Bytes::from(admin::config_cmd(args, &self.shared.cfg))),
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
            "acl" => match args.get(1) {
                Some(sub) if sub.eq_ignore_ascii_case(b"whoami") => {
                    let mut out = Vec::new();
                    resp::bulk(&mut out, b"default");
                    Some(Bytes::from(out))
                }
                _ => Some(error_frame("ERR unsupported ACL subcommand")),
            },
            "client" => {
                self.handle_client_cmd(args);
                None
            }
            _ => Some(error_frame("ERR unsupported command")),
        }
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

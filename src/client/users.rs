//! The session's ACL user: login, the per-command permission check, and the ACL command.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use super::error_frame;
use super::session::Session;
use crate::acl::{self, ERR_NOPERM_CHANNEL, ERR_NOPERM_KEY, LogEntry, User};
use crate::command::{self, Spec};
use crate::resp;

const STANDARD_CATEGORIES: usize = 22;
const LOG_DEFAULT_COUNT: usize = 10;
// what a log entry keeps of a client-supplied name or key
const LOG_FIELD_MAX: usize = 128;

impl Session {
    /// Authenticates as `name` on behalf of `cmd` (AUTH or HELLO); a failure lands in the ACL log.
    pub(super) fn login(&self, cmd: &str, name: &[u8], password: &[u8]) -> bool {
        let generation = self.shared.acl.generation();
        match self.shared.acl.user(name) {
            Some(user) if user.accepts(password) => {
                self.adopt(user, generation);
                self.authed.set(true);
                true
            }
            _ => {
                self.deny("auth", cmd, b"AUTH", Some(name));
                false
            }
        }
    }

    /// Switches to `user`, resolved after `generation` was read from the table.
    pub(super) fn adopt(&self, user: Arc<User>, generation: u64) {
        self.unrestricted.set(user.unrestricted());
        self.acl_gen.set(generation);
        *self.user.borrow_mut() = user;
    }

    /// Re-resolves the user after the table changed; false once it was deleted.
    pub(super) fn refresh_user(&self) -> bool {
        let generation = self.shared.acl.generation();
        let name = self.user.borrow().name.clone();
        match self.shared.acl.user(name.as_bytes()) {
            Some(user) => {
                self.adopt(user, generation);
                true
            }
            None => false,
        }
    }

    /// The error a restricted user gets for this request, if any.
    pub(super) fn acl_denies(&self, spec: &Spec, frame: &Bytes, argc: usize) -> Option<Bytes> {
        let user = self.user.borrow();
        let sub = resp::Args::new(frame, argc).nth(1);
        if !user.may_run(spec, sub) {
            drop(user);
            self.deny("command", spec.name, spec.name.as_bytes(), None);
            return Some(error_frame(&format!(
                "NOPERM this user has no permissions to run the '{}' command",
                spec.name
            )));
        }
        for key in spec.all_keys(resp::Args::new(frame, argc).skip(1), argc) {
            if !user.may_touch(key) {
                drop(user);
                self.deny("key", spec.name, key, None);
                return Some(Bytes::from_static(ERR_NOPERM_KEY));
            }
        }
        let mut channels = resp::Args::new(frame, argc).skip(1);
        let denied = match spec.name {
            "publish" => channels.take(1).find(|c| !user.may_use_channel(c)),
            "subscribe" => channels.find(|c| !user.may_use_channel(c)),
            "psubscribe" => channels.find(|c| !user.may_use_pattern(c)),
            _ => None,
        };
        if let Some(channel) = denied {
            drop(user);
            self.deny("channel", spec.name, channel, None);
            return Some(Bytes::from_static(ERR_NOPERM_CHANNEL));
        }
        None
    }

    fn deny(&self, reason: &'static str, cmd: &str, object: &[u8], username: Option<&[u8]>) {
        let clip = |v: &[u8]| {
            Box::from(String::from_utf8_lossy(&v[..v.len().min(LOG_FIELD_MAX)]).as_ref())
        };
        let current: Box<str> = clip(self.user.borrow().name.as_bytes());
        let username = username.map_or_else(|| current.clone(), clip);
        // the info line is space-delimited key=value pairs: names must stay one plain token
        let token = |v: &str| -> String {
            v.chars()
                .map(|c| {
                    if c.is_whitespace() || c == '=' {
                        '_'
                    } else {
                        c
                    }
                })
                .collect()
        };
        let (subs, patterns) = self.subs.borrow().counts();
        let queued = self
            .multi
            .borrow()
            .as_ref()
            .map_or(-1, |m| m.queued() as i64);
        let client = {
            let registry = self.shared.stats.registry();
            let info = registry.get(&self.id);
            format!(
                "id={} addr={} fd={} name={} age={} idle=0 flags=N db=0 sub={subs} psub={patterns} ssub=0 multi={queued} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={cmd} user={} redir=-1 resp={}",
                self.id,
                info.map_or(String::new(), |i| i.addr.to_string()),
                info.map_or(-1, |i| i.fd),
                info.map_or(String::new(), |i| token(&i.name)),
                info.map_or(0, |i| i.since.elapsed().as_secs()),
                token(&current),
                self.proto.get(),
            )
        };
        self.shared.acl.log_denial(LogEntry {
            reason,
            context: if self.in_multi.get() {
                "multi"
            } else {
                "toplevel"
            },
            object: clip(object),
            username,
            client: Box::from(client.as_str()),
            at: Instant::now(),
        });
    }

    pub(super) fn handle_acl(&self, args: &[&[u8]]) -> Bytes {
        let acl = &self.shared.acl;
        let mut out = Vec::new();
        let n = args.len();
        let sub = |name: &[u8]| args.get(1).is_some_and(|s| s.eq_ignore_ascii_case(name));
        if sub(b"whoami") && n == 2 {
            resp::bulk(&mut out, self.user.borrow().name.as_bytes());
        } else if sub(b"cat") && n <= 3 {
            acl_cat(&mut out, args.get(2).copied());
        } else if sub(b"setuser") && n >= 3 {
            match std::str::from_utf8(args[2]) {
                Ok(name) => match acl.set_user(name, &args[3..]) {
                    Ok(()) => out.extend_from_slice(resp::OK),
                    Err(e) => resp::write_error(&mut out, &e),
                },
                Err(_) => resp::write_error(&mut out, "ERR Usernames must be UTF-8"),
            }
        } else if sub(b"getuser") && n == 3 {
            match acl.user(args[2]) {
                Some(user) => acl_getuser(&mut out, &user),
                None => out.extend_from_slice(resp::NIL_BULK),
            }
        } else if sub(b"deluser") && n >= 3 {
            match acl.del_users(&args[2..]) {
                Ok(n) => resp::integer(&mut out, n as i64),
                Err(e) => resp::write_error(&mut out, e),
            }
        } else if sub(b"users") && n == 2 {
            acl_users(&mut out, acl, |out, user| {
                resp::bulk(out, user.name.as_bytes())
            });
        } else if sub(b"list") && n == 2 {
            acl_users(&mut out, acl, |out, user| resp::bulk(out, &user.describe()));
        } else if sub(b"genpass") && n <= 3 {
            let bits = match args.get(2) {
                Some(arg) => command::arg_int(arg)
                    .filter(|b| *b > 0)
                    .map_or(0, |b| b as u64),
                None => 256,
            };
            match acl::genpass(bits) {
                Ok(pass) => resp::bulk(&mut out, pass.as_bytes()),
                Err(e) => resp::write_error(&mut out, e),
            }
        } else if sub(b"log") && n <= 3 {
            acl_log(&mut out, acl, args.get(2).copied());
        } else if sub(b"load") || sub(b"save") {
            resp::write_error(
                &mut out,
                "ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.",
            );
        } else {
            resp::write_error(
                &mut out,
                "ERR unknown subcommand or wrong number of arguments for 'ACL'",
            );
        }
        Bytes::from(out)
    }
}

// the 22 Redis categories; module categories stay reachable through +@read/+@write and by name
fn acl_cat(out: &mut Vec<u8>, category: Option<&[u8]>) {
    let names = &command::cat_names()[..STANDARD_CATEGORIES.min(command::cat_names().len())];
    let Some(category) = category else {
        resp::array_header(out, names.len());
        for name in names {
            resp::bulk(out, &name.as_bytes()[1..]);
        }
        return;
    };
    let Some(bit) = command::cat_names()
        .iter()
        .position(|c| c.as_bytes()[1..].eq_ignore_ascii_case(category))
    else {
        resp::write_error(
            out,
            &format!(
                "ERR Unknown category '{}'",
                String::from_utf8_lossy(category)
            ),
        );
        return;
    };
    let members: Vec<Vec<u8>> = command::table()
        .iter()
        .flat_map(|spec| {
            let own = (spec.cats & (1 << bit) != 0).then(|| spec.name.as_bytes().to_vec());
            own.into_iter().chain(
                spec.subs
                    .iter()
                    .filter(|s| s.cats & (1 << bit) != 0)
                    .map(|s| [spec.name.as_bytes(), b"|", s.name.as_bytes()].concat()),
            )
        })
        .collect();
    resp::array_header(out, members.len());
    for name in &members {
        resp::bulk(out, name);
    }
}

fn acl_users(out: &mut Vec<u8>, acl: &acl::Acl, mut render: impl FnMut(&mut Vec<u8>, &User)) {
    let users = acl.users();
    resp::array_header(out, users.len());
    for user in users {
        render(out, &user);
    }
}

fn acl_getuser(out: &mut Vec<u8>, user: &User) {
    out.extend_from_slice(b"*10\r\n");
    resp::bulk(out, b"flags");
    let flags = user.flags();
    resp::array_header(out, flags.len());
    for flag in flags {
        resp::bulk(out, flag.as_bytes());
    }
    resp::bulk(out, b"passwords");
    let hashes: Vec<String> = user.password_hashes().collect();
    resp::array_header(out, hashes.len());
    for hash in &hashes {
        resp::bulk(out, hash.as_bytes());
    }
    resp::bulk(out, b"commands");
    resp::bulk(out, user.describe_commands().as_bytes());
    resp::bulk(out, b"keys");
    resp::array_header(out, user.key_patterns().len());
    for key in user.key_patterns() {
        resp::bulk(out, key);
    }
    resp::bulk(out, b"channels");
    resp::array_header(out, user.channel_patterns().len());
    for channel in user.channel_patterns() {
        resp::bulk(out, channel);
    }
}

fn acl_log(out: &mut Vec<u8>, acl: &acl::Acl, arg: Option<&[u8]>) {
    let count = match arg {
        None => LOG_DEFAULT_COUNT,
        Some(arg) if arg.eq_ignore_ascii_case(b"reset") => {
            acl.log_reset();
            out.extend_from_slice(resp::OK);
            return;
        }
        Some(arg) => match command::arg_int(arg) {
            Some(n) if n >= 0 => n as usize,
            _ => {
                resp::write_error(out, "ERR value is not an integer or out of range");
                return;
            }
        },
    };
    let now = Instant::now();
    let entries = acl.log_entries(count, |e| {
        let mut entry = Vec::new();
        entry.extend_from_slice(b"*14\r\n");
        resp::bulk(&mut entry, b"count");
        resp::integer(&mut entry, 1);
        resp::bulk(&mut entry, b"reason");
        resp::bulk(&mut entry, e.reason.as_bytes());
        resp::bulk(&mut entry, b"context");
        resp::bulk(&mut entry, e.context.as_bytes());
        resp::bulk(&mut entry, b"object");
        resp::bulk(&mut entry, e.object.as_bytes());
        resp::bulk(&mut entry, b"username");
        resp::bulk(&mut entry, e.username.as_bytes());
        resp::bulk(&mut entry, b"age-seconds");
        let age = now.saturating_duration_since(e.at).as_secs_f64();
        resp::bulk(&mut entry, format!("{age:.3}").as_bytes());
        resp::bulk(&mut entry, b"client-info");
        resp::bulk(&mut entry, e.client.as_bytes());
        entry
    });
    resp::array_header(out, entries.len());
    for entry in entries {
        out.extend_from_slice(&entry);
    }
}

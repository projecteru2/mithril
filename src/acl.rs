//! ACL users: passwords, command, key and channel rules, the process-wide table and denial log.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Instant;

use crate::command::{self, Spec};
use crate::config::Config;

pub const ERR_NOPERM_KEY: &[u8] =
    b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n";
pub const ERR_NOPERM_CHANNEL: &[u8] =
    b"-NOPERM this user has no permissions to access one of the channels used as arguments\r\n";
const DEFAULT_LOG_MAX: usize = 128;
const GENPASS_MAX_BITS: u64 = 4096;
const HEX: &[u8; 16] = b"0123456789abcdef";
const HASH_HEX_LEN: usize = 64;
const WORDS: usize = command::MAX_COMMANDS / 64;

/// One user: whether it may log in, with what, and what it may then run and touch.
#[derive(Clone)]
pub struct User {
    pub name: Box<str>,
    pub enabled: bool,
    pub nopass: bool,
    passwords: Vec<[u8; 32]>,
    // one bit per command or subcommand id
    allowed: [u64; WORDS],
    keys: Vec<Box<[u8]>>,
    channels: Vec<Box<[u8]>>,
}

impl User {
    fn new(name: &str, all_channels: bool) -> User {
        User {
            name: Box::from(name),
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            allowed: [0; WORDS],
            keys: Vec::new(),
            channels: if all_channels {
                vec![Box::from(&b"*"[..])]
            } else {
                Vec::new()
            },
        }
    }

    /// True when no command, key or channel check can deny this user.
    pub fn unrestricted(&self) -> bool {
        self.all_commands() && has_star(&self.keys) && has_star(&self.channels)
    }

    pub fn accepts(&self, password: &[u8]) -> bool {
        self.enabled && (self.nopass || self.passwords.contains(&sha256(password)))
    }

    /// Whether the command is allowed: a known subcommand by its own bit, anything else
    /// (including a subcommand the table does not know) by the command's bit.
    pub fn may_run(&self, spec: &Spec, sub: Option<&[u8]>) -> bool {
        let id = sub
            .and_then(|s| spec.subcommand(s))
            .map_or(spec.id, |s| s.id);
        self.has(id)
    }

    fn has(&self, id: u16) -> bool {
        self.allowed[id as usize / 64] & (1 << (id % 64)) != 0
    }

    pub fn may_touch(&self, key: &[u8]) -> bool {
        self.keys.iter().any(|p| glob_match(p, key))
    }

    pub fn may_use_channel(&self, channel: &[u8]) -> bool {
        self.channels.iter().any(|p| glob_match(p, channel))
    }

    /// A subscription pattern must be one the user holds verbatim.
    pub fn may_use_pattern(&self, pattern: &[u8]) -> bool {
        self.channels.iter().any(|p| &**p == pattern)
    }

    pub fn flags(&self) -> Vec<&'static str> {
        let mut flags = vec![if self.enabled { "on" } else { "off" }];
        if has_star(&self.keys) {
            flags.push("allkeys");
        }
        if has_star(&self.channels) {
            flags.push("allchannels");
        }
        if self.all_commands() {
            flags.push("allcommands");
        }
        if self.nopass {
            flags.push("nopass");
        }
        flags
    }

    pub fn password_hashes(&self) -> impl Iterator<Item = String> + '_ {
        self.passwords.iter().map(|h| hex(h))
    }

    pub fn key_patterns(&self) -> &[Box<[u8]>] {
        &self.keys
    }

    pub fn channel_patterns(&self) -> &[Box<[u8]>] {
        &self.channels
    }

    /// The command rules in Redis's canonical form: what ACL SETUSER would take to rebuild them.
    pub fn describe_commands(&self) -> String {
        if self.all_commands() {
            return "+@all".to_string();
        }
        let table = command::table();
        let mut out = String::from("-@all");
        let mut covered = [0u64; WORDS];
        let is_covered =
            |covered: &[u64; WORDS], id: u16| covered[id as usize / 64] & (1 << (id % 64)) != 0;
        for (bit, cat) in command::cat_names().iter().enumerate() {
            let members: Vec<u16> = table
                .iter()
                .flat_map(|s| {
                    std::iter::once((s.id, s.cats)).chain(s.subs.iter().map(|x| (x.id, x.cats)))
                })
                .filter(|(_, cats)| cats & (1 << bit) != 0)
                .map(|(id, _)| id)
                .collect();
            if members.is_empty() || !members.iter().all(|&id| self.has(id)) {
                continue;
            }
            out.push_str(" +");
            out.push_str(cat);
            for id in members {
                covered[id as usize / 64] |= 1 << (id % 64);
            }
        }
        for spec in table {
            let whole = self.has(spec.id) && spec.subs.iter().all(|s| self.has(s.id));
            if whole {
                if !is_covered(&covered, spec.id)
                    || spec.subs.iter().any(|s| !is_covered(&covered, s.id))
                {
                    out.push_str(" +");
                    out.push_str(spec.name);
                }
                continue;
            }
            for sub in spec.subs {
                if self.has(sub.id) && !is_covered(&covered, sub.id) {
                    out.push_str(" +");
                    out.push_str(spec.name);
                    out.push('|');
                    out.push_str(sub.name);
                }
            }
        }
        out
    }

    /// Renders the user as one ACL LIST line; patterns keep their bytes.
    pub fn describe(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"user ");
        out.extend_from_slice(self.name.as_bytes());
        out.extend_from_slice(if self.enabled { b" on" } else { b" off" });
        if self.nopass {
            out.extend_from_slice(b" nopass");
        }
        for hash in self.password_hashes() {
            out.extend_from_slice(b" #");
            out.extend_from_slice(hash.as_bytes());
        }
        for key in &self.keys {
            out.extend_from_slice(b" ~");
            out.extend_from_slice(key);
        }
        for channel in &self.channels {
            out.extend_from_slice(b" &");
            out.extend_from_slice(channel);
        }
        out.push(b' ');
        out.extend_from_slice(self.describe_commands().as_bytes());
        out
    }

    fn apply(&mut self, rule: &[u8], all_channels: bool) -> Result<(), String> {
        let syntax = || {
            format!(
                "Error in ACL SETUSER modifier '{}': Syntax error",
                lossy(rule)
            )
        };
        match rule {
            b"on" => self.enabled = true,
            b"off" => self.enabled = false,
            b"nopass" => {
                self.nopass = true;
                self.passwords.clear();
            }
            b"resetpass" => {
                self.nopass = false;
                self.passwords.clear();
            }
            b"allkeys" | b"~*" => set_star(&mut self.keys),
            b"resetkeys" => self.keys.clear(),
            b"allchannels" | b"&*" => set_star(&mut self.channels),
            b"resetchannels" => self.channels.clear(),
            b"allcommands" | b"+@all" => self.set_all(true),
            b"nocommands" | b"-@all" => self.set_all(false),
            b"reset" => {
                *self = User::new(&self.name, all_channels);
            }
            b"" => return Err(syntax()),
            _ => {
                let (op, arg) = rule.split_at(1);
                match op[0] {
                    b'>' => {
                        self.nopass = false;
                        self.passwords.push(sha256(arg));
                    }
                    b'<' => self.drop_password(sha256(arg), rule)?,
                    b'#' => {
                        self.nopass = false;
                        self.passwords
                            .push(parse_hash(arg).ok_or_else(|| bad_hash(rule))?);
                    }
                    b'!' => {
                        self.drop_password(parse_hash(arg).ok_or_else(|| bad_hash(rule))?, rule)?
                    }
                    b'~' => add_pattern(&mut self.keys, arg, "allkeys", "resetkeys", "patterns")
                        .map_err(|e| {
                            format!("Error in ACL SETUSER modifier '{}': {e}", lossy(rule))
                        })?,
                    b'&' => add_pattern(
                        &mut self.channels,
                        arg,
                        "allchannels",
                        "resetchannels",
                        "channels",
                    )
                    .map_err(|e| format!("Error in ACL SETUSER modifier '{}': {e}", lossy(rule)))?,
                    b'+' | b'-' => self.apply_command(op[0] == b'+', arg, rule)?,
                    _ => return Err(syntax()),
                }
            }
        }
        Ok(())
    }

    fn apply_command(&mut self, allow: bool, arg: &[u8], rule: &[u8]) -> Result<(), String> {
        let unknown = || {
            format!(
                "Error in ACL SETUSER modifier '{}': Unknown command or category name in ACL",
                lossy(rule)
            )
        };
        if let Some(cat) = arg.strip_prefix(b"@") {
            let bit = command::cat_names()
                .iter()
                .position(|c| c.as_bytes()[1..].eq_ignore_ascii_case(cat))
                .ok_or_else(unknown)?;
            for spec in command::table() {
                if spec.cats & (1 << bit) != 0 {
                    self.set(spec.id, allow);
                }
                for sub in spec.subs.iter().filter(|s| s.cats & (1 << bit) != 0) {
                    self.set(sub.id, allow);
                }
                self.sync_container(spec);
            }
            return Ok(());
        }
        let (name, sub) = match arg.iter().position(|&b| b == b'|') {
            Some(i) => (&arg[..i], Some(&arg[i + 1..])),
            None => (arg, None),
        };
        let spec = command::lookup(name).ok_or_else(unknown)?;
        match sub {
            None => {
                self.set(spec.id, allow);
                for sub in spec.subs {
                    self.set(sub.id, allow);
                }
            }
            Some(sub) => {
                self.set(spec.subcommand(sub).ok_or_else(unknown)?.id, allow);
                self.sync_container(spec);
            }
        }
        Ok(())
    }

    fn drop_password(&mut self, hash: [u8; 32], rule: &[u8]) -> Result<(), String> {
        let n = self.passwords.len();
        self.passwords.retain(|h| *h != hash);
        if self.passwords.len() == n {
            return Err(format!(
                "Error in ACL SETUSER modifier '{}': The password you are trying to remove from the user does not exist",
                lossy(rule)
            ));
        }
        Ok(())
    }

    // a container's own bit means "every subcommand": kept derived
    fn sync_container(&mut self, spec: &Spec) {
        if !spec.subs.is_empty() {
            let all = spec.subs.iter().all(|s| self.has(s.id));
            self.set(spec.id, all);
        }
    }

    fn set(&mut self, id: u16, allow: bool) {
        let (word, bit) = (id as usize / 64, 1u64 << (id % 64));
        if allow {
            self.allowed[word] |= bit;
        } else {
            self.allowed[word] &= !bit;
        }
    }

    fn set_all(&mut self, allow: bool) {
        self.allowed = if allow { full_words() } else { [0; WORDS] };
    }

    fn all_commands(&self) -> bool {
        self.allowed == full_words()
    }
}

/// One denied action, as ACL LOG reports it.
pub struct LogEntry {
    pub reason: &'static str,
    pub context: &'static str,
    pub object: Box<str>,
    pub username: Box<str>,
    pub client: Box<str>,
    pub at: Instant,
}

/// The process-wide user table and denial log.
pub struct Acl {
    users: RwLock<HashMap<Box<str>, Arc<User>>>,
    generation: AtomicU64,
    all_channels: AtomicBool,
    log: Mutex<VecDeque<LogEntry>>,
    log_max: AtomicUsize,
}

impl Acl {
    /// Builds the table from the config: the default user carries `requirepass`, `user` lines the rest.
    pub fn new(cfg: &Config) -> Result<Arc<Acl>, String> {
        let acl = Arc::new(Acl {
            users: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(1),
            all_channels: AtomicBool::new(cfg.acl_pubsub_default_all),
            log: Mutex::new(VecDeque::new()),
            log_max: AtomicUsize::new(cfg.acllog_max_len),
        });
        let mut default = User::new("default", true);
        default.enabled = true;
        set_star(&mut default.keys);
        default.set_all(true);
        if cfg.requirepass.is_empty() {
            default.nopass = true;
        } else {
            default.passwords.push(sha256(cfg.requirepass.as_bytes()));
        }
        acl.write().insert(Box::from("default"), Arc::new(default));
        for line in &cfg.users {
            let mut parts = line.split_whitespace();
            let name = parts.next().ok_or("user line without a name")?;
            let rules: Vec<&[u8]> = parts.map(str::as_bytes).collect();
            acl.set_user(name, &rules)
                .map_err(|e| format!("user {name}: {e}"))?;
        }
        Ok(acl)
    }

    /// The user every connection starts as.
    pub fn default_user(&self) -> Arc<User> {
        // del_users refuses "default", so the entry made in new() is always there
        #[allow(clippy::expect_used)]
        self.read()
            .get("default")
            .cloned()
            .expect("the default user is never removed")
    }

    pub fn user(&self, name: &[u8]) -> Option<Arc<User>> {
        let name = std::str::from_utf8(name).ok()?;
        self.read().get(name).cloned()
    }

    /// Bumps after every change; a session re-resolves its user once it sees it move.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Applies `rules` to `name`, creating it; nothing changes when a rule fails.
    pub fn set_user(&self, name: &str, rules: &[&[u8]]) -> Result<(), String> {
        let all_channels = self.all_channels.load(Ordering::Relaxed);
        // clone, apply and swap under the one write lock: two SETUSERs never lose each other
        let mut users = self.write();
        let mut user = match users.get(name) {
            Some(u) => (**u).clone(),
            None => User::new(name, all_channels),
        };
        for rule in rules {
            user.apply(rule, all_channels)
                .map_err(|e| format!("ERR {e}"))?;
        }
        users.insert(Box::from(name), Arc::new(user));
        drop(users);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Removes users by name; returns how many existed.
    pub fn del_users(&self, names: &[&[u8]]) -> Result<usize, &'static str> {
        if names.iter().any(|n| *n == b"default") {
            return Err("ERR The 'default' user cannot be removed");
        }
        let mut users = self.write();
        let removed = names
            .iter()
            .filter(|n| std::str::from_utf8(n).is_ok_and(|n| users.remove(n).is_some()))
            .count();
        drop(users);
        self.generation.fetch_add(1, Ordering::Release);
        Ok(removed)
    }

    pub fn users(&self) -> Vec<Arc<User>> {
        let mut users: Vec<Arc<User>> = self.read().values().cloned().collect();
        users.sort_by(|a, b| a.name.cmp(&b.name));
        users
    }

    pub fn all_channels_default(&self) -> bool {
        self.all_channels.load(Ordering::Relaxed)
    }

    pub fn set_all_channels_default(&self, all: bool) {
        self.all_channels.store(all, Ordering::Relaxed);
    }

    pub fn log_max(&self) -> usize {
        self.log_max.load(Ordering::Relaxed)
    }

    pub fn set_log_max(&self, max: usize) {
        self.log_max.store(max, Ordering::Relaxed);
        let mut log = self.lock_log();
        while log.len() > max {
            log.pop_back();
        }
    }

    pub fn log_denial(&self, entry: LogEntry) {
        let mut log = self.lock_log();
        let max = self.log_max();
        log.push_front(entry);
        while log.len() > max {
            log.pop_back();
        }
    }

    /// The newest `count` entries, newest first.
    pub fn log_entries<T>(&self, count: usize, f: impl FnMut(&LogEntry) -> T) -> Vec<T> {
        self.lock_log().iter().take(count).map(f).collect()
    }

    pub fn log_reset(&self) {
        self.lock_log().clear();
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Box<str>, Arc<User>>> {
        self.users.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<Box<str>, Arc<User>>> {
        self.users.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_log(&self) -> MutexGuard<'_, VecDeque<LogEntry>> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// `bits` random bits as lowercase hex, as ACL GENPASS defines it.
pub fn genpass(bits: u64) -> Result<String, &'static str> {
    if bits == 0 || bits > GENPASS_MAX_BITS {
        return Err(
            "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096",
        );
    }
    let bytes = (bits as usize).div_ceil(8);
    let mut raw = vec![0u8; bytes];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut raw))
        .map_err(|_| "ERR no entropy source")?;
    let mut out = hex(&raw);
    out.truncate((bits as usize).div_ceil(4));
    Ok(out)
}

pub fn default_log_max() -> usize {
    DEFAULT_LOG_MAX
}

/// Redis glob matching: `*`, `?`, `[...]` with ranges and negation, `\` escapes.
pub fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    let (mut p, mut i) = (0, 0);
    while p < pattern.len() {
        match pattern[p] {
            b'*' => {
                while p + 1 < pattern.len() && pattern[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pattern.len() {
                    return true;
                }
                return (i..=s.len()).any(|k| glob_match(&pattern[p + 1..], &s[k..]));
            }
            b'?' => {
                if i >= s.len() {
                    return false;
                }
                i += 1;
            }
            b'[' => {
                p += 1;
                let negate = p < pattern.len() && pattern[p] == b'^';
                if negate {
                    p += 1;
                }
                let Some(&c) = s.get(i) else {
                    return false;
                };
                let mut matched = false;
                loop {
                    match pattern.get(p) {
                        None | Some(b']') => break,
                        Some(b'\\') if p + 1 < pattern.len() => {
                            p += 1;
                            matched |= pattern[p] == c;
                        }
                        Some(&lo) if p + 2 < pattern.len() && pattern[p + 1] == b'-' => {
                            let hi = pattern[p + 2];
                            let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
                            matched |= (lo..=hi).contains(&c);
                            p += 2;
                        }
                        Some(&x) => matched |= x == c,
                    }
                    p += 1;
                }
                if matched == negate {
                    return false;
                }
                i += 1;
            }
            b'\\' if p + 1 < pattern.len() => {
                p += 1;
                if s.get(i) != Some(&pattern[p]) {
                    return false;
                }
                i += 1;
            }
            c => {
                if s.get(i) != Some(&c) {
                    return false;
                }
                i += 1;
            }
        }
        p += 1;
    }
    i == s.len()
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((data.len() as u64) * 8).to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (slot, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *slot = slot.wrapping_add(v);
        }
    }
    let mut out = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 15) as usize] as char);
    }
    out
}

fn parse_hash(arg: &[u8]) -> Option<[u8; 32]> {
    if arg.len() != HASH_HEX_LEN {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in arg.chunks(2).enumerate() {
        let nibble = |c: u8| HEX.iter().position(|&h| h == c);
        out[i] = (nibble(pair[0])? << 4 | nibble(pair[1])?) as u8;
    }
    Some(out)
}

fn bad_hash(rule: &[u8]) -> String {
    format!(
        "Error in ACL SETUSER modifier '{}': The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters",
        lossy(rule)
    )
}

// every command and subcommand id set, the rest of the bitmap clear
fn full_words() -> [u64; WORDS] {
    let n = command::entries();
    let mut words = [0u64; WORDS];
    for (i, word) in words.iter_mut().enumerate() {
        let have = n.saturating_sub(i * 64).min(64);
        *word = if have == 64 {
            u64::MAX
        } else {
            (1 << have) - 1
        };
    }
    words
}

fn has_star(patterns: &[Box<[u8]>]) -> bool {
    patterns.iter().any(|p| &**p == b"*")
}

fn set_star(patterns: &mut Vec<Box<[u8]>>) {
    patterns.clear();
    patterns.push(Box::from(&b"*"[..]));
}

fn add_pattern(
    patterns: &mut Vec<Box<[u8]>>,
    pattern: &[u8],
    all: &str,
    reset: &str,
    noun: &str,
) -> Result<(), String> {
    if pattern == b"*" {
        set_star(patterns);
    } else if has_star(patterns) {
        return Err(format!(
            "Adding a pattern after the * pattern (or the '{all}' flag) is not valid and does not have any effect. Try '{reset}' to start with an empty list of {noun}"
        ));
    } else if !patterns.iter().any(|p| &**p == pattern) {
        patterns.push(Box::from(pattern));
    }
    Ok(())
}

fn lossy(rule: &[u8]) -> String {
    String::from_utf8_lossy(rule).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl() -> Arc<Acl> {
        Acl::new(&Config::default()).unwrap()
    }

    fn rules(s: &str) -> Vec<&[u8]> {
        s.split_whitespace().map(str::as_bytes).collect()
    }

    #[test]
    fn sha256_matches_known_vectors() {
        assert_eq!(
            hex(&sha256(b"123456")),
            "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92"
        );
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn glob_matches_like_redis() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"foo:*", b"foo:1"));
        assert!(!glob_match(b"foo:*", b"bar:1"));
        assert!(glob_match(b"f?o", b"foo"));
        assert!(glob_match(b"[a-c]x", b"bx"));
        assert!(!glob_match(b"[^a-c]x", b"bx"));
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"abc", b"abcd"));
        assert!(glob_match(b"", b""));
    }

    #[test]
    fn default_user_follows_requirepass() {
        let open = acl();
        let user = open.user(b"default").unwrap();
        assert!(user.nopass && user.enabled && user.unrestricted());
        assert!(user.accepts(b"anything"));
        assert_eq!(user.describe(), b"user default on nopass ~* &* +@all");
        let cfg = Config {
            requirepass: "secret".to_string(),
            ..Config::default()
        };
        let closed = Acl::new(&cfg).unwrap();
        let user = closed.user(b"default").unwrap();
        assert!(!user.nopass && user.accepts(b"secret") && !user.accepts(b"nope"));
    }

    #[test]
    fn rules_build_the_canonical_description() {
        let acl = acl();
        acl.set_user(
            "test_on",
            &rules("on #8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92 ~* &* -@all +@set +@string +config|set"),
        )
        .unwrap();
        let user = acl.user(b"test_on").unwrap();
        assert_eq!(user.flags(), ["on", "allkeys", "allchannels"]);
        assert_eq!(user.describe_commands(), "-@all +@set +@string +config|set");
        assert_eq!(
            user.describe(),
            b"user test_on on #8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92 ~* &* -@all +@set +@string +config|set"
        );
        assert!(user.accepts(b"123456"));
        let set = command::lookup(b"set").unwrap();
        let lpush = command::lookup(b"lpush").unwrap();
        let config = command::lookup(b"config").unwrap();
        assert!(user.may_run(set, None));
        assert!(!user.may_run(lpush, None));
        assert!(user.may_run(config, Some(b"SET")));
        assert!(!user.may_run(config, Some(b"get")));
        acl.set_user("test_on", &rules("+@all +acl|whoami"))
            .unwrap();
        assert_eq!(acl.user(b"test_on").unwrap().describe_commands(), "+@all");
        acl.set_user("test_on", &rules("reset")).unwrap();
        let user = acl.user(b"test_on").unwrap();
        assert_eq!(user.flags(), ["off", "allchannels"]);
        assert_eq!(user.describe_commands(), "-@all");
        assert!(user.key_patterns().is_empty());
        acl.set_all_channels_default(false);
        acl.set_user("test_on", &rules("reset")).unwrap();
        assert_eq!(acl.user(b"test_on").unwrap().flags(), ["off"]);
    }

    #[test]
    fn categories_and_rules_reach_subcommands() {
        let acl = acl();
        let acl_cmd = command::lookup(b"acl").unwrap();
        let config = command::lookup(b"config").unwrap();
        acl.set_user("u", &rules("on +@all -@dangerous")).unwrap();
        let user = acl.user(b"u").unwrap();
        assert!(!user.may_run(acl_cmd, Some(b"setuser")));
        assert!(user.may_run(acl_cmd, Some(b"whoami")));
        assert!(!user.may_run(config, Some(b"get")));
        assert!(user.may_run(command::lookup(b"get").unwrap(), None));
        let described: String = user.describe_commands();
        assert!(!described.contains("acl|setuser") && described.contains("+acl|whoami"));
        acl.set_user("rt", &rules(&format!("on {described}")))
            .unwrap();
        assert_eq!(acl.user(b"rt").unwrap().allowed, user.allowed);
        acl.set_user("u", &rules("+config -config|set")).unwrap();
        let user = acl.user(b"u").unwrap();
        assert!(user.may_run(config, Some(b"get")) && !user.may_run(config, Some(b"set")));
        assert!(!user.may_run(config, Some(b"nosuch")));
        acl.set_user("u", &rules("+config")).unwrap();
        assert!(acl.user(b"u").unwrap().may_run(config, Some(b"nosuch")));
        assert!(acl.set_user("u", &rules("+config|nosuch")).is_err());
        assert!(acl.set_user("u", &[b""]).is_err());
        acl.set_user("b", &[b"on", b"~\xff\x00k"]).unwrap();
        assert!(
            acl.user(b"b")
                .unwrap()
                .describe()
                .windows(4)
                .any(|w| w == b"~\xff\x00k")
        );
    }

    #[test]
    fn passwords_add_remove_and_validate() {
        let acl = acl();
        acl.set_user("p", &rules("on >654321")).unwrap();
        assert!(acl.user(b"p").unwrap().accepts(b"654321"));
        assert!(acl.set_user("p", &rules("<nothere")).is_err());
        assert!(acl.set_user("p", &rules("#zz")).is_err());
        acl.set_user("p", &rules("<654321")).unwrap();
        assert!(!acl.user(b"p").unwrap().accepts(b"654321"));
        assert!(acl.set_user("p", &rules("+nosuchcommand")).is_err());
        assert!(acl.set_user("p", &rules("+@nosuchcat")).is_err());
        assert!(acl.set_user("p", &rules("off")).is_ok());
        assert!(!acl.user(b"p").unwrap().accepts(b"anything"));
        assert_eq!(acl.del_users(&[b"p", b"missing"]).unwrap(), 1);
        assert!(acl.del_users(&[b"default"]).is_err());
    }

    #[test]
    fn keys_and_channels_follow_their_patterns() {
        let acl = acl();
        acl.set_user(
            "k",
            &rules("on nopass +@all resetkeys ~foo:* ~bar:* resetchannels &chan*"),
        )
        .unwrap();
        let user = acl.user(b"k").unwrap();
        assert!(user.may_touch(b"foo:1") && user.may_touch(b"bar:2") && !user.may_touch(b"key"));
        assert!(user.may_use_channel(b"chan1") && !user.may_use_channel(b"other"));
        assert!(user.may_use_pattern(b"chan*") && !user.may_use_pattern(b"chanx*"));
        assert!(!user.unrestricted());
        assert!(acl.set_user("k", &rules("&more")).is_ok());
        assert!(acl.set_user("k", &rules("~* ~more")).is_err());
        assert!(acl.set_user("k", &rules("&* &more")).is_err());
    }

    #[test]
    fn genpass_sizes_follow_the_bits() {
        assert_eq!(genpass(4096).unwrap().len(), 1024);
        assert_eq!(genpass(1023).unwrap().len(), 256);
        assert_eq!(genpass(1).unwrap().len(), 1);
        assert!(genpass(0).is_err() && genpass(4097).is_err());
    }

    #[test]
    fn log_keeps_the_newest_up_to_its_cap() {
        let acl = acl();
        let entry = |n: usize| LogEntry {
            reason: "command",
            context: "toplevel",
            object: Box::from(format!("cmd{n}").as_str()),
            username: Box::from("u"),
            client: Box::from(""),
            at: Instant::now(),
        };
        for n in 0..5 {
            acl.log_denial(entry(n));
        }
        assert_eq!(
            acl.log_entries(2, |e| e.object.to_string()),
            ["cmd4", "cmd3"]
        );
        acl.set_log_max(1);
        assert_eq!(acl.log_entries(10, |e| e.object.to_string()), ["cmd4"]);
        acl.log_reset();
        assert!(acl.log_entries(10, |_| ()).is_empty());
    }
}

//! Per-worker counters, cacheline-padded, summed on demand for INFO.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::command;

/// Ticks per second in /proc stat fields (Linux USER_HZ).
pub const USER_HZ: u64 = 100;

/// The `cmd` of a client that has run nothing yet.
pub const NO_COMMAND: u16 = u16::MAX;

/// Newest entries SLOWLOG GET returns without a count.
pub const SLOWLOG_GET_DEFAULT: usize = 10;
/// Arguments a slow-log entry keeps; the rest is one summary, as in Redis.
const SLOWLOG_ARGC_MAX: usize = 32;
/// Bytes of one argument a slow-log entry keeps; the rest is summarized, as in Redis.
const SLOWLOG_ARG_MAX: usize = 128;

/// One worker's counters; padding keeps writers on distinct cachelines.
#[repr(align(64))]
#[derive(Default)]
pub struct WorkerStats {
    pub commands: AtomicU64,
    pub errors: AtomicU64,
    pub redirects: AtomicU64,
    pub redirect_waits: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub readers_exited: AtomicU64,
    pub writers_exited: AtomicU64,
    pub sessions_closed: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_invalidations: AtomicU64,
    pub cache_armed: AtomicU64,
    pub cache_entries: AtomicU64,
    pub cache_bytes: AtomicU64,
    pub cache_flips: AtomicU64,
    pub calls: Calls,
}

/// Calls per command, indexed by the dense command id.
pub struct Calls(Box<[AtomicU64]>);

impl Calls {
    pub fn at(&self, id: u16) -> &AtomicU64 {
        &self.0[usize::from(id)]
    }
}

impl Default for Calls {
    fn default() -> Calls {
        Calls((0..command::entries()).map(|_| AtomicU64::new(0)).collect())
    }
}

/// What CLIENT LIST reports about one connection; `cmd` is the session's
/// own relaxed store, the rest is touched only on connect, disconnect and SETNAME.
pub struct ClientInfo {
    pub addr: SocketAddr,
    pub fd: i32,
    pub name: Box<str>,
    pub since: Instant,
    pub cmd: Arc<AtomicU16>,
}

/// One command the slow log kept, as SLOWLOG GET reports it; `args` is the RESP array of
/// its clipped, redacted arguments.
#[derive(Clone)]
pub struct SlowEntry {
    pub id: u64,
    pub at: u64,
    pub micros: u64,
    pub args: Bytes,
    pub addr: Box<str>,
    pub name: Box<str>,
}

/// The slow log: a bounded ring behind the thresholds CONFIG SET changes at run time.
pub struct Slowlog {
    /// Microseconds from which a command is kept; -1 keeps nothing.
    pub slower_than: AtomicI64,
    pub max_len: AtomicUsize,
    ring: Mutex<(VecDeque<SlowEntry>, u64)>,
}

impl Slowlog {
    pub fn threshold(&self) -> i64 {
        self.slower_than.load(Ordering::Relaxed)
    }

    pub fn count(&self) -> usize {
        self.ring().0.len()
    }

    /// The newest `count` entries, newest first.
    pub fn newest(&self, count: usize) -> Vec<SlowEntry> {
        self.ring().0.iter().rev().take(count).cloned().collect()
    }

    pub fn reset(&self) {
        self.ring().0 = VecDeque::new();
    }

    fn record(&self, mut entry: SlowEntry) {
        let max = self.max_len.load(Ordering::Relaxed);
        let mut ring = self.ring();
        entry.id = ring.1;
        ring.1 += 1;
        ring.0.push_back(entry);
        while ring.0.len() > max {
            ring.0.pop_front();
        }
    }

    fn ring(&self) -> MutexGuard<'_, (VecDeque<SlowEntry>, u64)> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for Slowlog {
    fn default() -> Slowlog {
        Slowlog {
            slower_than: AtomicI64::new(-1),
            max_len: AtomicUsize::new(128),
            ring: Mutex::new((VecDeque::new(), 0)),
        }
    }
}

/// Process-wide stats shared across workers.
pub struct Stats {
    pub workers: Vec<Arc<WorkerStats>>,
    pub clients: AtomicUsize,
    pub total_connections: AtomicU64,
    pub registry: Mutex<HashMap<u64, ClientInfo>>,
    pub slowlog: Slowlog,
    epoch: Instant,
}

impl Stats {
    pub fn new(workers: usize) -> Arc<Stats> {
        Arc::new(Stats {
            workers: (0..workers).map(|_| Arc::default()).collect(),
            clients: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            registry: Mutex::new(HashMap::new()),
            slowlog: Slowlog::default(),
            epoch: Instant::now(),
        })
    }

    pub fn registry(&self) -> MutexGuard<'_, HashMap<u64, ClientInfo>> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Microseconds since the process started, never zero once it is a microsecond old.
    pub fn micros(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }

    /// Keeps a command that started at `started_us` when it ran at least the slow threshold.
    pub fn log_slow(&self, client_id: u64, started_us: u64, frame: Bytes) {
        let micros = self.micros().saturating_sub(started_us);
        if micros < self.slowlog.threshold() as u64 {
            return;
        }
        let (addr, name) = match self.registry().get(&client_id) {
            Some(c) => (Box::from(c.addr.to_string()), c.name.clone()),
            None => (Box::from(""), Box::from("")),
        };
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.slowlog.record(SlowEntry {
            id: 0,
            at,
            micros,
            args: Bytes::from(slow_args(&frame)),
            addr,
            name,
        });
    }

    pub fn sum<F: Fn(&WorkerStats) -> &AtomicU64>(&self, field: F) -> u64 {
        self.workers
            .iter()
            .map(|w| field(w).load(Ordering::Relaxed))
            .sum()
    }
}

/// Bumps a single-writer counter.
pub fn bump(counter: &AtomicU64) {
    add(counter, 1);
}

/// Adds to a single-writer counter; cross-thread readers use relaxed loads.
pub fn add(counter: &AtomicU64, n: u64) {
    counter.store(counter.load(Ordering::Relaxed) + n, Ordering::Relaxed);
}

// the entry keeps at most 32 arguments of 128 bytes, credentials replaced, and a MULTI blob
// as the EXEC it stands for
fn slow_args(frame: &[u8]) -> Vec<u8> {
    let mut cur = crate::resp::Cursor::default();
    let (len, argc) = match crate::resp::scan_request_at(frame, &mut cur) {
        crate::resp::ReqScan::Complete { len, argc } => (len, argc),
        _ => (frame.len(), 0),
    };
    let args: Vec<&[u8]> = crate::resp::Args::new(frame, argc).collect();
    let mut out = Vec::new();
    if len < frame.len() && args.len() == 1 && args[0].eq_ignore_ascii_case(b"multi") {
        crate::resp::write_command(&mut out, &[b"exec"]);
        return out;
    }
    let shown = args.len().min(SLOWLOG_ARGC_MAX);
    crate::resp::array_header(&mut out, shown);
    for (i, arg) in args.iter().take(shown).enumerate() {
        if i + 1 == SLOWLOG_ARGC_MAX && args.len() > SLOWLOG_ARGC_MAX {
            let more = format!("... ({} more arguments)", args.len() - SLOWLOG_ARGC_MAX + 1);
            crate::resp::bulk(&mut out, more.as_bytes());
        } else if sensitive(&args, i) {
            crate::resp::bulk(&mut out, b"(redacted)");
        } else if arg.len() > SLOWLOG_ARG_MAX {
            let mut clipped = arg[..SLOWLOG_ARG_MAX].to_vec();
            clipped.extend_from_slice(
                format!("... ({} more bytes)", arg.len() - SLOWLOG_ARG_MAX).as_bytes(),
            );
            crate::resp::bulk(&mut out, &clipped);
        } else {
            crate::resp::bulk(&mut out, arg);
        }
    }
    out
}

fn sensitive(args: &[&[u8]], i: usize) -> bool {
    let is = |k: usize, name: &[u8]| args.get(k).is_some_and(|a| a.eq_ignore_ascii_case(name));
    if is(0, b"auth") || is(0, b"hello") {
        return i >= 1;
    }
    if is(0, b"acl") && is(1, b"setuser") {
        return i >= 3;
    }
    is(0, b"config")
        && is(1, b"set")
        && i >= 3
        && (i - 3).is_multiple_of(2)
        && (is(i - 1, b"requirepass") || is(i - 1, b"backend-auth-pass"))
}

/// This thread's user+system CPU time in USER_HZ ticks; None where /proc is absent.
pub fn thread_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/thread-self/stat").ok()?;
    let rest = &stat[stat.rfind(')')? + 2..];
    let mut fields = rest.split(' ').skip(11);
    let utime: u64 = fields.next()?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some(utime + stime)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(args: &[&str]) -> Bytes {
        let mut out = Vec::new();
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        crate::resp::write_command(&mut out, &args);
        Bytes::from(out)
    }

    #[test]
    fn slowlog_keeps_the_newest_entries_up_to_max_len() {
        let stats = Stats::new(1);
        stats.slowlog.slower_than.store(0, Ordering::Relaxed);
        stats.slowlog.max_len.store(2, Ordering::Relaxed);
        for k in ["a", "b", "c"] {
            stats.log_slow(7, 1, frame(&["get", k]));
        }
        let newest = stats.slowlog.newest(10);
        assert_eq!(newest.len(), 2);
        assert_eq!((newest[0].id, newest[1].id), (2, 1));
        assert!(newest[0].micros > 0);
        assert_eq!(&*newest[0].addr, "");
        assert_eq!(stats.slowlog.newest(1).len(), 1);
        stats.slowlog.reset();
        assert_eq!(stats.slowlog.count(), 0);
        stats.log_slow(7, 1, frame(&["get", "d"]));
        assert_eq!(stats.slowlog.newest(10)[0].id, 3);
    }

    #[test]
    fn slow_args_clip_redact_and_name_a_transaction() {
        let text = |args: &[&str]| String::from_utf8_lossy(&slow_args(&frame(args))).into_owned();
        let long = "x".repeat(SLOWLOG_ARG_MAX + 5);
        assert!(text(&["set", "k", &long]).ends_with("... (5 more bytes)\r\n"));
        let mut wide = vec!["mget"];
        wide.extend(std::iter::repeat_n("k", SLOWLOG_ARGC_MAX + 3));
        let t = text(&wide);
        assert!(
            t.starts_with("*32\r\n") && t.ends_with("... (5 more arguments)\r\n"),
            "{t}"
        );
        let redacted = "$10\r\n(redacted)\r\n";
        assert_eq!(
            text(&["AUTH", "user", "pw"]),
            format!("*3\r\n$4\r\nAUTH\r\n{redacted}{redacted}")
        );
        assert!(text(&["hello", "3", "auth", "u", "pw"]).ends_with(&redacted.repeat(4)));
        assert!(
            text(&["acl", "setuser", "bob", ">pw", "on"])
                .ends_with(&format!("$3\r\nbob\r\n{redacted}{redacted}"))
        );
        assert_eq!(
            text(&["config", "set", "loglevel", "notice", "requirepass", "pw"]),
            format!(
                "*6\r\n$6\r\nconfig\r\n$3\r\nset\r\n$8\r\nloglevel\r\n$6\r\nnotice\r\n$11\r\nrequirepass\r\n{redacted}"
            )
        );
        assert!(text(&["config", "set", "loglevel", "debug"]).ends_with("$5\r\ndebug\r\n"));
        assert_eq!(text(&["multi"]), "*1\r\n$5\r\nmulti\r\n");
        let mut rules = vec!["acl", "setuser", "bob"];
        rules.extend(std::iter::repeat_n(">pw", 40));
        let t = text(&rules);
        assert!(t.ends_with("... (12 more arguments)\r\n"), "{t}");
        assert_eq!(t.matches("(redacted)").count(), 28, "{t}");
        let mut blob = frame(&["multi"]).to_vec();
        blob.extend_from_slice(&frame(&["set", "k", "v"]));
        blob.extend_from_slice(&frame(&["exec"]));
        assert_eq!(
            String::from_utf8_lossy(&slow_args(&blob)),
            "*1\r\n$4\r\nexec\r\n"
        );
    }

    #[test]
    fn slowlog_skips_commands_under_the_threshold() {
        let stats = Stats::new(1);
        stats
            .slowlog
            .slower_than
            .store(1_000_000_000, Ordering::Relaxed);
        stats.log_slow(7, stats.micros().max(1), frame(&["get", "a"]));
        assert_eq!(stats.slowlog.count(), 0);
        stats.slowlog.slower_than.store(-1, Ordering::Relaxed);
        stats.log_slow(7, 1, frame(&["get", "a"]));
        assert_eq!(stats.slowlog.count(), 0);
    }
}

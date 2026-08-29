//! Reply cache: worker-local GET cache, invalidated by RESP3 BCAST tracking.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::backend;
use crate::config::Config;
use crate::log_debug;
use crate::resp;
use crate::stats::{self, Stats};
use crate::topology::Topology;

// larger replies churn the byte budget faster than they earn hits
const ENTRY_MAX_BYTES: usize = 64 * 1024;
// per-entry map slot, key box, and refcount bookkeeping
const ENTRY_OVERHEAD: usize = 64;
const TRACKER_POLL: Duration = Duration::from_secs(1);
const TRACKER_RETRY: Duration = Duration::from_secs(1);
const TRACKER_PING: Duration = Duration::from_secs(2);
// unanswered pings before a silent tracker is declared dead
const PING_DEBT_MAX: u32 = 3;
const PING_FRAME: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const TRACKING_FRAME: &[u8] =
    b"*4\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$5\r\nBCAST\r\n";

/// FNV-1a; integer keys route here through their native-bytes default.
#[derive(Default)]
pub struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut h = if self.0 == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.0
        };
        for &b in bytes {
            h = (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3);
        }
        self.0 = h;
    }
}

type Map = HashMap<Box<[u8]>, Entry, BuildHasherDefault<KeyHasher>>;
type Fills = HashMap<Bytes, bool, BuildHasherDefault<KeyHasher>>;

struct Entry {
    frame: Bytes,
    at: Instant,
}

/// Worker-local reply cache; two generations flip under a byte budget.
pub struct ReplyCache {
    max_bytes: usize,
    max_age: Duration,
    hot: RefCell<Map>,
    prev: RefCell<Map>,
    hot_bytes: Cell<usize>,
    prev_bytes: Cell<usize>,
    // in-flight fills by key; true = an invalidation raced the reply
    fills: RefCell<Fills>,
    ready: Cell<usize>,
    masters: Cell<usize>,
    stats: Arc<Stats>,
    worker: usize,
}

impl ReplyCache {
    pub fn new(cfg: &Config, stats: Arc<Stats>, worker: usize) -> Rc<ReplyCache> {
        Rc::new(ReplyCache {
            max_bytes: cfg.reply_cache_max_bytes,
            max_age: Duration::from_secs(cfg.reply_cache_max_age_secs),
            hot: RefCell::new(Map::default()),
            prev: RefCell::new(Map::default()),
            hot_bytes: Cell::new(0),
            prev_bytes: Cell::new(0),
            fills: RefCell::new(Fills::default()),
            ready: Cell::new(0),
            masters: Cell::new(0),
            stats,
            worker,
        })
    }

    /// Returns the cached reply for `key` if fresh.
    pub fn lookup(&self, key: &[u8]) -> Option<Bytes> {
        let now = Instant::now();
        if let Some(e) = self.hot.borrow().get(key) {
            if now.duration_since(e.at) <= self.max_age {
                return Some(e.frame.clone());
            }
            return None;
        }
        let promoted = self.prev.borrow_mut().remove_entry(key);
        let (k, e) = promoted?;
        self.prev_bytes
            .set(self.prev_bytes.get() - entry_size(&k, &e.frame));
        if now.duration_since(e.at) > self.max_age {
            return None;
        }
        let frame = e.frame.clone();
        self.insert_hot(k, e);
        Some(frame)
    }

    /// Arms a fill for `key`; false while tracking coverage is incomplete.
    pub fn begin_fill(&self, key: &Bytes) -> bool {
        if !self.armed() {
            return false;
        }
        self.fills.borrow_mut().insert(key.clone(), false);
        true
    }

    /// Caches an armed fill's reply unless an invalidation raced it.
    pub fn complete_fill(&self, key: &Bytes, frame: &Bytes) {
        let Some(poisoned) = self.fills.borrow_mut().remove(key) else {
            return;
        };
        if poisoned
            || !self.armed()
            || frame.first() != Some(&b'$')
            || frame.len() > ENTRY_MAX_BYTES
        {
            return;
        }
        self.insert_hot(
            Box::from(&key[..]),
            Entry {
                frame: frame.clone(),
                at: Instant::now(),
            },
        );
    }

    /// Forgets a fill whose reply will never be observed.
    pub fn abandon_fill(&self, key: &Bytes) {
        self.fills.borrow_mut().remove(key);
    }

    /// Drops `key` from both generations and poisons its in-flight fill.
    pub fn invalidate(&self, key: &[u8]) {
        stats::bump(&self.stats.workers[self.worker].cache_invalidations);
        if let Some((k, e)) = self.hot.borrow_mut().remove_entry(key) {
            self.hot_bytes
                .set(self.hot_bytes.get() - entry_size(&k, &e.frame));
        }
        if let Some((k, e)) = self.prev.borrow_mut().remove_entry(key) {
            self.prev_bytes
                .set(self.prev_bytes.get() - entry_size(&k, &e.frame));
        }
        if let Some(p) = self.fills.borrow_mut().get_mut(key) {
            *p = true;
        }
    }

    /// Empties both generations and poisons every in-flight fill.
    pub fn clear(&self) {
        drop(std::mem::take(&mut *self.hot.borrow_mut()));
        drop(std::mem::take(&mut *self.prev.borrow_mut()));
        self.hot_bytes.set(0);
        self.prev_bytes.set(0);
        for p in self.fills.borrow_mut().values_mut() {
            *p = true;
        }
    }

    fn armed(&self) -> bool {
        self.masters.get() > 0 && self.ready.get() == self.masters.get()
    }

    fn insert_hot(&self, k: Box<[u8]>, e: Entry) {
        let size = entry_size(&k, &e.frame);
        if self.hot_bytes.get() + size > self.max_bytes / 2 {
            let dropped = std::mem::take(&mut *self.prev.borrow_mut());
            *self.prev.borrow_mut() = std::mem::take(&mut *self.hot.borrow_mut());
            self.prev_bytes.set(self.hot_bytes.get());
            self.hot_bytes.set(0);
            drop(dropped);
        }
        let mut hot = self.hot.borrow_mut();
        let mut grown = size;
        if let Some(old) = hot.insert(k, e) {
            grown -= old.frame.len();
        }
        self.hot_bytes.set(self.hot_bytes.get() + grown);
    }

    fn set_masters(&self, n: usize) {
        if self.masters.get() != n {
            self.masters.set(n);
            self.clear();
        }
        self.publish_armed();
    }

    fn tracker_up(&self) {
        self.ready.set(self.ready.get() + 1);
        self.publish_armed();
    }

    // a gone tracker means missed invalidations: nothing cached survives
    fn tracker_down(&self) {
        self.ready.set(self.ready.get().saturating_sub(1));
        self.clear();
        self.publish_armed();
    }

    fn publish_armed(&self) {
        self.stats.workers[self.worker]
            .cache_armed
            .store(u64::from(self.armed()), Ordering::Relaxed);
    }
}

fn entry_size(key: &[u8], frame: &Bytes) -> usize {
    key.len() + frame.len() + ENTRY_OVERHEAD
}

/// Keeps one tracking connection per master alive; spawned once per worker.
pub async fn run_trackers(cache: Rc<ReplyCache>, topo: Arc<ArcSwap<Topology>>, cfg: Rc<Config>) {
    let mut running: HashMap<Box<str>, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut seen_epoch = 0u64;
    loop {
        let t = topo.load_full();
        if t.epoch != seen_epoch {
            seen_epoch = t.epoch;
            let want: HashSet<&str> = t
                .masters
                .iter()
                .map(|&i| t.nodes[i as usize].addr.as_str())
                .collect();
            running.retain(|addr, task| {
                want.contains(&**addr) || {
                    task.abort();
                    false
                }
            });
            for &addr in &want {
                if !running.contains_key(addr) {
                    let task = tokio::task::spawn_local(run_tracker(
                        Box::from(addr),
                        cache.clone(),
                        cfg.clone(),
                    ));
                    running.insert(Box::from(addr), task);
                }
            }
            cache.set_masters(want.len());
        }
        tokio::time::sleep(TRACKER_POLL).await;
    }
}

async fn run_tracker(addr: Box<str>, cache: Rc<ReplyCache>, cfg: Rc<Config>) {
    loop {
        if let Err(e) = track_once(&addr, &cache, &cfg).await {
            log_debug!("tracker {addr}: {e}");
        }
        tokio::time::sleep(TRACKER_RETRY).await;
    }
}

// coverage accounting survives task aborts through this guard
struct UpGuard(Rc<ReplyCache>);

impl UpGuard {
    fn new(cache: &Rc<ReplyCache>) -> UpGuard {
        cache.tracker_up();
        UpGuard(cache.clone())
    }
}

impl Drop for UpGuard {
    fn drop(&mut self) {
        self.0.tracker_down();
    }
}

async fn track_once(addr: &str, cache: &Rc<ReplyCache>, cfg: &Config) -> Result<(), String> {
    let stream = backend::connect(addr, cfg.tcp_keepalive_secs)
        .await
        .map_err(|e| e.to_string())?;
    let (mut read_half, mut write_half) = stream.into_split();
    let mut setup: Vec<u8> = Vec::new();
    if cfg.backend_pass.is_empty() {
        resp::write_command(&mut setup, &[b"HELLO", b"3"]);
    } else {
        let user: &[u8] = if cfg.backend_user.is_empty() {
            b"default"
        } else {
            cfg.backend_user.as_bytes()
        };
        resp::write_command(
            &mut setup,
            &[b"HELLO", b"3", b"AUTH", user, cfg.backend_pass.as_bytes()],
        );
    }
    setup.extend_from_slice(TRACKING_FRAME);
    write_half
        .write_all(&setup)
        .await
        .map_err(|e| e.to_string())?;

    let mut buf = BytesMut::with_capacity(backend::READ_INIT);
    let mut expected = 2u32;
    while expected > 0 {
        match resp::scan_value(&buf) {
            resp::Scan::Complete(len) => {
                let frame = buf.split_to(len);
                if frame.first() == Some(&b'-') {
                    return Err(String::from_utf8_lossy(&frame).trim_end().to_string());
                }
                expected -= 1;
                continue;
            }
            resp::Scan::Invalid(e) => return Err(e.to_string()),
            resp::Scan::Incomplete => {}
        }
        let n = read_half
            .read_buf(&mut buf)
            .await
            .map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed during tracking handshake".to_string());
        }
    }

    let _up = UpGuard::new(cache);
    let mut ping = tokio::time::interval(TRACKER_PING);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut debt = 0u32;
    loop {
        loop {
            match resp::scan_value(&buf) {
                resp::Scan::Complete(len) => {
                    let frame = buf.split_to(len);
                    match frame.first() {
                        Some(b'>') => apply_push(&frame, cache),
                        Some(b'+') => debt = debt.saturating_sub(1),
                        Some(b'-') => return Err("tracking connection error reply".to_string()),
                        _ => {}
                    }
                }
                resp::Scan::Invalid(e) => return Err(e.to_string()),
                resp::Scan::Incomplete => break,
            }
        }
        backend::ensure_read_room(&mut buf);
        tokio::select! {
            _ = ping.tick() => {
                if debt >= PING_DEBT_MAX {
                    return Err("tracker silent past ping budget".to_string());
                }
                debt += 1;
                write_half
                    .write_all(PING_FRAME)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            r = read_half.read_buf(&mut buf) => {
                match r {
                    Ok(0) => return Err("tracking connection closed".to_string()),
                    Err(e) => return Err(e.to_string()),
                    Ok(_) => {}
                }
            }
        }
    }
}

// BCAST invalidation push: ["invalidate", [key, ...]] or a null for flushes
fn apply_push(frame: &[u8], cache: &ReplyCache) {
    let Some((_, mut pos)) = resp::scan_int_line(frame, 1) else {
        return;
    };
    match resp::scan_bulk(frame, pos) {
        Some(Ok(b))
            if frame[b.payload_start..b.payload_end].eq_ignore_ascii_case(b"invalidate") =>
        {
            pos = b.next;
        }
        _ => return,
    }
    match frame.get(pos) {
        Some(b'*') => {
            let Some((n, mut cur)) = resp::scan_int_line(frame, pos + 1) else {
                return;
            };
            if n < 0 {
                cache.clear();
                return;
            }
            for _ in 0..n {
                let Some(Ok(b)) = resp::scan_bulk(frame, cur) else {
                    return;
                };
                cache.invalidate(&frame[b.payload_start..b.payload_end]);
                cur = b.next;
            }
        }
        Some(b'_') => cache.clear(),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(max_bytes: usize) -> Rc<ReplyCache> {
        let cfg = Config {
            reply_cache_max_bytes: max_bytes,
            ..Config::default()
        };
        let c = ReplyCache::new(&cfg, Stats::new(1), 0);
        c.set_masters(1);
        c.tracker_up();
        c
    }

    fn fill(c: &ReplyCache, key: &str, val: &str) -> (Bytes, Bytes) {
        let key = Bytes::copy_from_slice(key.as_bytes());
        let frame = Bytes::from(format!("${}\r\n{val}\r\n", val.len()));
        assert!(c.begin_fill(&key));
        c.complete_fill(&key, &frame);
        (key, frame)
    }

    #[test]
    fn fills_hit_and_invalidate() {
        let c = cache(1 << 20);
        let (key, frame) = fill(&c, "k1", "v1");
        assert_eq!(c.lookup(&key), Some(frame));
        c.invalidate(&key);
        assert_eq!(c.lookup(&key), None);
    }

    #[test]
    fn racing_invalidation_poisons_the_fill() {
        let c = cache(1 << 20);
        let key = Bytes::from_static(b"k1");
        let frame = Bytes::from_static(b"$2\r\nv1\r\n");
        assert!(c.begin_fill(&key));
        c.invalidate(&key);
        c.complete_fill(&key, &frame);
        assert_eq!(c.lookup(&key), None);
    }

    #[test]
    fn clear_poisons_in_flight_fills() {
        let c = cache(1 << 20);
        let key = Bytes::from_static(b"k1");
        assert!(c.begin_fill(&key));
        c.clear();
        c.complete_fill(&key, &Bytes::from_static(b"$2\r\nv1\r\n"));
        assert_eq!(c.lookup(&key), None);
    }

    #[test]
    fn incomplete_coverage_blocks_fills_and_loss_clears() {
        let c = cache(1 << 20);
        let (key, frame) = fill(&c, "k1", "v1");
        c.set_masters(2);
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
        c.tracker_up();
        let (key, frame2) = fill(&c, "k1", "v2");
        assert_eq!(c.lookup(&key), Some(frame2));
        drop(frame);
        c.tracker_down();
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
    }

    #[test]
    fn generation_flip_bounds_bytes_and_promotes_hits() {
        let budget = 4 * (ENTRY_OVERHEAD + 16);
        let c = cache(budget);
        let (k1, f1) = fill(&c, "a", "1");
        let (k2, _) = fill(&c, "b", "2");
        fill(&c, "c", "3");
        assert_eq!(c.lookup(&k1), Some(f1.clone()), "promoted out of prev");
        fill(&c, "d", "4");
        assert_eq!(c.lookup(&k1), Some(f1), "promotion survived the flip");
        assert_eq!(c.lookup(&k2), None, "unpromoted entry aged out");
        assert!(c.hot_bytes.get() + c.prev_bytes.get() <= budget);
    }

    #[test]
    fn non_bulk_and_oversized_replies_stay_out() {
        let c = cache(1 << 20);
        let key = Bytes::from_static(b"k1");
        assert!(c.begin_fill(&key));
        c.complete_fill(&key, &Bytes::from_static(b"-ERR nope\r\n"));
        assert_eq!(c.lookup(&key), None);
        let big = format!(
            "${}\r\n{}\r\n",
            ENTRY_MAX_BYTES,
            "x".repeat(ENTRY_MAX_BYTES)
        );
        assert!(c.begin_fill(&key));
        c.complete_fill(&key, &Bytes::from(big));
        assert_eq!(c.lookup(&key), None);
    }

    #[test]
    fn parses_invalidation_pushes() {
        let c = cache(1 << 20);
        let (k1, _) = fill(&c, "k1", "v1");
        let (k2, f2) = fill(&c, "k2", "v2");
        apply_push(b">2\r\n$10\r\ninvalidate\r\n*1\r\n$2\r\nk1\r\n", &c);
        assert_eq!(c.lookup(&k1), None);
        assert_eq!(c.lookup(&k2), Some(f2));
        apply_push(b">2\r\n$10\r\ninvalidate\r\n_\r\n", &c);
        assert_eq!(c.lookup(&k2), None);
        let (k3, f3) = fill(&c, "k3", "v3");
        apply_push(b">2\r\n$10\r\ninvalidate\r\n*-1\r\n", &c);
        let _ = f3;
        assert_eq!(c.lookup(&k3), None);
    }
}

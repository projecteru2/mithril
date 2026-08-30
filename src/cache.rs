//! Reply cache: worker-local GET cache, invalidated by RESP3 BCAST tracking.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, hash_map};
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
// routing follows a new epoch per request; trackers follow within one poll
const TRACKER_POLL: Duration = Duration::from_millis(100);
const TRACKER_RETRY: Duration = Duration::from_secs(1);
const TRACKER_PING: Duration = Duration::from_secs(2);
// unanswered pings before a silent tracker is declared dead
const PING_DEBT_MAX: u32 = 3;
const PING_FRAME: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const TRACKING_FRAME: &[u8] =
    b"*4\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n$5\r\nBCAST\r\n";
const MIX: u64 = 0x9E37_79B9_7F4A_7C15;

type Map = HashMap<Box<[u8]>, Entry, BuildHasherDefault<KeyHasher>>;
type Fills = HashMap<Bytes, Fill, BuildHasherDefault<KeyHasher>>;

/// Word-at-a-time multiply-fold; the length binds in the tail, not a prefix.
#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    // hashbrown indexes buckets by the low bits: fold the mixed high half down
    fn finish(&self) -> u64 {
        let h = (self.0 ^ (self.0 >> 32)).wrapping_mul(MIX);
        h ^ (h >> 29)
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut h = self.0;
        let (words, rest) = bytes.as_chunks::<8>();
        for w in words {
            h = (h ^ u64::from_le_bytes(*w)).wrapping_mul(MIX);
        }
        let mut word = [0u8; 8];
        word[..rest.len()].copy_from_slice(rest);
        h = (h ^ u64::from_le_bytes(word)).wrapping_mul(MIX);
        self.0 = (h ^ bytes.len() as u64).wrapping_mul(MIX);
    }

    fn write_usize(&mut self, _: usize) {}
}

struct Entry {
    frame: Bytes,
    at: Instant,
}

// one key's in-flight state: at most one fill ticket, any number of writes
struct Fill {
    ticket: bool,
    writes: u32,
    poisoned: bool,
}

/// Worker-local reply cache; two generations flip under a byte budget.
pub struct ReplyCache {
    max_bytes: usize,
    max_age: Duration,
    hot: RefCell<Map>,
    prev: RefCell<Map>,
    hot_bytes: Cell<usize>,
    prev_bytes: Cell<usize>,
    fills: RefCell<Fills>,
    wanted: RefCell<HashSet<Box<str>>>,
    ready: RefCell<HashSet<Box<str>>>,
    armed: Cell<bool>,
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
            wanted: RefCell::new(HashSet::new()),
            ready: RefCell::new(HashSet::new()),
            armed: Cell::new(false),
            stats,
            worker,
        })
    }

    /// Returns the cached reply for `key` if fresh.
    pub fn lookup(&self, key: &[u8]) -> Option<Bytes> {
        if let Some(e) = self.hot.borrow().get(key) {
            if e.at.elapsed() <= self.max_age {
                return Some(e.frame.clone());
            }
            return None;
        }
        let (k, e) = take_entry(&self.prev, &self.prev_bytes, key)?;
        if e.at.elapsed() > self.max_age {
            return None;
        }
        let frame = e.frame.clone();
        self.insert_hot(k, e);
        Some(frame)
    }

    /// Arms a fill for `key`; false while coverage is incomplete or the key
    /// already has a ticket or a write in flight.
    pub fn begin_fill(&self, key: &Bytes) -> bool {
        if !self.armed.get() {
            return false;
        }
        match self.fills.borrow_mut().entry(key.clone()) {
            hash_map::Entry::Occupied(_) => false,
            hash_map::Entry::Vacant(v) => {
                v.insert(Fill {
                    ticket: true,
                    writes: 0,
                    poisoned: false,
                });
                true
            }
        }
    }

    /// Caches an armed fill's reply unless an invalidation raced it.
    pub fn complete_fill(&self, key: &Bytes, frame: &Bytes) {
        if self.settle_ticket(key) != Some(false)
            || frame.first() != Some(&b'$')
            || frame.len() > ENTRY_MAX_BYTES
            || entry_size(key, frame) > self.max_bytes / 2
        {
            return;
        }
        // the copy unpins the request read buffer the key slice points into
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
        self.settle_ticket(key);
    }

    /// Drops `key` from both generations and poisons its in-flight fill.
    pub fn invalidate(&self, key: &[u8]) {
        stats::bump(&self.stats.workers[self.worker].cache_invalidations);
        take_entry(&self.hot, &self.hot_bytes, key);
        take_entry(&self.prev, &self.prev_bytes, key);
        if let Some(f) = self.fills.borrow_mut().get_mut(key) {
            f.poisoned = true;
        }
    }

    /// Invalidates `key` and blocks fills until [`ReplyCache::end_write`]:
    /// a detached write may still be retrying while later reads complete.
    pub fn begin_write(&self, key: &Bytes) {
        self.invalidate(key);
        let mut fills = self.fills.borrow_mut();
        let f = fills.entry(key.clone()).or_insert(Fill {
            ticket: false,
            writes: 0,
            poisoned: true,
        });
        f.writes += 1;
        f.poisoned = true;
    }

    pub fn end_write(&self, key: &[u8]) {
        let mut fills = self.fills.borrow_mut();
        if let Some(f) = fills.get_mut(key) {
            f.writes = f.writes.saturating_sub(1);
            if f.writes == 0 && !f.ticket {
                fills.remove(key);
            }
        }
    }

    /// Empties both generations and poisons every in-flight fill.
    pub fn clear(&self) {
        *self.hot.borrow_mut() = Map::default();
        *self.prev.borrow_mut() = Map::default();
        self.hot_bytes.set(0);
        self.prev_bytes.set(0);
        for f in self.fills.borrow_mut().values_mut() {
            f.poisoned = true;
        }
    }

    // returns the ticket's poison state; the key's entry goes once nothing is in flight
    fn settle_ticket(&self, key: &[u8]) -> Option<bool> {
        let mut fills = self.fills.borrow_mut();
        let f = fills.get_mut(key)?;
        if !f.ticket {
            return None;
        }
        f.ticket = false;
        let poisoned = f.poisoned;
        if f.writes == 0 {
            fills.remove(key);
        }
        Some(poisoned)
    }

    fn insert_hot(&self, k: Box<[u8]>, e: Entry) {
        let size = entry_size(&k, &e.frame);
        if self.hot_bytes.get() + size > self.max_bytes / 2 {
            let flipped = std::mem::take(&mut *self.hot.borrow_mut());
            *self.prev.borrow_mut() = flipped;
            self.prev_bytes.set(self.hot_bytes.get());
            self.hot_bytes.set(0);
        }
        let mut hot = self.hot.borrow_mut();
        let klen = k.len();
        let replaced = match hot.insert(k, e) {
            Some(old) => klen + old.frame.len() + ENTRY_OVERHEAD,
            None => 0,
        };
        self.hot_bytes.set(self.hot_bytes.get() + size - replaced);
    }

    // any master-set change disarms immediately, before old trackers unwind
    fn set_coverage(&self, want: &HashSet<&str>) {
        let same = {
            let cur = self.wanted.borrow();
            cur.len() == want.len() && want.iter().all(|a| cur.contains(*a))
        };
        if !same {
            *self.wanted.borrow_mut() = want.iter().map(|&a| Box::from(a)).collect();
            self.clear();
        }
        self.recompute_armed();
    }

    fn tracker_up(&self, addr: &str) {
        self.ready.borrow_mut().insert(Box::from(addr));
        self.recompute_armed();
    }

    // a gone tracker means missed invalidations: nothing cached survives
    fn tracker_down(&self, addr: &str) {
        self.ready.borrow_mut().remove(addr);
        self.clear();
        self.recompute_armed();
    }

    fn recompute_armed(&self) {
        let wanted = self.wanted.borrow();
        let ready = self.ready.borrow();
        let armed = !wanted.is_empty() && wanted.iter().all(|a| ready.contains(a));
        self.armed.set(armed);
        self.stats.workers[self.worker]
            .cache_armed
            .store(u64::from(armed), Ordering::Relaxed);
    }
}

fn entry_size(key: &[u8], frame: &Bytes) -> usize {
    key.len() + frame.len() + ENTRY_OVERHEAD
}

fn take_entry(map: &RefCell<Map>, bytes: &Cell<usize>, key: &[u8]) -> Option<(Box<[u8]>, Entry)> {
    let (k, e) = map.borrow_mut().remove_entry(key)?;
    bytes.set(bytes.get() - entry_size(&k, &e.frame));
    Some((k, e))
}

/// Keeps one tracking connection per master alive; spawned once per worker.
pub async fn run_trackers(cache: Rc<ReplyCache>, topo: Arc<ArcSwap<Topology>>, cfg: Rc<Config>) {
    let mut running: HashMap<Box<str>, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut seen_epoch = 0u64;
    loop {
        if crate::server::topo_epoch() != seen_epoch {
            let t = topo.load_full();
            seen_epoch = t.epoch;
            let want: HashSet<&str> = t
                .masters
                .iter()
                .map(|&i| t.nodes[i as usize].addr.as_str())
                .collect();
            cache.set_coverage(&want);
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
struct UpGuard {
    cache: Rc<ReplyCache>,
    addr: Box<str>,
}

impl UpGuard {
    fn new(cache: &Rc<ReplyCache>, addr: &str) -> UpGuard {
        cache.tracker_up(addr);
        UpGuard {
            cache: cache.clone(),
            addr: Box::from(addr),
        }
    }
}

impl Drop for UpGuard {
    fn drop(&mut self) {
        self.cache.tracker_down(&self.addr);
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
    for _ in 0..2 {
        backend::read_reply(&mut read_half, &mut buf).await?;
    }

    let _up = UpGuard::new(cache, addr);
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
    use std::hash::BuildHasher;

    use super::*;

    fn cache(max_bytes: usize) -> Rc<ReplyCache> {
        let cfg = Config {
            reply_cache_max_bytes: max_bytes,
            ..Config::default()
        };
        let c = ReplyCache::new(&cfg, Stats::new(1), 0);
        c.set_coverage(&HashSet::from(["m1"]));
        c.tracker_up("m1");
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
    fn hasher_separates_lengths_and_agrees_across_key_types() {
        let h = BuildHasherDefault::<KeyHasher>::default();
        assert_ne!(h.hash_one(b"ab".as_slice()), h.hash_one(b"ab\0".as_slice()));
        assert_ne!(h.hash_one(b"".as_slice()), h.hash_one(b"\0".as_slice()));
        assert_eq!(
            h.hash_one(Bytes::from_static(b"key-1234567890")),
            h.hash_one(Box::<[u8]>::from(&b"key-1234567890"[..]))
        );
        let mut low = HashSet::new();
        for i in 0..4096u32 {
            low.insert(h.hash_one(format!("k{i}").as_bytes()) & 0xfff);
        }
        assert!(low.len() > 2300, "low bits collapse: {}", low.len());
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
        c.set_coverage(&HashSet::from(["m1", "m2"]));
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
        c.tracker_up("m2");
        let (key, frame2) = fill(&c, "k1", "v2");
        assert_eq!(c.lookup(&key), Some(frame2));
        drop(frame);
        c.tracker_down("m1");
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
    }

    #[test]
    fn replaced_master_disarms_before_old_tracker_unwinds() {
        let c = cache(1 << 20);
        let (key, _) = fill(&c, "k1", "v1");
        c.set_coverage(&HashSet::from(["m3"]));
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
        c.tracker_down("m1");
        c.tracker_up("m3");
        assert!(c.begin_fill(&key));
    }

    #[test]
    fn one_ticket_per_key_survives_a_write_between_misses() {
        let c = cache(1 << 20);
        let key = Bytes::from_static(b"k1");
        let stale = Bytes::from_static(b"$2\r\nv1\r\n");
        let fresh = Bytes::from_static(b"$2\r\nv2\r\n");
        assert!(c.begin_fill(&key));
        assert!(!c.begin_fill(&key), "second miss gets no ticket");
        c.invalidate(&key);
        c.complete_fill(&key, &stale);
        assert_eq!(c.lookup(&key), None);
        assert!(c.begin_fill(&key), "settled key accepts a new ticket");
        c.complete_fill(&key, &stale);
        assert_eq!(
            c.lookup(&key),
            Some(stale),
            "the untracked reply cannot poison it"
        );
        c.invalidate(&key);
        assert!(c.begin_fill(&key));
        c.complete_fill(&key, &fresh);
        assert_eq!(c.lookup(&key), Some(fresh));
    }

    #[test]
    fn pending_write_blocks_fills_until_it_ends() {
        let c = cache(1 << 20);
        let (key, _) = fill(&c, "k1", "v1");
        c.begin_write(&key);
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
        c.begin_write(&key);
        c.end_write(&key);
        assert!(!c.begin_fill(&key), "still one write in flight");
        c.end_write(&key);
        assert!(c.begin_fill(&key));
        c.complete_fill(&key, &Bytes::from_static(b"$2\r\nv2\r\n"));
        assert!(c.lookup(&key).is_some());
    }

    #[test]
    fn abandoned_ticket_frees_the_key() {
        let c = cache(1 << 20);
        let key = Bytes::from_static(b"k1");
        assert!(c.begin_fill(&key));
        c.abandon_fill(&key);
        assert!(c.fills.borrow().is_empty());
        assert!(c.begin_fill(&key));
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
    fn overwrite_debits_the_replaced_entry() {
        let c = cache(1 << 20);
        let (_, f1) = fill(&c, "k", "short");
        let before = c.hot_bytes.get();
        let (_, f2) = fill(&c, "k", "a much longer value");
        assert_eq!(c.hot_bytes.get(), before + f2.len() - f1.len());
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
        let huge_key = Bytes::from(vec![b'k'; (1 << 20) + 1]);
        assert!(c.begin_fill(&huge_key));
        c.complete_fill(&huge_key, &Bytes::from_static(b"$1\r\nv\r\n"));
        assert_eq!(c.lookup(&huge_key), None);
        assert_eq!(c.hot_bytes.get(), 0);
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

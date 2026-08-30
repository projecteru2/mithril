//! Reply cache: worker-local GET cache kept coherent by redirected RESP3
//! tracking; backend connections opt each cached read in.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, hash_map};
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::backend::{self, Backends};
use crate::config::Config;
use crate::log_debug;
use crate::resp;
use crate::shard::Fabric;
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
const CLIENT_ID_FRAME: &[u8] = b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n";
/// Marks the next command on a tracking connection as cached.
pub const CACHING_FRAME: &[u8] = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nCACHING\r\n$3\r\nYES\r\n";
const MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// Per-node `CLIENT TRACKING` frames, shared with the dialers on this worker.
pub type TrackingFrames = Rc<RefCell<HashMap<Box<str>, Bytes>>>;

type Map = HashMap<Box<[u8]>, Entry, BuildHasherDefault<KeyHasher>>;
type Fills = HashMap<Bytes, Fill, BuildHasherDefault<KeyHasher>>;
type Sets = (HashSet<Box<str>>, HashSet<Box<str>>);

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

/// Which masters are tracked: per worker, or process-wide under sharding.
enum Scope {
    Local {
        sets: RefCell<Sets>,
        armed: Cell<bool>,
    },
    Shared(Arc<Coverage>),
}

/// Process-wide tracking coverage; owner workers report, everyone reads.
pub struct Coverage {
    sets: Mutex<Sets>,
    armed: AtomicBool,
}

impl Coverage {
    pub fn new() -> Arc<Coverage> {
        Arc::new(Coverage {
            sets: Mutex::new((HashSet::new(), HashSet::new())),
            armed: AtomicBool::new(false),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Sets> {
        self.sets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
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
    scope: Scope,
    frames: TrackingFrames,
    stats: Arc<Stats>,
    worker: usize,
}

impl ReplyCache {
    pub fn new(
        cfg: &Config,
        stats: Arc<Stats>,
        worker: usize,
        coverage: Option<Arc<Coverage>>,
    ) -> Rc<ReplyCache> {
        let scope = match coverage {
            Some(c) => Scope::Shared(c),
            None => Scope::Local {
                sets: RefCell::new((HashSet::new(), HashSet::new())),
                armed: Cell::new(false),
            },
        };
        Rc::new(ReplyCache {
            max_bytes: cfg.reply_cache_max_bytes,
            max_age: Duration::from_secs(cfg.reply_cache_max_age_secs),
            hot: RefCell::new(Map::default()),
            prev: RefCell::new(Map::default()),
            hot_bytes: Cell::new(0),
            prev_bytes: Cell::new(0),
            fills: RefCell::new(Fills::default()),
            scope,
            frames: Rc::new(RefCell::new(HashMap::new())),
            stats,
            worker,
        })
    }

    /// The tracking frames the dialers on this worker attach to handshakes.
    pub fn tracking_frames(&self) -> TrackingFrames {
        self.frames.clone()
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
        if !self.armed() {
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

    fn armed(&self) -> bool {
        match &self.scope {
            Scope::Local { armed, .. } => armed.get(),
            Scope::Shared(c) => c.armed.load(Ordering::Relaxed),
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

    // any master-set change disarms immediately, before old trackers unwind;
    // true when the caller must flush every cache in the scope
    fn set_coverage(&self, want: &HashSet<&str>) -> bool {
        match &self.scope {
            Scope::Local { sets, armed } => {
                let mut sets = sets.borrow_mut();
                if replace_set(&mut sets.0, want) {
                    self.clear();
                }
                armed.set(covered(&sets));
                self.publish(armed.get());
                false
            }
            Scope::Shared(c) => {
                let mut sets = c.lock();
                let changed = replace_set(&mut sets.0, want);
                let armed = covered(&sets);
                c.armed.store(armed, Ordering::Relaxed);
                self.publish(armed);
                changed
            }
        }
    }

    fn tracker_up(&self, addr: &str, frame: Bytes) {
        self.frames.borrow_mut().insert(Box::from(addr), frame);
        match &self.scope {
            Scope::Local { sets, armed } => {
                let mut sets = sets.borrow_mut();
                sets.1.insert(Box::from(addr));
                armed.set(covered(&sets));
                self.publish(armed.get());
            }
            Scope::Shared(c) => {
                let mut sets = c.lock();
                sets.1.insert(Box::from(addr));
                let armed = covered(&sets);
                c.armed.store(armed, Ordering::Relaxed);
                self.publish(armed);
            }
        }
    }

    // a gone tracker means missed invalidations: nothing cached survives, and
    // a new connection must not redirect to the dead client id
    fn tracker_down(&self, addr: &str) {
        self.frames.borrow_mut().remove(addr);
        match &self.scope {
            Scope::Local { sets, armed } => {
                sets.borrow_mut().1.remove(addr);
                self.clear();
                armed.set(false);
            }
            Scope::Shared(c) => {
                c.lock().1.remove(addr);
                c.armed.store(false, Ordering::Relaxed);
            }
        }
        self.publish(false);
    }

    fn publish(&self, armed: bool) {
        let armed = u64::from(armed);
        match &self.scope {
            Scope::Local { .. } => self.stats.workers[self.worker]
                .cache_armed
                .store(armed, Ordering::Relaxed),
            Scope::Shared(_) => {
                for w in &self.stats.workers {
                    w.cache_armed.store(armed, Ordering::Relaxed);
                }
            }
        }
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

fn replace_set(cur: &mut HashSet<Box<str>>, want: &HashSet<&str>) -> bool {
    if cur.len() == want.len() && want.iter().all(|a| cur.contains(*a)) {
        return false;
    }
    *cur = want.iter().map(|&a| Box::from(a)).collect();
    true
}

fn covered((wanted, ready): &Sets) -> bool {
    !wanted.is_empty() && wanted.iter().all(|a| ready.contains(a))
}

/// Where a worker's trackers deliver what they receive.
pub struct Wiring {
    pub cache: Rc<ReplyCache>,
    pub backends: Rc<Backends>,
    pub fabric: Option<Arc<Fabric>>,
    pub cfg: Rc<Config>,
}

impl Wiring {
    // under sharding a node's pipe and its tracker both live on the owner
    fn owns(&self, addr: &str) -> bool {
        self.fabric
            .as_ref()
            .is_none_or(|f| f.owner(addr) == self.cache.worker)
    }

    async fn rearm(&self, addr: &str, frame: &Bytes) {
        match &self.fabric {
            Some(f) => f.rearm(addr, frame.clone()).await,
            None => self.backends.rearm(addr, frame).await,
        }
    }

    fn invalidate(&self, keys: &[Bytes]) {
        match &self.fabric {
            Some(f) => f.invalidate(Arc::from(keys)),
            None => {
                for k in keys {
                    self.cache.invalidate(k);
                }
            }
        }
    }

    fn flush(&self) {
        match &self.fabric {
            Some(f) => f.flush(),
            None => self.cache.clear(),
        }
    }
}

/// Keeps this worker's tracking connections alive; spawned once per worker.
pub async fn run_trackers(w: Rc<Wiring>, topo: Arc<ArcSwap<Topology>>) {
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
            if w.cache.set_coverage(&want) {
                w.flush();
            }
            running.retain(|addr, task| {
                want.contains(&**addr) || {
                    task.abort();
                    false
                }
            });
            for &addr in &want {
                if !running.contains_key(addr) && w.owns(addr) {
                    let task = tokio::task::spawn_local(run_tracker(Box::from(addr), w.clone()));
                    running.insert(Box::from(addr), task);
                }
            }
        }
        tokio::time::sleep(TRACKER_POLL).await;
    }
}

async fn run_tracker(addr: Box<str>, w: Rc<Wiring>) {
    loop {
        if let Err(e) = track_once(&addr, &w).await {
            log_debug!("tracker {addr}: {e}");
        }
        tokio::time::sleep(TRACKER_RETRY).await;
    }
}

// coverage accounting survives task aborts through this guard
struct UpGuard {
    w: Rc<Wiring>,
    addr: Box<str>,
}

impl Drop for UpGuard {
    fn drop(&mut self) {
        self.w.cache.tracker_down(&self.addr);
        self.w.flush();
    }
}

async fn track_once(addr: &str, w: &Rc<Wiring>) -> Result<(), String> {
    let cfg = &w.cfg;
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
    setup.extend_from_slice(CLIENT_ID_FRAME);
    write_half
        .write_all(&setup)
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = BytesMut::with_capacity(backend::READ_INIT);
    backend::read_reply(&mut read_half, &mut buf).await?;
    let id = backend::read_reply(&mut read_half, &mut buf).await?;
    let id = crate::multikey::parse_int(&id).ok_or("CLIENT ID: not an integer")?;
    let mut digits = [0u8; resp::DEC_BUF];
    let mut frame = Vec::new();
    resp::write_command(
        &mut frame,
        &[
            b"CLIENT",
            b"TRACKING",
            b"ON",
            b"REDIRECT",
            resp::u64_digits(&mut digits, id as u64),
            b"OPTIN",
        ],
    );
    let frame = Bytes::from(frame);
    // live connections learn the redirect before any fill can be armed
    w.rearm(addr, &frame).await;
    w.cache.tracker_up(addr, frame);
    let _up = UpGuard {
        w: w.clone(),
        addr: Box::from(addr),
    };

    let mut ping = tokio::time::interval(TRACKER_PING);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut debt = 0u32;
    let mut keys: Vec<Bytes> = Vec::new();
    loop {
        loop {
            match resp::scan_value(&buf) {
                resp::Scan::Complete(len) => {
                    let frame = buf.split_to(len).freeze();
                    match frame.first() {
                        Some(b'>') => match collect_push(&frame, &mut keys) {
                            Some(true) => w.flush(),
                            Some(false) => {
                                w.invalidate(&keys);
                                keys.clear();
                            }
                            None => {}
                        },
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

// invalidation push: ["invalidate", [key, ...]], or a null meaning flush
fn collect_push(frame: &Bytes, keys: &mut Vec<Bytes>) -> Option<bool> {
    let (_, mut pos) = resp::scan_int_line(frame, 1)?;
    match resp::scan_bulk(frame, pos) {
        Some(Ok(b))
            if frame[b.payload_start..b.payload_end].eq_ignore_ascii_case(b"invalidate") =>
        {
            pos = b.next;
        }
        _ => return None,
    }
    match frame.get(pos)? {
        b'*' => {
            let (n, mut cur) = resp::scan_int_line(frame, pos + 1)?;
            if n < 0 {
                return Some(true);
            }
            for _ in 0..n {
                let Some(Ok(b)) = resp::scan_bulk(frame, cur) else {
                    return None;
                };
                keys.push(frame.slice(b.payload_start..b.payload_end));
                cur = b.next;
            }
            Some(false)
        }
        b'_' => Some(true),
        _ => None,
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
        let c = ReplyCache::new(&cfg, Stats::new(1), 0, None);
        c.set_coverage(&HashSet::from(["m1"]));
        c.tracker_up("m1", Bytes::new());
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
        c.tracker_up("m2", Bytes::new());
        let (key, frame2) = fill(&c, "k1", "v2");
        assert_eq!(c.lookup(&key), Some(frame2));
        drop(frame);
        c.tracker_down("m1");
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
        assert!(c.frames.borrow().get("m1").is_none());
    }

    #[test]
    fn replaced_master_disarms_before_old_tracker_unwinds() {
        let c = cache(1 << 20);
        let (key, _) = fill(&c, "k1", "v1");
        c.set_coverage(&HashSet::from(["m3"]));
        assert_eq!(c.lookup(&key), None);
        assert!(!c.begin_fill(&key));
        c.tracker_down("m1");
        c.tracker_up("m3", Bytes::new());
        assert!(c.begin_fill(&key));
    }

    #[test]
    fn shared_scope_arms_process_wide_and_reports_flushes() {
        let cfg = Config::default();
        let stats = Stats::new(2);
        let cov = Coverage::new();
        let a = ReplyCache::new(&cfg, stats.clone(), 0, Some(cov.clone()));
        let b = ReplyCache::new(&cfg, stats.clone(), 1, Some(cov));
        assert!(a.set_coverage(&HashSet::from(["m1", "m2"])));
        assert!(
            !b.set_coverage(&HashSet::from(["m1", "m2"])),
            "same set is idempotent"
        );
        a.tracker_up("m1", Bytes::new());
        assert!(!b.begin_fill(&Bytes::from_static(b"k")));
        b.tracker_up("m2", Bytes::new());
        assert!(a.begin_fill(&Bytes::from_static(b"k")));
        assert_eq!(stats.workers[1].cache_armed.load(Ordering::Relaxed), 1);
        assert!(
            a.set_coverage(&HashSet::from(["m1"])),
            "a changed set asks for a flush"
        );
        a.tracker_down("m1");
        assert!(!b.begin_fill(&Bytes::from_static(b"k")));
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
        let mut keys = Vec::new();
        let two = Bytes::from_static(b">2\r\n$10\r\ninvalidate\r\n*2\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
        assert_eq!(collect_push(&two, &mut keys), Some(false));
        assert_eq!(keys, [Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]);
        keys.clear();
        let nil = Bytes::from_static(b">2\r\n$10\r\ninvalidate\r\n_\r\n");
        assert_eq!(collect_push(&nil, &mut keys), Some(true));
        let nil_array = Bytes::from_static(b">2\r\n$10\r\ninvalidate\r\n*-1\r\n");
        assert_eq!(collect_push(&nil_array, &mut keys), Some(true));
        let other = Bytes::from_static(b">2\r\n$7\r\nmessage\r\n$1\r\nx\r\n");
        assert_eq!(collect_push(&other, &mut keys), None);
        assert!(keys.is_empty());
    }
}

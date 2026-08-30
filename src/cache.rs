//! Reply cache: worker-local GET cache kept coherent by redirected RESP3
//! tracking; backend connections opt each cached read in.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, hash_map};
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::Range;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::{Buf, Bytes, BytesMut};
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
// map slot, two heap allocations with headers and size-class rounding
const ENTRY_OVERHEAD: usize = 96;
// trackers follow topology within one poll, which is also the cache clock's tick
const TRACKER_POLL: Duration = Duration::from_millis(100);
const TICKS_PER_SEC: u32 = 10;
// a generation this large is freed in chunks off the request path
const DROP_CHUNK: usize = 4096;
const TRACKER_RETRY: Duration = Duration::from_secs(1);
const TRACKER_PING: Duration = Duration::from_secs(2);
// a tracker this many pings behind is dead
const PING_DEBT_MAX: u32 = 3;
const PING_FRAME: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const CLIENT_ID_FRAME: &[u8] = b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n";
/// Marks the next command on a tracking connection as cached.
pub const CACHING_FRAME: &[u8] = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nCACHING\r\n$3\r\nYES\r\n";
const MIX: u64 = 0x9E37_79B9_7F4A_7C15;
// a larger invalidation push flushes the scope instead of pinning its frame
const INVAL_KEYS_MAX: i64 = 1024;
const INVAL_BYTES_MAX: usize = 64 * 1024;
// a push that cannot complete within this many buffered bytes restarts the tracker
const TRACKER_FRAME_MAX: usize = 1024 * 1024;
// the armed flag rides above the flush generation: one load is a consistent snapshot
const ARMED_BIT: u64 = 1 << 63;

/// Per-node `CLIENT TRACKING` frames, shared with the dialers on this worker.
pub type TrackingFrames = Rc<RefCell<HashMap<Box<str>, Bytes>>>;

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
    at: u32,
}

#[derive(Default)]
struct Gen {
    map: RefCell<Map>,
    bytes: Cell<usize>,
}

impl Gen {
    fn take(&self, key: &[u8]) -> Option<(Box<[u8]>, Entry)> {
        let mut map = self.map.borrow_mut();
        if map.is_empty() {
            return None;
        }
        let (k, e) = map.remove_entry(key)?;
        self.bytes
            .set(self.bytes.get() - entry_size(k.len(), e.frame.len()));
        Some((k, e))
    }

    fn reset(&self) {
        retire(std::mem::take(&mut *self.map.borrow_mut()));
        self.bytes.set(0);
    }
}

#[derive(Default)]
struct Sets {
    wanted: HashSet<Box<str>>,
    ready: HashSet<Box<str>>,
}

impl Sets {
    fn covered(&self) -> bool {
        !self.wanted.is_empty() && self.wanted.iter().all(|a| self.ready.contains(a))
    }
}

struct Fill {
    ticket: bool,
    writes: u32,
    poisoned: bool,
}

// what an invalidation push asks for; Keys carries the payload byte total
#[derive(Debug, PartialEq, Eq)]
enum Push {
    Flush,
    Keys(usize),
}

/// Which masters are tracked: per worker, or process-wide under sharding.
enum Scope {
    Local {
        sets: RefCell<Sets>,
        armed: Cell<bool>,
    },
    Shared(Arc<Coverage>),
}

/// Process-wide tracking coverage; a flush bumps the generation every worker syncs to.
pub struct Coverage {
    sets: Mutex<Sets>,
    state: AtomicU64,
}

impl Coverage {
    pub fn new() -> Arc<Coverage> {
        Arc::new(Coverage {
            sets: Mutex::new(Sets::default()),
            state: AtomicU64::new(0),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Sets> {
        self.sets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // callers hold the sets lock, which serializes every writer
    fn publish(&self, armed: bool, flush: bool) {
        let generation = (self.state.load(Ordering::Relaxed) & !ARMED_BIT) + u64::from(flush);
        let armed = if armed { ARMED_BIT } else { 0 };
        self.state.store(generation | armed, Ordering::Relaxed);
    }
}

/// Worker-local reply cache; two generations flip under a byte budget.
pub struct ReplyCache {
    max_bytes: usize,
    max_age: u32,
    clock: Cell<u32>,
    hot: Gen,
    prev: Gen,
    fills: RefCell<Fills>,
    scope: Scope,
    seen_flushes: Cell<u64>,
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
                sets: RefCell::new(Sets::default()),
                armed: Cell::new(false),
            },
        };
        Rc::new(ReplyCache {
            max_bytes: cfg.reply_cache_max_bytes,
            max_age: cfg.reply_cache_max_age_secs as u32 * TICKS_PER_SEC,
            clock: Cell::new(0),
            hot: Gen::default(),
            prev: Gen::default(),
            fills: RefCell::new(Fills::default()),
            scope,
            seen_flushes: Cell::new(0),
            frames: Rc::new(RefCell::new(HashMap::new())),
            stats,
            worker,
        })
    }

    /// The tracking frames the dialers on this worker attach to handshakes.
    pub fn tracking_frames(&self) -> TrackingFrames {
        self.frames.clone()
    }

    /// Advances the coarse clock entries age against.
    pub fn tick(&self, now: u32) {
        self.clock.set(now);
    }

    /// Returns the cached reply for `key` if fresh.
    pub fn lookup(&self, key: &[u8]) -> Option<Bytes> {
        self.sync();
        let now = self.clock.get();
        if let Some(e) = self.hot.map.borrow().get(key) {
            if now.wrapping_sub(e.at) <= self.max_age {
                return Some(e.frame.clone());
            }
            return None;
        }
        let (k, e) = self.prev.take(key)?;
        if now.wrapping_sub(e.at) > self.max_age {
            return None;
        }
        let frame = e.frame.clone();
        self.insert_hot(k, e);
        Some(frame)
    }

    /// Arms a fill for `key`; false when uncovered or the key is already in flight.
    pub fn begin_fill(&self, key: &Bytes) -> bool {
        if !self.sync() {
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
        self.sync();
        if !self.settle_ticket(key)
            || frame.first() != Some(&b'$')
            || frame.len() > ENTRY_MAX_BYTES
            || entry_size(key.len(), frame.len()) > self.max_bytes / 2
        {
            return;
        }
        // both copies unpin the socket read chunks the slices point into
        self.insert_hot(
            Box::from(&key[..]),
            Entry {
                frame: Bytes::copy_from_slice(frame),
                at: self.clock.get(),
            },
        );
    }

    /// Forgets a fill whose reply will never be observed.
    pub fn abandon_fill(&self, key: &Bytes) {
        self.settle_ticket(key);
    }

    /// Drops `key` from both generations and poisons its in-flight fill.
    pub fn invalidate(&self, key: &[u8]) {
        self.evict(key);
        if let Some(f) = self.fills.borrow_mut().get_mut(key) {
            f.poisoned = true;
        }
    }

    /// Invalidates `key` and blocks fills until [`ReplyCache::end_write`].
    pub fn begin_write(&self, key: &Bytes) {
        self.evict(key);
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
        self.hot.reset();
        self.prev.reset();
        for f in self.fills.borrow_mut().values_mut() {
            f.poisoned = true;
        }
    }

    /// Clears every cache in the scope.
    pub fn flush(&self) {
        self.coverage(|_| true);
    }

    // catches up with the scope's flush generation; returns whether fills may arm
    fn sync(&self) -> bool {
        match &self.scope {
            Scope::Local { armed, .. } => armed.get(),
            Scope::Shared(c) => {
                let state = c.state.load(Ordering::Relaxed);
                let generation = state & !ARMED_BIT;
                if generation != self.seen_flushes.get() {
                    self.seen_flushes.set(generation);
                    self.clear();
                }
                state & ARMED_BIT != 0
            }
        }
    }

    fn evict(&self, key: &[u8]) {
        self.sync();
        stats::bump(&self.stats.workers[self.worker].cache_invalidations);
        self.hot.take(key);
        self.prev.take(key);
    }

    // true when the key's ticket settled unpoisoned; the entry goes once nothing is in flight
    fn settle_ticket(&self, key: &[u8]) -> bool {
        let mut fills = self.fills.borrow_mut();
        let Some(f) = fills.get_mut(key).filter(|f| f.ticket) else {
            return false;
        };
        f.ticket = false;
        let clean = !f.poisoned;
        if f.writes == 0 {
            fills.remove(key);
        }
        clean
    }

    fn insert_hot(&self, k: Box<[u8]>, e: Entry) {
        let klen = k.len();
        let size = entry_size(klen, e.frame.len());
        if self.hot.bytes.get() + size > self.max_bytes / 2 {
            let flipped = std::mem::take(&mut *self.hot.map.borrow_mut());
            retire(std::mem::replace(&mut *self.prev.map.borrow_mut(), flipped));
            self.prev.bytes.set(self.hot.bytes.get());
            self.hot.bytes.set(0);
        }
        let replaced = match self.hot.map.borrow_mut().insert(k, e) {
            Some(old) => entry_size(klen, old.frame.len()),
            None => 0,
        };
        self.hot.bytes.set(self.hot.bytes.get() + size - replaced);
    }

    // applies one coverage change; `f` says whether the scope must flush
    fn coverage(&self, f: impl FnOnce(&mut Sets) -> bool) {
        let armed = match &self.scope {
            Scope::Local { sets, armed } => {
                let mut sets = sets.borrow_mut();
                if f(&mut sets) {
                    self.clear();
                }
                armed.set(sets.covered());
                armed.get()
            }
            Scope::Shared(c) => {
                let mut sets = c.lock();
                let flush = f(&mut sets);
                let armed = sets.covered();
                c.publish(armed, flush);
                drop(sets);
                self.sync();
                armed
            }
        };
        self.publish_stat(armed);
    }

    // any master-set change disarms immediately, before old trackers unwind
    fn set_coverage(&self, want: &HashSet<&str>) {
        self.coverage(|s| replace_set(&mut s.wanted, want));
    }

    // a dialer that races the rearm picks the frame up from here
    fn install_frame(&self, addr: &str, frame: Bytes) {
        self.frames.borrow_mut().insert(Box::from(addr), frame);
    }

    fn tracker_up(&self, addr: &str) {
        self.coverage(|s| {
            s.ready.insert(Box::from(addr));
            false
        });
    }

    // missed invalidations: nothing cached survives, and no dial may redirect at the dead id
    fn tracker_down(&self, addr: &str) {
        self.frames.borrow_mut().remove(addr);
        self.coverage(|s| {
            s.ready.remove(addr);
            true
        });
    }

    fn publish_stat(&self, armed: bool) {
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

    async fn rearm(&self, addr: &str, frame: &Bytes) -> Result<(), String> {
        match &self.fabric {
            Some(f) => f.rearm(addr, frame.clone()).await,
            None => self.backends.rearm(addr, frame).await,
        }
    }

    // compacted so the batch never pins the read buffer; a worker that cannot take it flushes
    fn invalidate(&self, frame: &[u8], keys: &[Range<usize>], bytes: usize) {
        match &self.fabric {
            Some(f) => {
                let mut compact = BytesMut::with_capacity(bytes);
                for k in keys {
                    compact.extend_from_slice(&frame[k.start..k.end]);
                }
                let compact = compact.freeze();
                let mut at = 0;
                let batch: Arc<[Bytes]> = keys
                    .iter()
                    .map(|k| {
                        let s = compact.slice(at..at + k.len());
                        at += k.len();
                        s
                    })
                    .collect();
                if !f.invalidate(batch) {
                    self.cache.flush();
                }
            }
            None => {
                for k in keys {
                    self.cache.invalidate(&frame[k.start..k.end]);
                }
            }
        }
    }
}

// coverage accounting survives task aborts
struct UpGuard {
    w: Rc<Wiring>,
    addr: Box<str>,
}

impl Drop for UpGuard {
    fn drop(&mut self) {
        self.w.cache.tracker_down(&self.addr);
    }
}

fn entry_size(key_len: usize, frame_len: usize) -> usize {
    key_len + frame_len + ENTRY_OVERHEAD
}

// frees a large generation a chunk at a time so no request waits on the whole drop
fn retire(mut map: Map) {
    if map.len() <= DROP_CHUNK || tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    tokio::task::spawn_local(async move {
        let mut drain = map.drain();
        while drain.by_ref().take(DROP_CHUNK).count() == DROP_CHUNK {
            tokio::task::yield_now().await;
        }
    });
}

fn replace_set(cur: &mut HashSet<Box<str>>, want: &HashSet<&str>) -> bool {
    if cur.len() == want.len() && want.iter().all(|a| cur.contains(*a)) {
        return false;
    }
    *cur = want.iter().map(|&a| Box::from(a)).collect();
    true
}

/// Keeps this worker's tracking connections alive; spawned once per worker.
pub async fn run_trackers(w: Rc<Wiring>, topo: Arc<ArcSwap<Topology>>) {
    let mut running: HashMap<Box<str>, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut seen_epoch = 0u64;
    let started = Instant::now();
    loop {
        w.cache
            .tick((started.elapsed().as_millis() / TRACKER_POLL.as_millis()) as u32);
        if crate::server::topo_epoch() != seen_epoch {
            let t = topo.load_full();
            seen_epoch = t.epoch;
            let want: HashSet<&str> = t
                .masters
                .iter()
                .map(|&i| t.nodes[i as usize].addr.as_str())
                .collect();
            w.cache.set_coverage(&want);
            running.retain(|addr, task| {
                let keep = want.contains(&**addr);
                if !keep {
                    task.abort();
                }
                keep
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

async fn track_once(addr: &str, w: &Rc<Wiring>) -> Result<(), String> {
    let cfg = &w.cfg;
    let stream = backend::connect(addr, cfg.tcp_keepalive_secs)
        .await
        .map_err(|e| e.to_string())?;
    let (mut read_half, mut write_half) = stream.into_split();
    {
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
    }
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
    // frame first, so a dial racing the rearm redirects too; the guard forgets it on any exit
    w.cache.install_frame(addr, frame.clone());
    let _up = UpGuard {
        w: w.clone(),
        addr: Box::from(addr),
    };
    w.rearm(addr, &frame).await?;
    w.cache.tracker_up(addr);

    let mut ping = tokio::time::interval(TRACKER_PING);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut debt = 0u32;
    let mut keys: Vec<Range<usize>> = Vec::new();
    let mut cur = resp::Cursor::default();
    loop {
        loop {
            match resp::scan_value_at(&buf, &mut cur) {
                resp::Scan::Complete(len) => {
                    let frame = &buf[..len];
                    match frame.first() {
                        Some(b'>') => match collect_push(frame, &mut keys) {
                            Some(Push::Flush) => w.cache.flush(),
                            Some(Push::Keys(bytes)) => {
                                w.invalidate(frame, &keys, bytes);
                                keys.clear();
                            }
                            None => {}
                        },
                        Some(b'+') => debt = debt.saturating_sub(1),
                        Some(b'-') => return Err("tracking connection error reply".to_string()),
                        _ => {}
                    }
                    buf.advance(len);
                }
                resp::Scan::Invalid(e) => return Err(e.to_string()),
                resp::Scan::Incomplete => break,
            }
        }
        if buf.len() > TRACKER_FRAME_MAX {
            return Err("tracking push exceeds the frame bound".to_string());
        }
        if buf.is_empty() && buf.capacity() > backend::READ_CHUNK {
            buf = BytesMut::with_capacity(backend::READ_INIT);
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
fn collect_push(frame: &[u8], keys: &mut Vec<Range<usize>>) -> Option<Push> {
    let (_, pos) = resp::scan_int_line(frame, 1)?;
    let Some(Ok(b)) = resp::scan_bulk(frame, pos) else {
        return None;
    };
    if !frame[b.payload_start..b.payload_end].eq_ignore_ascii_case(b"invalidate") {
        return None;
    }
    let pos = b.next;
    match frame.get(pos)? {
        b'*' => {
            let (n, mut cur) = resp::scan_int_line(frame, pos + 1)?;
            if !(0..=INVAL_KEYS_MAX).contains(&n) {
                return Some(Push::Flush);
            }
            let mut bytes = 0;
            for _ in 0..n {
                let Some(Ok(b)) = resp::scan_bulk(frame, cur) else {
                    keys.clear();
                    return None;
                };
                bytes += b.payload_end - b.payload_start;
                if bytes > INVAL_BYTES_MAX {
                    keys.clear();
                    return Some(Push::Flush);
                }
                keys.push(b.payload_start..b.payload_end);
                cur = b.next;
            }
            Some(Push::Keys(bytes))
        }
        b'_' => Some(Push::Flush),
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
        c.install_frame("m1", Bytes::new());
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
        c.tracker_up("m3");
        assert!(c.begin_fill(&key));
    }

    #[test]
    fn departed_master_leaves_the_rest_covered() {
        let c = cache(1 << 20);
        c.set_coverage(&HashSet::from(["m1", "m2"]));
        c.tracker_up("m2");
        assert!(c.begin_fill(&Bytes::from_static(b"a")));
        c.set_coverage(&HashSet::from(["m2"]));
        c.tracker_down("m1");
        assert!(
            c.begin_fill(&Bytes::from_static(b"b")),
            "m2 alone covers the set"
        );
    }

    #[test]
    fn shared_scope_arms_process_wide_and_flushes_by_generation() {
        let cfg = Config::default();
        let stats = Stats::new(2);
        let cov = Coverage::new();
        let a = ReplyCache::new(&cfg, stats.clone(), 0, Some(cov.clone()));
        let b = ReplyCache::new(&cfg, stats.clone(), 1, Some(cov.clone()));
        a.set_coverage(&HashSet::from(["m1", "m2"]));
        b.set_coverage(&HashSet::from(["m1", "m2"]));
        a.tracker_up("m1");
        assert!(!b.begin_fill(&Bytes::from_static(b"k")));
        b.tracker_up("m2");
        let (key, frame) = fill(&b, "k", "v");
        assert_eq!(stats.workers[0].cache_armed.load(Ordering::Relaxed), 1);
        let pending = Bytes::from_static(b"p");
        assert!(b.begin_fill(&pending));
        let before = cov.state.load(Ordering::Relaxed);
        a.tracker_down("m1");
        let after = cov.state.load(Ordering::Relaxed);
        assert_eq!(after & !ARMED_BIT, (before & !ARMED_BIT) + 1);
        assert_eq!(after & ARMED_BIT, 0);
        assert_eq!(b.lookup(&key), None, "b clears itself on its next touch");
        b.complete_fill(&pending, &Bytes::from_static(b"$1\r\nx\r\n"));
        a.tracker_up("m1");
        assert_eq!(
            b.lookup(&pending),
            None,
            "a fill from the old generation is gone"
        );
        assert!(b.begin_fill(&key));
        drop(frame);
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
        assert!(c.hot.bytes.get() + c.prev.bytes.get() <= budget);
    }

    #[test]
    fn overwrite_debits_the_replaced_entry() {
        let c = cache(1 << 20);
        let (_, f1) = fill(&c, "k", "short");
        let before = c.hot.bytes.get();
        let (_, f2) = fill(&c, "k", "a much longer value");
        assert_eq!(c.hot.bytes.get(), before + f2.len() - f1.len());
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
        assert_eq!(c.hot.bytes.get(), 0);
    }

    #[test]
    fn parses_invalidation_pushes() {
        let mut keys = Vec::new();
        let two = b">2\r\n$10\r\ninvalidate\r\n*2\r\n$2\r\nk1\r\n$2\r\nk2\r\n";
        assert_eq!(collect_push(two, &mut keys), Some(Push::Keys(4)));
        let got: Vec<&[u8]> = keys.iter().map(|r| &two[r.clone()]).collect();
        assert_eq!(got, [b"k1".as_slice(), b"k2".as_slice()]);
        keys.clear();
        assert_eq!(
            collect_push(b">2\r\n$10\r\ninvalidate\r\n_\r\n", &mut keys),
            Some(Push::Flush)
        );
        assert_eq!(
            collect_push(b">2\r\n$10\r\ninvalidate\r\n*-1\r\n", &mut keys),
            Some(Push::Flush)
        );
        assert_eq!(
            collect_push(b">2\r\n$7\r\nmessage\r\n$1\r\nx\r\n", &mut keys),
            None
        );
        assert!(keys.is_empty());
        let mut huge =
            format!(">2\r\n$10\r\ninvalidate\r\n*{}\r\n", INVAL_KEYS_MAX + 1).into_bytes();
        for _ in 0..=INVAL_KEYS_MAX {
            huge.extend_from_slice(b"$1\r\nk\r\n");
        }
        assert_eq!(collect_push(&huge, &mut keys), Some(Push::Flush));
        assert!(keys.is_empty());
        let wide_key = "k".repeat(INVAL_BYTES_MAX / 2 + 1);
        let wide = format!(
            ">2\r\n$10\r\ninvalidate\r\n*2\r\n${0}\r\n{1}\r\n${0}\r\n{1}\r\n",
            wide_key.len(),
            wide_key
        );
        assert_eq!(collect_push(wide.as_bytes(), &mut keys), Some(Push::Flush));
        assert!(keys.is_empty());
    }

    #[test]
    fn entries_age_against_the_coarse_clock() {
        let c = cache(1 << 20);
        let (key, frame) = fill(&c, "k1", "v1");
        c.tick(c.max_age);
        assert_eq!(c.lookup(&key), Some(frame));
        c.tick(c.max_age + 1);
        assert_eq!(c.lookup(&key), None);
    }
}

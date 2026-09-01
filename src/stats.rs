//! Per-worker counters, cacheline-padded, summed on demand for INFO.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Ticks per second in /proc stat fields (Linux USER_HZ).
pub const USER_HZ: u64 = 100;

/// One worker's counters; padding keeps writers on distinct cachelines.
#[repr(align(64))]
#[derive(Default)]
pub struct WorkerStats {
    pub commands: AtomicU64,
    pub errors: AtomicU64,
    pub redirects: AtomicU64,
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
}

/// What CLIENT LIST reports about one connection; touched only on connect,
/// disconnect and SETNAME.
pub struct ClientInfo {
    pub addr: SocketAddr,
    pub fd: i32,
    pub name: Box<str>,
    pub since: Instant,
}

/// Process-wide stats shared across workers.
pub struct Stats {
    pub workers: Vec<Arc<WorkerStats>>,
    pub clients: AtomicUsize,
    pub total_connections: AtomicU64,
    pub registry: Mutex<HashMap<u64, ClientInfo>>,
}

impl Stats {
    pub fn new(workers: usize) -> Arc<Stats> {
        Arc::new(Stats {
            workers: (0..workers).map(|_| Arc::default()).collect(),
            clients: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            registry: Mutex::new(HashMap::new()),
        })
    }

    pub fn registry(&self) -> std::sync::MutexGuard<'_, HashMap<u64, ClientInfo>> {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn sum<F: Fn(&WorkerStats) -> &AtomicU64>(&self, field: F) -> u64 {
        self.workers
            .iter()
            .map(|w| field(w).load(Ordering::Relaxed))
            .sum()
    }
}

/// Bumps a single-writer counter: plain load/store beats an atomic RMW on the hot path.
pub fn bump(counter: &AtomicU64) {
    counter.store(counter.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
}

/// Adds to a single-writer counter; cross-thread readers use relaxed loads.
pub fn add(counter: &AtomicU64, n: u64) {
    counter.store(counter.load(Ordering::Relaxed) + n, Ordering::Relaxed);
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

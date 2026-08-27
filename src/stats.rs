//! Per-worker counters, cacheline-padded, summed on demand for INFO.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
}

/// Process-wide stats shared across workers.
pub struct Stats {
    pub workers: Vec<WorkerStats>,
    pub clients: AtomicUsize,
    pub total_connections: AtomicU64,
}

impl Stats {
    pub fn new(workers: usize) -> Arc<Stats> {
        Arc::new(Stats {
            workers: (0..workers).map(|_| WorkerStats::default()).collect(),
            clients: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
        })
    }

    pub fn sum<F: Fn(&WorkerStats) -> &AtomicU64>(&self, field: F) -> u64 {
        self.workers
            .iter()
            .map(|w| field(w).load(Ordering::Relaxed))
            .sum()
    }
}

/// Bumps a counter with relaxed ordering.
pub fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Adds to a counter with relaxed ordering.
pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

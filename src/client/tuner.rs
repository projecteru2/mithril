//! Pipe selection under auto sharding: the session score and the worker-level tuner.

use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::Shared;
use super::session::Session;
use crate::stats::{self, WorkerStats};

// pipelining score: a local session shares at 0, a shared one returns at PIPELINED_LOCAL
pub(super) const PIPELINED_LOCAL: u8 = 4;
const PIPELINED_MAX: u8 = 8;
// what triggers a probe: the worker is busy and its local backend batches would
// stay thin (in-flight commands per master)
const TUNE_PERIOD: Duration = Duration::from_millis(100);
const BUSY_ENTER: u32 = 85;
const BUSY_LEAVE: u32 = 60;
const DEPTH_ENTER: u64 = 8;
const DEPTH_LEAVE: u64 = 16;
// ticks the enter or leave condition must hold: moving sessions off a worker
// lowers its own busyness, so leaving is slow and entering prompt
const ENTER_TICKS: u32 = 3;
const LEAVE_TICKS: u32 = 30;
// the command-rate window, ten ticks of 100 ms, so a rate is commands per second
const RATE_TICKS: usize = 10;
// ticks the probe gives the sessions to move before it measures
const SETTLE_TICKS: u32 = 10;
const PROBE_TICKS: u32 = SETTLE_TICKS + RATE_TICKS as u32;
// the shared pipes are kept only where they measure this much faster
const KEEP_GAIN_PCT: u64 = 5;
// commands per second under which the worker is idle and its rates say nothing
const MIN_RATE_PER_SEC: u64 = 100;
// ticks to the next probe, doubling while decisions confirm the current state
const PROBE_BACKOFF_TICKS: u32 = 300;
const PROBE_BACKOFF_MAX_TICKS: u32 = 4800;
const PROBE_DOUBLINGS: u32 = (PROBE_BACKOFF_MAX_TICKS / PROBE_BACKOFF_TICKS).ilog2();

impl Session {
    // an unpipelined session gains from the deeper batches of the shared pipe, a
    // pipelined one from its worker-local connection; switch only while nothing is in flight
    pub(super) fn adapt_pipes(&self) {
        let depth = self.outstanding();
        let score = self.pipelined.get();
        self.pipelined.set(pipelining_score(score, depth));
        let sharded = self.link.sharded.get();
        let prefer_shared = self.shared.prefer_shared.get();
        self.switch_pending
            .set(prefer_shared && !sharded && depth > 0);
        if depth == 0 && switch_pipes(sharded, score, prefer_shared) && !self.fanouts_pending() {
            self.link.sharded.set(!sharded);
            self.conns.borrow_mut().by_node.clear();
        }
    }
}

#[derive(Clone, Copy, Default)]
enum Phase {
    #[default]
    Steady,
    Probing {
        baseline: u64,
        ticks: u32,
    },
}

// the worker's experiment: the busy-and-thin rule triggers it, the measured command rate decides
#[derive(Default)]
struct Probe {
    prefer: bool,
    probes: u64,
    keeps: u64,
    reverts: u64,
    phase: Phase,
    streak: u32,
    wait: u32,
    confirmed: u32,
    ring: [u64; RATE_TICKS],
    at: usize,
    seen: usize,
    last: u64,
}

impl Probe {
    fn tick(&mut self, busy_pct: u32, depth: u64, commands_now: u64) -> bool {
        self.record(commands_now);
        self.wait = self.wait.saturating_sub(1);
        match self.phase {
            Phase::Probing { baseline, ticks } => self.probing(baseline, ticks),
            Phase::Steady if self.prefer => self.leave(busy_pct, depth),
            Phase::Steady => self.enter(busy_pct, depth),
        }
        self.prefer
    }

    fn publish(&self, wstats: &WorkerStats) {
        wstats
            .pipe_shared
            .store(u64::from(self.prefer), Ordering::Relaxed);
        wstats.pipe_probes.store(self.probes, Ordering::Relaxed);
        wstats.pipe_keeps.store(self.keeps, Ordering::Relaxed);
        wstats.pipe_reverts.store(self.reverts, Ordering::Relaxed);
    }

    fn record(&mut self, commands_now: u64) {
        self.ring[self.at] = commands_now.saturating_sub(self.last);
        self.at = (self.at + 1) % RATE_TICKS;
        self.last = commands_now;
        self.seen = (self.seen + 1).min(RATE_TICKS);
    }

    fn rate(&self) -> u64 {
        self.ring.iter().sum()
    }

    fn enter(&mut self, busy_pct: u32, depth: u64) {
        if busy_pct < BUSY_ENTER || depth >= DEPTH_ENTER {
            self.streak = 0;
            return;
        }
        self.streak = (self.streak + 1).min(ENTER_TICKS);
        if self.streak == ENTER_TICKS {
            self.start_probe();
        }
    }

    fn leave(&mut self, busy_pct: u32, depth: u64) {
        if busy_pct > BUSY_LEAVE && depth < DEPTH_LEAVE {
            self.streak = 0;
            self.start_probe();
            return;
        }
        self.streak += 1;
        if self.streak >= LEAVE_TICKS {
            // the load went away rather than a measurement: the next busy stretch probes at once
            self.streak = 0;
            self.wait = 0;
            self.prefer = false;
        }
    }

    fn start_probe(&mut self) {
        let baseline = self.rate();
        if self.wait > 0 || self.seen < RATE_TICKS || baseline < MIN_RATE_PER_SEC {
            return;
        }
        self.probes += 1;
        self.streak = 0;
        self.prefer = !self.prefer;
        self.phase = Phase::Probing { baseline, ticks: 0 };
    }

    fn probing(&mut self, baseline: u64, ticks: u32) {
        let ticks = ticks + 1;
        if ticks < PROBE_TICKS {
            self.phase = Phase::Probing { baseline, ticks };
            return;
        }
        let measured = self.rate();
        let (shared, local) = if self.prefer {
            (measured, baseline)
        } else {
            (baseline, measured)
        };
        let keep = shared * 100 >= local * (100 + KEEP_GAIN_PCT);
        // a decision that flips the state starts the schedule over, one that confirms it waits longer
        if keep == self.prefer {
            self.confirmed = 0;
            self.wait = PROBE_BACKOFF_TICKS;
        } else {
            self.wait = PROBE_BACKOFF_TICKS << self.confirmed;
            self.confirmed = (self.confirmed + 1).min(PROBE_DOUBLINGS);
        }
        if keep {
            self.keeps += 1;
        } else {
            self.reverts += 1;
        }
        self.prefer = keep;
        self.phase = Phase::Steady;
    }
}

/// Samples this worker's CPU busyness, batch depth and command rate, and publishes
/// whether its sessions should prefer the shared pipes.
pub async fn auto_tuner(shared: Rc<Shared>) {
    let mut last_ticks = stats::thread_cpu_ticks();
    let mut last_at = Instant::now();
    let mut busy_x16 = 0u32;
    let mut probe = Probe::default();
    loop {
        tokio::time::sleep(TUNE_PERIOD).await;
        let now = Instant::now();
        let ticks = stats::thread_cpu_ticks();
        if let (Some(a), Some(b)) = (last_ticks, ticks) {
            let wall_ms = now.duration_since(last_at).as_millis().max(1) as u64;
            let sample =
                ((b.saturating_sub(a)) * 1000 * 100 / (stats::USER_HZ * wall_ms)).min(100) as u32;
            busy_x16 = busy_ewma(busy_x16, sample);
        }
        last_ticks = ticks;
        last_at = now;
        let masters = shared.topo.load().masters.len().max(1) as u64;
        let (measured, writes) = shared.backends.batch_depth();
        let depth = tune_depth(measured, writes, shared.inflight.get(), masters);
        let commands = shared.wstats.commands.load(Ordering::Relaxed);
        shared
            .prefer_shared
            .set(probe.tick((busy_x16 + 8) / 16, depth, commands));
        probe.publish(&shared.wstats);
    }
}

// decided on an idle dispatch from the score before that dispatch counts
fn switch_pipes(sharded: bool, score: u8, worker_prefers_shared: bool) -> bool {
    if worker_prefers_shared {
        return !sharded;
    }
    if sharded {
        score >= PIPELINED_LOCAL
    } else {
        score <= 1
    }
}

// the measured local batch while local traffic flows; with none, the in-flight
// commands spread over the masters — an estimate that errs toward staying on the
// shared pipes, which at saturation batch at least as well as local connections
fn tune_depth(measured: u32, writes: u32, inflight: u64, masters: u64) -> u64 {
    if writes > 0 {
        u64::from(measured)
    } else {
        inflight / masters
    }
}

fn busy_ewma(busy_x16: u32, sample: u32) -> u32 {
    (busy_x16 * 3 + sample * 16) / 4
}

fn pipelining_score(score: u8, depth: u64) -> u8 {
    if depth > 0 {
        (score + 1).min(PIPELINED_MAX)
    } else {
        score.saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipe_switch_needs_four_idle_dispatches_to_share_and_four_busy_to_return() {
        let mut score = PIPELINED_LOCAL;
        for _ in 0..3 {
            assert!(!switch_pipes(false, score, false));
            score = pipelining_score(score, 0);
        }
        assert!(switch_pipes(false, score, false));
        score = pipelining_score(score, 0);
        assert_eq!(score, 0);
        for _ in 0..3 {
            score = pipelining_score(score, 3);
            assert!(!switch_pipes(true, score, false));
        }
        score = pipelining_score(score, 3);
        assert!(switch_pipes(true, score, false));
        assert!(!switch_pipes(true, pipelining_score(score, 0), false) || score > PIPELINED_LOCAL);
        assert_eq!(pipelining_score(PIPELINED_MAX, 9), PIPELINED_MAX);
        assert_eq!(pipelining_score(0, 0), 0);
    }

    #[test]
    fn worker_preference_overrides_the_score() {
        assert!(switch_pipes(false, PIPELINED_MAX, true));
        assert!(!switch_pipes(true, PIPELINED_MAX, true));
        let mut b = 0;
        for _ in 0..64 {
            b = busy_ewma(b, BUSY_ENTER);
        }
        assert_eq!((b + 8) / 16, BUSY_ENTER);
        assert_eq!(tune_depth(3, 10, 900, 128), 3);
        assert_eq!(tune_depth(3, 0, 900, 128), 7);
    }

    #[test]
    fn a_probe_that_gains_keeps_the_shared_pipes() {
        let mut rig = Rig::default();
        assert!(!rig.run(RATE_TICKS as u32 - 1, 1_000, 90, 1));
        assert!(rig.run(1, 1_000, 90, 1));
        assert_eq!(rig.probe.probes, 1);
        rig.run(SETTLE_TICKS, 1_000, 90, 1);
        assert!(rig.run(RATE_TICKS as u32, 1_200, 90, 1));
        assert_eq!((rig.probe.keeps, rig.probe.reverts), (1, 0));
        assert_eq!(rig.probe.wait, PROBE_BACKOFF_TICKS);
    }

    #[test]
    fn a_probe_that_loses_reverts_and_waits_twice_as_long_after_the_second() {
        let mut rig = Rig::default();
        rig.run(RATE_TICKS as u32, 1_000, 90, 1);
        rig.run(PROBE_TICKS - 1, 800, 90, 1);
        assert!(!rig.run(1, 800, 90, 1));
        assert_eq!((rig.probe.keeps, rig.probe.reverts), (0, 1));
        assert_eq!(rig.probe.wait, PROBE_BACKOFF_TICKS);
        assert!(!rig.run(PROBE_BACKOFF_TICKS - 1, 1_000, 90, 1));
        assert_eq!(rig.probe.probes, 1);
        assert!(rig.run(1, 1_000, 90, 1));
        assert_eq!(rig.probe.probes, 2);
        rig.run(PROBE_TICKS - 1, 500, 90, 1);
        assert!(!rig.run(1, 500, 90, 1));
        assert_eq!(rig.probe.reverts, 2);
        assert_eq!(rig.probe.wait, PROBE_BACKOFF_TICKS * 2);
    }

    #[test]
    fn low_busyness_leaves_the_shared_pipes_without_a_backoff() {
        let mut rig = Rig::default();
        rig.run(RATE_TICKS as u32, 1_000, 90, 1);
        assert!(rig.run(PROBE_TICKS, 1_200, 90, 1));
        assert!(rig.run(LEAVE_TICKS - 1, 1_200, BUSY_LEAVE, 1));
        assert!(!rig.run(1, 1_200, BUSY_LEAVE, 1));
        assert_eq!(rig.probe.wait, 0);
        assert_eq!((rig.probe.probes, rig.probe.keeps), (1, 1));
    }

    #[test]
    fn a_reverse_probe_returns_to_local_when_the_shared_pipes_are_not_faster() {
        let mut rig = Rig::default();
        rig.run(RATE_TICKS as u32, 1_000, 90, 1);
        rig.run(PROBE_TICKS, 1_200, 90, 1);
        assert!(!rig.run(PROBE_BACKOFF_TICKS, 1_200, 90, 1));
        assert_eq!(rig.probe.probes, 2);
        rig.run(PROBE_TICKS - 1, 1_200, 90, 1);
        assert!(!rig.run(1, 1_200, 90, 1));
        assert_eq!((rig.probe.keeps, rig.probe.reverts), (1, 1));
        assert_eq!(rig.probe.wait, PROBE_BACKOFF_TICKS);
    }

    #[test]
    fn an_idle_worker_never_probes() {
        let mut rig = Rig::default();
        assert!(!rig.run(PROBE_BACKOFF_TICKS, MIN_RATE_PER_SEC - 10, 90, 1));
        assert_eq!(rig.probe.probes, 0);
    }

    #[derive(Default)]
    struct Rig {
        probe: Probe,
        commands: u64,
    }

    impl Rig {
        fn run(&mut self, ticks: u32, per_sec: u64, busy_pct: u32, depth: u64) -> bool {
            let mut prefer = self.probe.prefer;
            for _ in 0..ticks {
                self.commands += per_sec / RATE_TICKS as u64;
                prefer = self.probe.tick(busy_pct, depth, self.commands);
            }
            prefer
        }
    }
}

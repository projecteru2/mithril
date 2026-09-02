//! Pipe selection under auto sharding: the session score and the worker-level tuner.

use std::rc::Rc;
use std::time::{Duration, Instant};

use super::Shared;
use super::session::Session;
use crate::stats;

// pipelining score: a local session shares at 0, a shared one returns at PIPELINED_LOCAL
pub(super) const PIPELINED_LOCAL: u8 = 4;
const PIPELINED_MAX: u8 = 8;
// auto sharding moves a worker's sessions to the shared pipes while the worker is
// busy and its local backend batches would stay thin (in-flight commands per master)
const TUNE_PERIOD: Duration = Duration::from_millis(100);
const BUSY_ENTER: u32 = 85;
const BUSY_LEAVE: u32 = 60;
const DEPTH_ENTER: u64 = 8;
const DEPTH_LEAVE: u64 = 16;
// ticks the enter or leave condition must hold: moving sessions off a worker
// lowers its own busyness, so leaving is slow and entering prompt
const ENTER_TICKS: u32 = 3;
const LEAVE_TICKS: u32 = 30;

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

/// Samples this worker's CPU busyness and in-flight depth per master and publishes
/// whether its sessions should prefer the shared pipes.
pub async fn auto_tuner(shared: Rc<Shared>) {
    let mut last_ticks = stats::thread_cpu_ticks();
    let mut last_at = Instant::now();
    let mut busy_x16 = 0u32;
    let mut streak = 0u32;
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
        let cur = shared.prefer_shared.get();
        let wanted = prefer_shared(cur, (busy_x16 + 8) / 16, depth);
        shared.prefer_shared.set(settle(cur, wanted, &mut streak));
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

// hysteresis on both signals so a worker does not flap its sessions between pipe kinds
fn prefer_shared(current: bool, busy_pct: u32, depth: u64) -> bool {
    if current {
        busy_pct > BUSY_LEAVE && depth < DEPTH_LEAVE
    } else {
        busy_pct >= BUSY_ENTER && depth < DEPTH_ENTER
    }
}

// a change of preference must hold for ENTER_TICKS or LEAVE_TICKS samples
fn settle(current: bool, wanted: bool, streak: &mut u32) -> bool {
    if wanted == current {
        *streak = 0;
        return current;
    }
    *streak += 1;
    let need = if current { LEAVE_TICKS } else { ENTER_TICKS };
    if *streak < need {
        return current;
    }
    *streak = 0;
    wanted
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
    fn worker_preference_overrides_the_score_with_hysteresis() {
        assert!(switch_pipes(false, PIPELINED_MAX, true));
        assert!(!switch_pipes(true, PIPELINED_MAX, true));
        assert!(!prefer_shared(false, 84, 2));
        assert!(!prefer_shared(false, 90, DEPTH_ENTER));
        assert!(prefer_shared(false, 90, DEPTH_ENTER - 1));
        assert!(prefer_shared(true, 61, DEPTH_LEAVE - 1));
        assert!(!prefer_shared(true, 60, 1));
        assert!(!prefer_shared(true, 99, DEPTH_LEAVE));
        let mut b = 0;
        for _ in 0..64 {
            b = busy_ewma(b, BUSY_ENTER);
        }
        assert_eq!((b + 8) / 16, BUSY_ENTER);
        assert_eq!(tune_depth(3, 10, 900, 128), 3);
        assert_eq!(tune_depth(3, 0, 900, 128), 7);
        let mut streak = 0;
        for _ in 0..ENTER_TICKS - 1 {
            assert!(!settle(false, true, &mut streak));
        }
        assert!(settle(false, true, &mut streak));
        for _ in 0..LEAVE_TICKS - 1 {
            assert!(settle(true, false, &mut streak));
        }
        assert!(!settle(true, false, &mut streak));
        assert!(settle(true, true, &mut streak));
        assert_eq!(streak, 0);
    }
}

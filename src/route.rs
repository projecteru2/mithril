//! Node selection: slot ownership plus replica read balancing.

use crate::config::SlaveMode;
use crate::topology::Topology;

/// Picks the target node address for `slot`, honoring read splitting.
pub fn pick(
    topo: &Topology,
    slot: u16,
    readonly: bool,
    mode: SlaveMode,
    rng: &mut u64,
) -> Option<(u16, bool)> {
    let midx = topo.owner(slot)?;
    let master = &topo.nodes[midx as usize];
    if !readonly || mode == SlaveMode::Off || master.replicas.is_empty() {
        return Some((midx, false));
    }
    let n = master.replicas.len();
    let pool = match mode {
        SlaveMode::WriteOnly => n,
        _ => n + 1,
    };
    let pick = bounded(next_rand(rng), pool);
    if pick >= n {
        return Some((midx, false));
    }
    Some((master.replicas[pick], true))
}

/// Picks a random master for keyless commands.
pub fn any_master(topo: &Topology, rng: &mut u64) -> Option<u16> {
    if topo.masters.is_empty() {
        return None;
    }
    Some(topo.masters[bounded(next_rand(rng), topo.masters.len())])
}

// multiply-shift keeps the uniform draw and avoids a runtime-divisor divide
pub(crate) fn bounded(x: u64, n: usize) -> usize {
    ((u128::from(x) * n as u128) >> 64) as usize
}

pub(crate) fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Topology;

    const SAMPLE: &str = "\
m1 10.0.0.1:7001@17001 master - 0 1 1 connected 0-8191\n\
m2 10.0.0.2:7002@17002 master - 0 1 2 connected 8192-16383\n\
r1 10.0.0.3:7003@17003 slave m1 0 1 1 connected\n";

    #[test]
    fn routes_writes_to_master() {
        let topo = Topology::parse(SAMPLE).unwrap();
        let mut rng = 42;
        for _ in 0..16 {
            let (idx, is_replica) = pick(&topo, 0, false, SlaveMode::WriteOnly, &mut rng).unwrap();
            assert_eq!(topo.nodes[idx as usize].addr, "10.0.0.1:7001");
            assert!(!is_replica);
        }
    }

    #[test]
    fn writeonly_reads_go_to_replicas() {
        let topo = Topology::parse(SAMPLE).unwrap();
        let mut rng = 42;
        for _ in 0..16 {
            let (idx, is_replica) = pick(&topo, 0, true, SlaveMode::WriteOnly, &mut rng).unwrap();
            assert_eq!(topo.nodes[idx as usize].addr, "10.0.0.3:7003");
            assert!(is_replica);
        }
        let (idx, _) = pick(&topo, 8192, true, SlaveMode::WriteOnly, &mut rng).unwrap();
        assert_eq!(topo.nodes[idx as usize].addr, "10.0.0.2:7002");
    }

    #[test]
    fn readwrite_reads_balance_over_master_and_replicas() {
        let topo = Topology::parse(SAMPLE).unwrap();
        let mut rng = 7;
        let mut hit_master = false;
        let mut hit_replica = false;
        for _ in 0..64 {
            let (_, is_replica) = pick(&topo, 0, true, SlaveMode::ReadWrite, &mut rng).unwrap();
            if is_replica {
                hit_replica = true;
            } else {
                hit_master = true;
            }
        }
        assert!(hit_master && hit_replica);
    }
}

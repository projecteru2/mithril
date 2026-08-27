//! Cluster topology: CLUSTER NODES parsing and the slot-to-node map.

use crate::crc16::SLOTS;

pub const NO_NODE: u16 = u16::MAX;

/// One cluster node as seen in CLUSTER NODES output.
#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub addr: String,
    pub master: Option<u16>,
    pub fail: bool,
    pub replicas: Vec<u16>,
}

impl Node {
    pub fn is_master(&self) -> bool {
        self.master.is_none()
    }
}

/// Immutable snapshot of cluster topology, swapped atomically on refresh.
#[derive(Debug)]
pub struct Topology {
    pub nodes: Vec<Node>,
    pub slots: Vec<u16>,
    pub masters: Vec<u16>,
    pub raw: String,
}

impl Topology {
    /// Parses CLUSTER NODES text; rejects torn or masterless snapshots.
    pub fn parse(raw: &str) -> Result<Topology, String> {
        let mut nodes = Vec::new();
        let mut slot_specs: Vec<(u16, Vec<(u16, u16)>)> = Vec::new();
        let mut master_ids: Vec<(String, u16)> = Vec::new();
        let mut replica_refs: Vec<(u16, String)> = Vec::new();

        for line in raw.lines() {
            let mut fields = line.split(' ');
            let (Some(id), Some(addr), Some(flags)) = (fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            if flags.contains("handshake") || flags.contains("noaddr") {
                continue;
            }
            let master_ref = fields.next().unwrap_or("-");
            let addr = addr.split('@').next().unwrap_or(addr);
            let idx = nodes.len() as u16;
            let is_master = flags.contains("master");
            let fail = flags.contains("fail") && !flags.contains("failover");
            if is_master {
                master_ids.push((id.to_string(), idx));
                let ranges = fields
                    .skip(4)
                    .filter(|f| !f.starts_with('['))
                    .filter_map(parse_slot_range)
                    .collect::<Vec<_>>();
                slot_specs.push((idx, ranges));
            } else {
                replica_refs.push((idx, master_ref.to_string()));
            }
            nodes.push(Node {
                id: id.to_string(),
                addr: addr.to_string(),
                master: None,
                fail,
                replicas: Vec::new(),
            });
        }

        let mut masters: Vec<u16> = Vec::new();
        for (_, idx) in &master_ids {
            masters.push(*idx);
        }
        if masters.is_empty() {
            return Err("no masters in topology".to_string());
        }
        for (idx, master_ref) in replica_refs {
            let Some((_, midx)) = master_ids.iter().find(|(id, _)| *id == master_ref) else {
                return Err(format!("replica {} references unknown master", idx));
            };
            nodes[idx as usize].master = Some(*midx);
            let replica_ok = !nodes[idx as usize].fail;
            if replica_ok {
                nodes[*midx as usize].replicas.push(idx);
            }
        }

        let mut slots = vec![NO_NODE; SLOTS];
        for (idx, ranges) in slot_specs {
            for (start, end) in ranges {
                if end as usize >= SLOTS {
                    return Err(format!("slot range {start}-{end} out of bounds"));
                }
                for s in start..=end {
                    slots[s as usize] = idx;
                }
            }
        }
        if slots.iter().all(|&s| s == NO_NODE) {
            return Err("no slots assigned".to_string());
        }
        Ok(Topology {
            nodes,
            slots,
            masters,
            raw: raw.to_string(),
        })
    }

    /// Returns the node index owning `slot`, if assigned.
    pub fn owner(&self, slot: u16) -> Option<u16> {
        match self.slots[slot as usize] {
            NO_NODE => None,
            idx => Some(idx),
        }
    }
}

fn parse_slot_range(field: &str) -> Option<(u16, u16)> {
    match field.split_once('-') {
        Some((a, b)) => Some((a.parse().ok()?, b.parse().ok()?)),
        None => {
            let s = field.parse().ok()?;
            Some((s, s))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
07c3 127.0.0.1:7002@17002 master - 0 1 2 connected 5461-10922\n\
e7d1 127.0.0.1:7001@17001 myself,master - 0 1 1 connected 0-5460 12000\n\
a1b2 127.0.0.1:7003@17003 master - 0 1 3 connected 10923-11999 12001-16383\n\
99ff 127.0.0.1:7004@17004 slave e7d1 0 1 1 connected\n";

    #[test]
    fn parses_cluster_nodes() {
        let topo = Topology::parse(SAMPLE).unwrap();
        assert_eq!(topo.nodes.len(), 4);
        assert_eq!(topo.masters.len(), 3);
        let owner = topo.owner(0).unwrap();
        assert_eq!(topo.nodes[owner as usize].addr, "127.0.0.1:7001");
        assert_eq!(topo.owner(12000), topo.owner(0));
        let m = topo.owner(0).unwrap() as usize;
        assert_eq!(topo.nodes[m].replicas.len(), 1);
        assert!(topo.owner(5461) != topo.owner(0));
    }

    #[test]
    fn rejects_torn_snapshots() {
        assert!(Topology::parse("").is_err());
        let orphan = "99ff 1.2.3.4:7004@17004 slave nope 0 1 1 connected\n\
                      e7d1 1.2.3.4:7001@17001 master - 0 1 1 connected 0-16383\n";
        assert!(Topology::parse(orphan).is_err());
    }
}

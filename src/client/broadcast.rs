//! Cluster-wide commands: one request to every master or every node, the replies folded into one.

use bytes::Bytes;

use tokio::sync::oneshot;

use super::Reply;
use super::pipe::{recv_or_lost, scatter_one};
use super::session::Session;
use super::writer::{REDIRECT_HOPS, REDIRECT_WAIT};
use crate::multikey;
use crate::resp;
use crate::stats;
use crate::topology::Topology;

/// Which nodes a broadcast reaches.
#[derive(Clone, Copy)]
pub(super) enum Targets {
    Masters,
    /// Every node not flagged failed; a node that comes back is served by the NOSCRIPT reload.
    LiveNodes,
    /// Every known node; an unreachable one makes the broadcast report its loss.
    AllNodes,
}

/// How the replies of a broadcast fold into one.
#[derive(Clone, Copy)]
pub(super) enum Gather {
    Sum,
    Ok,
    Same,
    Every,
}

impl Session {
    // the session waits for the replies: a later command of the same client never overtakes
    // a keyless write, not even one resent after a failover
    pub(super) async fn run_broadcast(
        &self,
        frame: Bytes,
        targets: Targets,
        gather: Gather,
        // a SCRIPT LOAD body with the flush count at its dispatch, remembered under the returned sha
        remember: Option<(Bytes, u64)>,
    ) {
        let seq = self.alloc_seq();
        let shared = &self.shared;
        let topo = shared.topo.load_full();
        let lane = self.lane();
        let all;
        let nodes: &[u16] = match targets {
            Targets::Masters => &topo.masters,
            Targets::LiveNodes | Targets::AllNodes => {
                let live_only = matches!(targets, Targets::LiveNodes);
                all = (0..topo.nodes.len() as u16)
                    .filter(|&i| !(live_only && topo.nodes[i as usize].fail))
                    .collect::<Vec<_>>();
                &all
            }
        };
        let mut receivers = Vec::with_capacity(nodes.len());
        for &i in nodes {
            let addr = &topo.nodes[i as usize].addr;
            receivers.push(scatter_one(shared, addr, lane, None, frame.clone()).await);
        }
        let mut replies = collect(receivers).await;
        if matches!(targets, Targets::Masters) {
            self.ride_out_demoted(&topo, nodes, &frame, &mut replies)
                .await;
        }
        let merged = match gather {
            Gather::Sum => multikey::merge_sum(replies.iter(), 0),
            Gather::Ok => multikey::merge_ok(replies.iter()),
            Gather::Same => multikey::merge_same(replies.iter()),
            Gather::Every => multikey::merge_every(replies.iter()),
        };
        if let (Ok(reply), Some((body, flushes))) = (&merged, remember)
            && let Some(sha) = resp::bulk_payload(reply)
        {
            shared.scripts.remember(sha, body, flushes);
        }
        let _ = self
            .reply_q
            .send(Reply::At(seq, merged.unwrap_or_else(|e| e)));
    }

    // a master demoted since the last refresh answers a keyless write READONLY: after a
    // refresh its shard's new master gets the request, the replies of the others stand
    async fn ride_out_demoted(
        &self,
        topo: &Topology,
        nodes: &[u16],
        frame: &Bytes,
        replies: &mut [Bytes],
    ) {
        let shared = &self.shared;
        let mut wait = REDIRECT_WAIT;
        for _ in 0..REDIRECT_HOPS {
            let demoted: Vec<usize> = (0..replies.len())
                .filter(|&k| replies[k].starts_with(b"-READONLY"))
                .collect();
            if demoted.is_empty() {
                return;
            }
            stats::bump(&shared.wstats.redirect_waits);
            let _ = shared.refresh.send(());
            tokio::time::sleep(wait).await;
            wait *= 2;
            let fresh = shared.topo.load_full();
            for k in demoted {
                let Some(slot) = topo.slots.iter().position(|&o| o == nodes[k]) else {
                    continue;
                };
                let Some(addr) = fresh.owner_addr(slot as u16) else {
                    continue;
                };
                let rx = scatter_one(shared, addr, self.lane(), None, frame.clone()).await;
                replies[k] = recv_or_lost(rx).await;
            }
        }
    }
}

async fn collect(receivers: Vec<oneshot::Receiver<Bytes>>) -> Vec<Bytes> {
    let mut replies = Vec::with_capacity(receivers.len());
    for rx in receivers {
        replies.push(recv_or_lost(rx).await);
    }
    replies
}

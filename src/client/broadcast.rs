//! Cluster-wide commands: one request to every master or every node, the replies folded into one.

use std::rc::Rc;

use bytes::Bytes;

use tokio::sync::oneshot;

use super::pipe::{recv_or_lost, scatter_one};
use super::session::Session;
use super::writer::{REDIRECT_HOPS, REDIRECT_WAIT};
use super::{Lane, Reply, Shared};
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
    pub(super) async fn run_broadcast(
        &self,
        frame: Bytes,
        targets: Targets,
        gather: Gather,
        // a SCRIPT LOAD body with the flush count at its dispatch, remembered under the returned sha
        remember: Option<(Bytes, u64)>,
    ) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let link = self.link.clone();
        let topo = shared.topo.load_full();
        let lane = self.lane();
        let all: Vec<u16> = match targets {
            Targets::Masters => Vec::new(),
            Targets::LiveNodes | Targets::AllNodes => {
                let live_only = matches!(targets, Targets::LiveNodes);
                (0..topo.nodes.len() as u16)
                    .filter(|&i| !(live_only && topo.nodes[i as usize].fail))
                    .collect()
            }
        };
        let nodes: &[u16] = match targets {
            Targets::Masters => &topo.masters,
            _ => &all,
        };
        let mut receivers = Vec::with_capacity(nodes.len());
        for &i in nodes {
            let addr = &topo.nodes[i as usize].addr;
            receivers.push(scatter_one(&shared, addr, lane, None, frame.clone()).await);
        }
        link.hold.set(true);
        // detached: the hold keeps later commands of the session behind it while its reader
        // still sees a hang-up, and teardown aborts it like a blocking command
        let task = tokio::task::spawn_local(async move {
            let mut replies = collect(receivers).await;
            if matches!(targets, Targets::Masters) {
                ride_out_demoted(&shared, &topo, &topo.masters, lane, &frame, &mut replies).await;
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
            link.hold.set(false);
            let _ = reply_q.send(Reply::At(seq, merged.unwrap_or_else(|e| e)));
        });
        self.link.track(seq, task);
    }
}

// a leg answered READONLY is a master demoted since the last refresh: after a refresh its
// shard's new master gets the request; the replies of the other legs stand
async fn ride_out_demoted(
    shared: &Rc<Shared>,
    topo: &Topology,
    nodes: &[u16],
    lane: Lane,
    frame: &Bytes,
    replies: &mut [Bytes],
) {
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
            let rx = scatter_one(shared, addr, lane, None, frame.clone()).await;
            replies[k] = recv_or_lost(rx).await;
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

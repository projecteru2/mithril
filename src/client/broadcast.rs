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
            receivers.push(scatter_one(&shared, addr, lane, None, frame.clone()).await);
        }
        // detached deliberately: completion is bounded by backend replies
        tokio::task::spawn_local(async move {
            let mut replies = collect(receivers).await;
            // a master demoted since the last refresh answers a keyless write READONLY: ask for
            // the topology again and send once more to the masters it reports, a few times
            let mut wait = REDIRECT_WAIT;
            for _ in 0..REDIRECT_HOPS {
                if !matches!(targets, Targets::Masters)
                    || !replies.iter().any(|r| r.starts_with(b"-READONLY"))
                {
                    break;
                }
                stats::bump(&shared.wstats.redirect_waits);
                let _ = shared.refresh.send(());
                tokio::time::sleep(wait).await;
                wait *= 2;
                let topo = shared.topo.load_full();
                let mut receivers = Vec::with_capacity(topo.masters.len());
                for &i in &topo.masters {
                    let addr = &topo.nodes[i as usize].addr;
                    receivers.push(scatter_one(&shared, addr, lane, None, frame.clone()).await);
                }
                replies = collect(receivers).await;
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
            let _ = reply_q.send(Reply::At(seq, merged.unwrap_or_else(|e| e)));
        });
    }
}

async fn collect(receivers: Vec<oneshot::Receiver<Bytes>>) -> Vec<Bytes> {
    let mut replies = Vec::with_capacity(receivers.len());
    for rx in receivers {
        replies.push(recv_or_lost(rx).await);
    }
    replies
}

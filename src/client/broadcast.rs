//! Cluster-wide commands: one request to every master or every node, the replies folded into one.

use bytes::Bytes;

use super::pipe::{recv_or_lost, scatter_one};
use super::session::Session;
use super::{Reply, Shared};
use crate::multikey;
use crate::resp;

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

/// What a successful broadcast leaves behind in the proxy.
pub(super) enum Effect {
    /// The body a SCRIPT LOAD carried, and the flush count when it was dispatched.
    RememberScript(Bytes, u64),
}

impl Effect {
    fn apply(self, shared: &Shared, reply: &Bytes) {
        match self {
            Effect::RememberScript(body, flushes) => {
                if let Some(sha) = resp::bulk_payload(reply) {
                    shared.scripts.remember(sha, body, flushes);
                }
            }
        }
    }
}

impl Session {
    pub(super) async fn run_broadcast(
        &self,
        frame: Bytes,
        targets: Targets,
        gather: Gather,
        effect: Option<Effect>,
    ) {
        let seq = self.alloc_seq();
        let shared = self.shared.clone();
        let reply_q = self.reply_q.clone();
        let topo = shared.topo.load_full();
        let sharded = self.link.sharded.get();
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
            receivers.push(scatter_one(&shared, addr, self.id, sharded, None, frame.clone()).await);
        }
        // detached deliberately: completion is bounded by backend replies
        tokio::task::spawn_local(async move {
            let mut replies: Vec<Bytes> = Vec::with_capacity(receivers.len());
            for rx in receivers {
                replies.push(recv_or_lost(rx).await);
            }
            let merged = match gather {
                Gather::Sum => multikey::merge_sum(replies.iter(), 0),
                Gather::Ok => multikey::merge_ok(replies.iter()),
                Gather::Same => multikey::merge_same(replies.iter()),
                Gather::Every => multikey::merge_every(replies.iter()),
            };
            if let (Ok(reply), Some(effect)) = (&merged, effect) {
                effect.apply(&shared, reply);
            }
            let _ = reply_q.send(Reply::At(seq, merged.unwrap_or_else(|e| e)));
        });
    }
}

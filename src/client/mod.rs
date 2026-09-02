//! Client sessions: dispatch, ordered replies, MULTI, pubsub, redirects.

mod blocking;
mod fanout;
mod link;
mod local;
mod pipe;
mod pubsub;
mod queue;
mod session;
mod tuner;
mod writer;

pub use queue::{ReplyQueue, SharedQueue};
pub use session::serve;
pub use tuner::auto_tuner;

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::backend::Backends;
use crate::cache::ReplyCache;
use crate::config::Config;
use crate::resp;
use crate::shard::Fabric;
use crate::stats::{self, Stats};
use crate::topology::Topology;

pub(super) const MAX_INFLIGHT: usize = 65536;
pub(super) const ERR_NOAUTH: &[u8] = b"-NOAUTH Authentication required.\r\n";
pub(super) const ERR_CROSSSLOT: &[u8] =
    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n";
pub(super) const ERR_NO_OWNER: &[u8] = b"-CLUSTERDOWN Hash slot not served\r\n";
pub(super) const ERR_TRYAGAIN: &[u8] = b"-TRYAGAIN slot is migrating, retry later\r\n";

/// Everything a session needs from its worker.
pub struct Shared {
    pub cfg: Arc<Config>,
    pub topo: Arc<ArcSwap<Topology>>,
    pub backends: Rc<Backends>,
    pub wstats: Arc<stats::WorkerStats>,
    pub stats: Arc<Stats>,
    pub refresh: mpsc::UnboundedSender<()>,
    pub started: u64,
    pub fabric: Option<Arc<Fabric>>,
    pub cache: Option<Rc<ReplyCache>>,
    pub inflight: Cell<u64>,
    pub prefer_shared: Cell<bool>,
}

/// One frame travelling to the client writer.
pub enum Reply {
    /// Ordered reply at its sequence.
    At(u64, Bytes),
    /// Pubsub confirmation at its sequence, emitted as a push frame.
    Ack(u64, Bytes),
    /// Out-of-band push, never emitted before the ack it followed.
    Push { after: Option<u64>, frame: Bytes },
    /// Closes the client connection once the pending batch is flushed.
    Close,
}

// the awaited remainder of a command whose fast path could not finish
pub(super) type Cold<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

pub(super) fn error_frame(msg: &str) -> Bytes {
    let mut out = Vec::with_capacity(msg.len() + 3);
    resp::write_error(&mut out, msg);
    Bytes::from(out)
}

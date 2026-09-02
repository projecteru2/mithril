//! Per-session reply queue: worker-local by default, mutex-backed when owner workers deliver.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::Reply;

/// The session's reply queue: a RefCell locally, a mutex when owner workers deliver.
pub struct ReplyQueue {
    inner: QueueInner,
}

impl ReplyQueue {
    pub(super) fn new(shared: bool) -> Rc<ReplyQueue> {
        let inner = if shared {
            QueueInner::Shared(Arc::new(SharedQueue {
                state: Mutex::new(SharedState {
                    closed: false,
                    q: VecDeque::new(),
                }),
                bell: Notify::new(),
            }))
        } else {
            QueueInner::Local {
                q: RefCell::new(VecDeque::new()),
                bell: Notify::new(),
                closed: Cell::new(false),
            }
        };
        Rc::new(ReplyQueue { inner })
    }

    pub(super) fn shard_handle(&self) -> Option<Arc<SharedQueue>> {
        match &self.inner {
            QueueInner::Local { .. } => None,
            QueueInner::Shared(sq) => Some(sq.clone()),
        }
    }

    pub fn send(&self, reply: Reply) -> Result<(), Reply> {
        match &self.inner {
            QueueInner::Local { q, bell, closed } => {
                if closed.get() {
                    return Err(reply);
                }
                let mut q = q.borrow_mut();
                q.push_back(reply);
                if q.len() == 1 {
                    bell.notify_one();
                }
                Ok(())
            }
            QueueInner::Shared(sq) => sq.send(reply),
        }
    }

    // one lock per refill keeps the coalescing pass off per-item locking
    pub(super) fn pop_into(&self, batch: &mut Vec<Reply>, max: usize) {
        match &self.inner {
            QueueInner::Local { q, .. } => {
                let mut q = q.borrow_mut();
                while batch.len() < max {
                    match q.pop_front() {
                        Some(r) => batch.push(r),
                        None => break,
                    }
                }
            }
            QueueInner::Shared(sq) => {
                let Ok(mut state) = sq.state.lock() else {
                    return;
                };
                while batch.len() < max {
                    match state.q.pop_front() {
                        Some(r) => batch.push(r),
                        None => break,
                    }
                }
            }
        }
    }

    // cancel-safe: the drain is synchronous and a notified permit persists
    pub(super) async fn recv_batch(&self, batch: &mut Vec<Reply>, max: usize) {
        let bell = match &self.inner {
            QueueInner::Local { bell, .. } => bell,
            QueueInner::Shared(sq) => &sq.bell,
        };
        loop {
            self.pop_into(batch, max);
            if !batch.is_empty() {
                return;
            }
            bell.notified().await;
        }
    }

    // queued frames and their backing buffer must not outlive the writer
    pub(super) fn close(&self) {
        match &self.inner {
            QueueInner::Local { q, closed, .. } => {
                closed.set(true);
                drop(std::mem::take(&mut *q.borrow_mut()));
            }
            QueueInner::Shared(sq) => {
                if let Ok(mut state) = sq.state.lock() {
                    state.closed = true;
                    drop(std::mem::take(&mut state.q));
                }
            }
        }
    }
}

/// Cross-worker reply queue for sharded sessions.
pub struct SharedQueue {
    state: Mutex<SharedState>,
    bell: Notify,
}

impl SharedQueue {
    /// Delivers a reply straight from an owner worker.
    pub fn send(&self, reply: Reply) -> Result<(), Reply> {
        let Ok(mut state) = self.state.lock() else {
            return Err(reply);
        };
        if state.closed {
            return Err(reply);
        }
        state.q.push_back(reply);
        // the writer only parks on an empty queue: notify on that transition
        if state.q.len() == 1 {
            self.bell.notify_one();
        }
        Ok(())
    }
}

enum QueueInner {
    Local {
        q: RefCell<VecDeque<Reply>>,
        bell: Notify,
        closed: Cell<bool>,
    },
    Shared(Arc<SharedQueue>),
}

struct SharedState {
    closed: bool,
    q: VecDeque<Reply>,
}

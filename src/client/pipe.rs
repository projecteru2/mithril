//! Routes to backend nodes and the requests staged on them.

use std::rc::Rc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::queue::ReplyQueue;
use super::{Reply, Shared};
use crate::backend::{Conn, ERR_BACKEND_LOST, Outbound, Sink};
use crate::shard::{RemoteOutbound, RemoteSink};

/// A route to one backend node: worker-local conn or the process-wide shard.
#[derive(Clone)]
pub(super) enum Pipe {
    Local(Rc<Conn>),
    Shard(mpsc::Sender<RemoteOutbound>),
}

impl Pipe {
    pub(super) fn is_dead(&self) -> bool {
        match self {
            Pipe::Local(conn) => conn.is_dead(),
            Pipe::Shard(tx) => tx.is_closed(),
        }
    }

    pub(super) fn has_room(&self, n: usize) -> bool {
        match self {
            Pipe::Local(conn) => conn.has_room(n),
            Pipe::Shard(tx) => tx.capacity() >= n,
        }
    }
}

// the rare full-queue leftover; flushed with an awaited send
pub(super) struct ColdSend {
    staged: Staged,
    reply_q: Rc<ReplyQueue>,
}

impl ColdSend {
    pub(super) async fn flush(self: Box<Self>) {
        let ColdSend { staged, reply_q } = *self;
        match staged {
            Staged::Local(conn, out) => conn.send_wait(out).await,
            Staged::Shard(tx, out) => {
                if let Err(e) = Box::pin(tx.send(out)).await
                    && let RemoteSink::Session { seq, .. } = e.0.sink
                {
                    let _ = reply_q.send(Reply::At(seq, Bytes::from_static(ERR_BACKEND_LOST)));
                }
            }
        }
    }
}

// a request with a oneshot sink, built for either pipe flavor
pub(super) enum Staged {
    Local(Rc<Conn>, Outbound),
    Shard(mpsc::Sender<RemoteOutbound>, RemoteOutbound),
}

impl Staged {
    // a closed queue drops the oneshot, which the receiver reads as LOST; a
    // full one hands the request back to be awaited
    pub(super) fn try_send(self) -> Result<(), Staged> {
        match self {
            Staged::Local(conn, out) => match conn.try_send(out) {
                Ok(()) => Ok(()),
                Err(out) => Err(Staged::Local(conn, out)),
            },
            Staged::Shard(tx, out) => match tx.try_send(out) {
                Err(mpsc::error::TrySendError::Full(out)) => Err(Staged::Shard(tx, out)),
                _ => Ok(()),
            },
        }
    }

    pub(super) async fn send(self) {
        match self {
            Staged::Local(conn, out) => conn.send(out).await,
            Staged::Shard(tx, out) => {
                let _ = tx.send(out).await;
            }
        }
    }
}

pub(super) fn pipe_for(
    shared: &Shared,
    addr: &str,
    id: u64,
    readonly: bool,
    sharded: bool,
) -> Pipe {
    match &shared.fabric {
        Some(f) if sharded => Pipe::Shard(f.pipe(addr, readonly)),
        _ => Pipe::Local(shared.backends.shared(addr, id, readonly)),
    }
}

// queues one request for the session's reply stream; Err when its shard is gone
pub(super) fn queue_on(
    pipe: &Pipe,
    reply_q: &Rc<ReplyQueue>,
    seq: u64,
    head: Option<Bytes>,
    frame: Bytes,
    expect: u32,
) -> Result<Option<Box<ColdSend>>, ()> {
    match pipe {
        Pipe::Local(conn) => {
            let out = Outbound {
                head,
                frame,
                expect,
                sink: Sink::Client(reply_q.clone(), seq),
            };
            Ok(conn.try_send(out).err().map(|out| {
                Box::new(ColdSend {
                    staged: Staged::Local(conn.clone(), out),
                    reply_q: reply_q.clone(),
                })
            }))
        }
        Pipe::Shard(tx) => {
            let queue = reply_q.shard_handle().ok_or(())?;
            let out = RemoteOutbound {
                head,
                frame,
                expect,
                sink: RemoteSink::Session { queue, seq },
            };
            match tx.try_send(out) {
                Ok(()) => Ok(None),
                Err(mpsc::error::TrySendError::Full(out)) => Ok(Some(Box::new(ColdSend {
                    staged: Staged::Shard(tx.clone(), out),
                    reply_q: reply_q.clone(),
                }))),
                Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
            }
        }
    }
}

pub(super) fn stage_one(
    pipe: &Pipe,
    head: Option<Bytes>,
    frame: Bytes,
) -> (Staged, oneshot::Receiver<Bytes>) {
    let (tx, rx) = oneshot::channel();
    let expect = 1 + u32::from(head.is_some());
    let staged = match pipe {
        Pipe::Local(conn) => Staged::Local(
            conn.clone(),
            Outbound {
                head,
                frame,
                expect,
                sink: Sink::One(tx),
            },
        ),
        Pipe::Shard(sender) => Staged::Shard(
            sender.clone(),
            Outbound {
                head,
                frame,
                expect,
                sink: RemoteSink::One(tx),
            },
        ),
    };
    (staged, rx)
}

pub(super) async fn scatter_pipe(
    pipe: &Pipe,
    head: Option<Bytes>,
    frame: Bytes,
) -> oneshot::Receiver<Bytes> {
    let (staged, rx) = stage_one(pipe, head, frame);
    staged.send().await;
    rx
}

pub(super) async fn scatter_one(
    shared: &Rc<Shared>,
    addr: &str,
    id: u64,
    sharded: bool,
    head: Option<Bytes>,
    frame: Bytes,
) -> oneshot::Receiver<Bytes> {
    scatter_pipe(&pipe_for(shared, addr, id, false, sharded), head, frame).await
}

pub(super) async fn recv_or_lost(rx: oneshot::Receiver<Bytes>) -> Bytes {
    rx.await
        .unwrap_or_else(|_| Bytes::from_static(ERR_BACKEND_LOST))
}

pub(super) fn parse_redirect(frame: &[u8]) -> Option<(bool, &str)> {
    let ask = if frame.starts_with(b"-MOVED ") {
        false
    } else if frame.starts_with(b"-ASK ") {
        true
    } else {
        return None;
    };
    let text = std::str::from_utf8(frame).ok()?;
    let addr = text.trim_end().rsplit(' ').next()?;
    if !addr.contains(':') {
        return None;
    }
    Some((ask, addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_redirects() {
        assert_eq!(
            parse_redirect(b"-MOVED 3999 10.0.0.2:7002\r\n"),
            Some((false, "10.0.0.2:7002"))
        );
        assert_eq!(
            parse_redirect(b"-ASK 42 10.0.0.3:7003\r\n"),
            Some((true, "10.0.0.3:7003"))
        );
        assert_eq!(parse_redirect(b"-ERR nope\r\n"), None);
        assert_eq!(parse_redirect(b"-MOVED garbage\r\n"), None);
    }
}

//! Backend connections: shared pipelined conns per node, exclusive ones for blocking.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::IoSlice;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

use crate::client::{Reply, ReplyQueue};
use crate::config::Config;
use crate::log_debug;
use crate::resp;

pub const OUTBOUND_QUEUE: usize = 8192;
pub const READ_CHUNK: usize = 64 * 1024;
pub const READ_INIT: usize = 8 * 1024;
pub const BATCH: usize = 256;
pub const MAX_EXCLUSIVE_PER_NODE: usize = 512;

pub const ASKING_FRAME: &[u8] = b"*1\r\n$6\r\nASKING\r\n";
pub const ERR_BACKEND_LOST: &[u8] = b"-ERR mithril: backend connection lost\r\n";

/// Where a backend reply is delivered.
pub enum Sink {
    /// Ordered client reply stream at a fixed sequence.
    Client(Rc<ReplyQueue>, u64),
    /// Single reply for mergers and blocking commands.
    One(oneshot::Sender<Bytes>),
}

/// One pipelined request: optional prefix frame, payload, expected replies.
pub struct Outbound {
    pub head: Option<Bytes>,
    pub frame: Bytes,
    /// Number of backend replies this produces; only the last is delivered.
    pub expect: u32,
    pub sink: Sink,
}

/// A live backend connection; cheap to clone via Rc.
pub struct Conn {
    tx: mpsc::Sender<Outbound>,
    dead: Cell<bool>,
    abort: tokio::sync::Notify,
}

impl Conn {
    /// Queues a request, delivering an error frame if the connection is gone.
    pub async fn send(&self, out: Outbound) {
        if let Err(out) = self.try_send(out) {
            self.send_wait(out).await;
        }
    }

    /// Queues without waiting; a full queue hands the request back.
    pub fn try_send(&self, out: Outbound) -> Result<(), Outbound> {
        if self.dead.get() {
            deliver(out.sink, Bytes::from_static(ERR_BACKEND_LOST));
            return Ok(());
        }
        match self.tx.try_send(out) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(out)) => Err(out),
            Err(mpsc::error::TrySendError::Closed(out)) => {
                deliver(out.sink, Bytes::from_static(ERR_BACKEND_LOST));
                Ok(())
            }
        }
    }

    /// Full-queue slow path; the box keeps the wait out of every caller's future.
    pub fn send_wait(&self, out: Outbound) -> impl Future<Output = ()> + '_ {
        Box::pin(async move {
            if let Err(e) = self.tx.send(out).await {
                deliver(e.0.sink, Bytes::from_static(ERR_BACKEND_LOST));
            }
        })
    }

    pub fn is_dead(&self) -> bool {
        self.dead.get()
    }

    /// Force-closes the connection so the backend cancels blocked commands.
    pub fn abort(&self) {
        self.dead.set(true);
        self.abort.notify_one();
    }
}

/// Per-worker backend pools keyed by node address, split by readonly role.
pub struct Backends {
    cfg: Rc<Config>,
    pools: RefCell<HashMap<Box<str>, PoolPair>>,
}

impl Backends {
    pub fn new(cfg: Rc<Config>) -> Rc<Backends> {
        Rc::new(Backends {
            cfg,
            pools: RefCell::new(HashMap::new()),
        })
    }

    /// Returns the sticky shared connection for `addr`.
    pub fn shared(self: &Rc<Self>, addr: &str, sticky: u64, readonly: bool) -> Rc<Conn> {
        let pool = self.pool(addr, readonly);
        let want = self.cfg.backend_conns;
        let idx = (sticky % want as u64) as usize;
        let mut conns = pool.shared.borrow_mut();
        while conns.len() <= idx {
            conns.push(self.dial(addr, readonly, false));
        }
        if conns[idx].is_dead() {
            conns[idx] = self.dial(addr, readonly, false);
        }
        conns[idx].clone()
    }

    /// Leases an exclusive connection; drop returns it or frees its quota.
    pub fn take_exclusive(self: &Rc<Self>, addr: &str) -> Option<ExclusiveLease> {
        let pool = self.pool(addr, false);
        let conn = loop {
            let idle = pool.idle_exclusive.borrow_mut().pop();
            match idle {
                Some(c) if !c.is_dead() => break c,
                Some(_) => pool
                    .exclusive_count
                    .set(pool.exclusive_count.get().saturating_sub(1)),
                None => {
                    if pool.exclusive_count.get() >= MAX_EXCLUSIVE_PER_NODE {
                        return None;
                    }
                    pool.exclusive_count.set(pool.exclusive_count.get() + 1);
                    break self.dial(addr, false, true);
                }
            }
        };
        Some(ExclusiveLease {
            conn,
            pool,
            complete: Cell::new(false),
        })
    }

    fn pool(&self, addr: &str, readonly: bool) -> Rc<Pool> {
        if let Some(pair) = self.pools.borrow().get(addr)
            && let Some(p) = &pair[usize::from(readonly)]
        {
            return p.clone();
        }
        let pool = Rc::new(Pool {
            shared: RefCell::new(Vec::new()),
            idle_exclusive: RefCell::new(Vec::new()),
            exclusive_count: Cell::new(0),
        });
        let mut pools = self.pools.borrow_mut();
        let pair = pools.entry(addr.into()).or_default();
        pair[usize::from(readonly)] = Some(pool.clone());
        pool
    }

    fn dial(&self, addr: &str, readonly: bool, abortable: bool) -> Rc<Conn> {
        let (tx, rx) = mpsc::channel(OUTBOUND_QUEUE);
        let conn = Rc::new(Conn {
            tx,
            dead: Cell::new(false),
            abort: tokio::sync::Notify::new(),
        });
        let task_conn = conn.clone();
        let addr = addr.to_string();
        let cfg = self.cfg.clone();
        tokio::task::spawn_local(async move {
            run_conn(&addr, rx, readonly, abortable, &cfg, &task_conn).await;
            task_conn.dead.set(true);
        });
        conn
    }
}

/// Lease whose incomplete drop frees quota: the pipeline still carries the command.
pub struct ExclusiveLease {
    conn: Rc<Conn>,
    pool: Rc<Pool>,
    complete: Cell<bool>,
}

impl ExclusiveLease {
    pub fn conn(&self) -> &Conn {
        &self.conn
    }

    pub fn complete(self) {
        self.complete.set(true);
    }
}

impl Drop for ExclusiveLease {
    fn drop(&mut self) {
        if self.complete.get() && !self.conn.is_dead() {
            self.pool
                .idle_exclusive
                .borrow_mut()
                .push(self.conn.clone());
        } else {
            self.conn.abort();
            self.pool
                .exclusive_count
                .set(self.pool.exclusive_count.get().saturating_sub(1));
        }
    }
}

type PoolPair = [Option<Rc<Pool>>; 2];

struct Pool {
    shared: RefCell<Vec<Rc<Conn>>>,
    idle_exclusive: RefCell<Vec<Rc<Conn>>>,
    exclusive_count: Cell<usize>,
}

/// Dials a raw authenticated backend connection for relays and the refresher.
pub async fn dial_raw(addr: &str, cfg: &Config) -> std::io::Result<TcpStream> {
    let stream = connect(addr, cfg.tcp_keepalive_secs).await?;
    if cfg.backend_pass.is_empty() {
        return Ok(stream);
    }
    let (mut r, mut w) = stream.into_split();
    handshake(&mut r, &mut w, false, &cfg.backend_user, &cfg.backend_pass)
        .await
        .map_err(std::io::Error::other)?;
    r.reunite(w).map_err(std::io::Error::other)
}

/// Writes every slice fully, advancing across partial writes.
pub async fn write_slices<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    slices: &mut [IoSlice<'_>],
) -> std::io::Result<()> {
    let mut rest = &mut slices[..];
    while !rest.is_empty() {
        let n = w.write_vectored(rest).await?;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
        }
        IoSlice::advance_slices(&mut rest, n);
    }
    Ok(())
}

// reply pairing assumes RESP2 backends: no unsolicited pushes
pub(crate) struct Pending<S> {
    pub(crate) expect: u32,
    pub(crate) sink: S,
}

// pairs buffered replies against the pipeline; Err is a protocol error
pub(crate) fn pair_replies<S>(
    buf: &mut BytesMut,
    pending: &mut VecDeque<Pending<S>>,
    front_err: &mut Option<Bytes>,
    deliver: impl Fn(S, Bytes),
) -> Result<(), &'static str> {
    loop {
        match resp::scan_value(buf) {
            resp::Scan::Complete(len) => {
                let frame = buf.split_to(len).freeze();
                let is_err = frame.first() == Some(&b'-');
                match pending.front_mut() {
                    Some(front) if front.expect > 1 => {
                        front.expect -= 1;
                        if front_err.is_none() && is_err {
                            *front_err = Some(frame);
                        }
                    }
                    _ => {
                        if let Some(d) = pending.pop_front() {
                            // an aborted MULTI reports its first queued error
                            let reply = match front_err.take() {
                                Some(err) if is_err => err,
                                _ => frame,
                            };
                            deliver(d.sink, reply);
                        }
                    }
                }
            }
            resp::Scan::Invalid(e) => return Err(e),
            resp::Scan::Incomplete => return Ok(()),
        }
    }
}

async fn run_conn(
    addr: &str,
    mut rx: mpsc::Receiver<Outbound>,
    readonly: bool,
    abortable: bool,
    cfg: &Config,
    conn: &Conn,
) {
    let setup = async {
        let stream = connect(addr, cfg.tcp_keepalive_secs)
            .await
            .map_err(|e| e.to_string())?;
        let (mut r, mut w) = stream.into_split();
        handshake(
            &mut r,
            &mut w,
            readonly,
            &cfg.backend_user,
            &cfg.backend_pass,
        )
        .await?;
        Ok::<_, String>((r, w))
    };
    let halves = tokio::select! {
        _ = conn.abort.notified(), if abortable => Err("aborted".to_string()),
        r = setup => r,
    };
    let (mut read_half, mut write_half) = match halves {
        Ok(h) => h,
        Err(e) => {
            log_debug!("connect {addr}: {e}");
            drain_channel(&mut rx);
            return;
        }
    };

    // one task owns both directions; a dead connection still drains its queue
    let mut pending: VecDeque<Pending<Sink>> = VecDeque::new();
    let mut front_err: Option<Bytes> = None;
    let mut batch: Vec<Outbound> = Vec::with_capacity(BATCH);
    let mut frames: Vec<Bytes> = Vec::with_capacity(BATCH * 2);
    let mut buf = BytesMut::with_capacity(READ_INIT);
    let mut tx_open = true;
    'io: loop {
        if let Err(e) = pair_replies(&mut buf, &mut pending, &mut front_err, deliver) {
            log_debug!("backend {addr} protocol error: {e}");
            break 'io;
        }
        if !tx_open && pending.is_empty() {
            break 'io;
        }
        ensure_read_room(&mut buf);
        tokio::select! {
            _ = conn.abort.notified(), if abortable => {
                break 'io;
            }
            n = rx.recv_many(&mut batch, BATCH), if tx_open => {
                if n == 0 {
                    tx_open = false;
                    if pending.is_empty() {
                        break 'io;
                    }
                    continue;
                }
                frames.clear();
                for out in batch.drain(..) {
                    pending.push_back(Pending {
                        expect: out.expect,
                        sink: out.sink,
                    });
                    if let Some(h) = out.head {
                        frames.push(h);
                    }
                    frames.push(out.frame);
                }
                let mut slices: Vec<IoSlice<'_>> = frames.iter().map(|f| IoSlice::new(f)).collect();
                let wrote = tokio::select! {
                    _ = conn.abort.notified(), if abortable => false,
                    r = write_slices(&mut write_half, &mut slices) => r.is_ok(),
                };
                if !wrote {
                    break 'io;
                }
            }
            r = read_half.read_buf(&mut buf) => {
                match r {
                    Ok(0) | Err(_) => break 'io,
                    Ok(_) => {}
                }
            }
        }
    }

    for p in pending.drain(..) {
        deliver(p.sink, Bytes::from_static(ERR_BACKEND_LOST));
    }
    drain_channel(&mut rx);
}

/// Grows a read buffer geometrically to READ_CHUNK; idle sessions stay small.
pub fn ensure_read_room(buf: &mut BytesMut) {
    if buf.capacity() - buf.len() < 2048 {
        buf.reserve(buf.capacity().clamp(READ_INIT, READ_CHUNK));
    }
}

fn deliver(sink: Sink, frame: Bytes) {
    match sink {
        Sink::Client(tx, seq) => {
            let _ = tx.send(Reply::At(seq, frame));
        }
        Sink::One(tx) => {
            let _ = tx.send(frame);
        }
    }
}

fn drain_channel(rx: &mut mpsc::Receiver<Outbound>) {
    rx.close();
    while let Ok(out) = rx.try_recv() {
        deliver(out.sink, Bytes::from_static(ERR_BACKEND_LOST));
    }
}

pub(crate) async fn connect(addr: &str, keepalive_secs: u64) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let sock = socket2::SockRef::from(&stream);
    let ka = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(keepalive_secs.max(1)));
    sock.set_tcp_keepalive(&ka)?;
    Ok(stream)
}

pub(crate) async fn handshake(
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
    readonly: bool,
    user: &str,
    pass: &str,
) -> Result<(), String> {
    let mut cmds: Vec<u8> = Vec::new();
    let mut expected = 0u32;
    if !pass.is_empty() {
        if user.is_empty() {
            resp::write_command(&mut cmds, &[b"AUTH", pass.as_bytes()]);
        } else {
            resp::write_command(&mut cmds, &[b"AUTH", user.as_bytes(), pass.as_bytes()]);
        }
        expected += 1;
    }
    if readonly {
        cmds.extend_from_slice(b"*1\r\n$8\r\nREADONLY\r\n");
        expected += 1;
    }
    if expected == 0 {
        return Ok(());
    }
    writer.write_all(&cmds).await.map_err(|e| e.to_string())?;
    let mut buf = BytesMut::with_capacity(1024);
    for _ in 0..expected {
        read_reply(reader, &mut buf).await?;
    }
    Ok(())
}

/// Reads one complete reply; an error reply or a closed stream is an Err.
pub(crate) async fn read_reply<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
) -> Result<Bytes, String> {
    loop {
        match resp::scan_value(buf) {
            resp::Scan::Complete(len) => {
                let frame = buf.split_to(len).freeze();
                if frame.first() == Some(&b'-') {
                    return Err(String::from_utf8_lossy(&frame).trim_end().to_string());
                }
                return Ok(frame);
            }
            resp::Scan::Invalid(e) => return Err(e.to_string()),
            resp::Scan::Incomplete => {}
        }
        let n = reader.read_buf(buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed before reply".to_string());
        }
    }
}

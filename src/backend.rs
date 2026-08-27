//! Backend connections: pipelined writer/reader task pairs per node, with
//! shared connections for regular traffic and exclusive ones for blocking
//! commands and pubsub relays.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::io::IoSlice;
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;
use crate::log_debug;
use crate::resp;

pub const OUTBOUND_QUEUE: usize = 8192;
pub const READ_CHUNK: usize = 64 * 1024;
pub const MAX_EXCLUSIVE_PER_NODE: usize = 512;

pub const ASKING_FRAME: &[u8] = b"*1\r\n$6\r\nASKING\r\n";
pub const ERR_BACKEND_LOST: &[u8] = b"-ERR mithril: backend connection lost\r\n";

/// Where a backend reply is delivered.
pub enum Sink {
    /// Ordered client reply stream: (sequence, frame).
    Client(mpsc::UnboundedSender<(u64, Bytes)>),
    /// Single reply for mergers and blocking commands.
    One(oneshot::Sender<Bytes>),
}

/// One pipelined request: optional prefix frame, payload, expected replies.
pub struct Outbound {
    pub head: Option<Bytes>,
    pub frame: Bytes,
    /// Number of backend replies this produces; only the last is delivered.
    pub expect: u32,
    pub seq: u64,
    pub sink: Sink,
}

struct Pending {
    expect: u32,
    seq: u64,
    sink: Sink,
}

/// A live backend connection; cheap to clone via Rc.
pub struct Conn {
    tx: mpsc::Sender<Outbound>,
    dead: Cell<bool>,
}

impl Conn {
    /// Queues a request; delivers an error frame to the sink if the
    /// connection is gone.
    pub async fn send(&self, out: Outbound) {
        if self.dead.get() {
            deliver(out.sink, out.seq, Bytes::from_static(ERR_BACKEND_LOST));
            return;
        }
        if let Err(e) = self.tx.send(out).await {
            let out = e.0;
            deliver(out.sink, out.seq, Bytes::from_static(ERR_BACKEND_LOST));
        }
    }

    pub fn is_dead(&self) -> bool {
        self.dead.get()
    }
}

/// Per-worker backend pools keyed by node address.
pub struct Backends {
    cfg: Rc<Config>,
    pools: RefCell<HashMap<Box<str>, Rc<Pool>>>,
}

struct Pool {
    shared: RefCell<Vec<Rc<Conn>>>,
    idle_exclusive: RefCell<Vec<Rc<Conn>>>,
    exclusive_count: Cell<usize>,
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
        let pool = self.pool(addr);
        let want = self.cfg.backend_conns;
        let idx = (sticky % want as u64) as usize;
        let mut conns = pool.shared.borrow_mut();
        while conns.len() <= idx {
            conns.push(self.dial(addr, readonly));
        }
        if conns[idx].is_dead() {
            conns[idx] = self.dial(addr, readonly);
        }
        conns[idx].clone()
    }

    /// Takes an exclusive connection for blocking commands or pubsub.
    pub fn take_exclusive(self: &Rc<Self>, addr: &str, readonly: bool) -> Option<Rc<Conn>> {
        let pool = self.pool(addr);
        loop {
            let conn = pool.idle_exclusive.borrow_mut().pop();
            match conn {
                Some(c) if !c.is_dead() => return Some(c),
                Some(_) => pool.exclusive_count.set(pool.exclusive_count.get() - 1),
                None => break,
            }
        }
        if pool.exclusive_count.get() >= MAX_EXCLUSIVE_PER_NODE {
            return None;
        }
        pool.exclusive_count.set(pool.exclusive_count.get() + 1);
        Some(self.dial(addr, readonly))
    }

    /// Returns an exclusive connection to the idle pool.
    pub fn put_exclusive(&self, addr: &str, conn: Rc<Conn>) {
        let pool = self.pool(addr);
        if conn.is_dead() {
            pool.exclusive_count
                .set(pool.exclusive_count.get().saturating_sub(1));
            return;
        }
        pool.idle_exclusive.borrow_mut().push(conn);
    }

    fn pool(&self, addr: &str) -> Rc<Pool> {
        if let Some(p) = self.pools.borrow().get(addr) {
            return p.clone();
        }
        let pool = Rc::new(Pool {
            shared: RefCell::new(Vec::new()),
            idle_exclusive: RefCell::new(Vec::new()),
            exclusive_count: Cell::new(0),
        });
        self.pools.borrow_mut().insert(addr.into(), pool.clone());
        pool
    }

    fn dial(&self, addr: &str, readonly: bool) -> Rc<Conn> {
        let (tx, rx) = mpsc::channel(OUTBOUND_QUEUE);
        let conn = Rc::new(Conn {
            tx,
            dead: Cell::new(false),
        });
        let task_conn = conn.clone();
        let addr = addr.to_string();
        let user = self.cfg.backend_user.clone();
        let pass = self.cfg.backend_pass.clone();
        let keepalive = self.cfg.tcp_keepalive_secs;
        tokio::task::spawn_local(async move {
            run_conn(&addr, rx, readonly, &user, &pass, keepalive).await;
            task_conn.dead.set(true);
        });
        conn
    }
}

async fn run_conn(
    addr: &str,
    mut rx: mpsc::Receiver<Outbound>,
    readonly: bool,
    user: &str,
    pass: &str,
    keepalive_secs: u64,
) {
    let stream = match connect(addr, keepalive_secs).await {
        Ok(s) => s,
        Err(e) => {
            log_debug!("dial {addr}: {e}");
            drain_channel(&mut rx);
            return;
        }
    };
    let (mut read_half, mut write_half) = stream.into_split();
    if let Err(e) = handshake(&mut read_half, &mut write_half, readonly, user, pass).await {
        log_debug!("handshake {addr}: {e}");
        drain_channel(&mut rx);
        return;
    }

    let pending: Rc<RefCell<VecDeque<Pending>>> = Rc::new(RefCell::new(VecDeque::new()));
    let write_pending = pending.clone();
    let writer = tokio::task::spawn_local(async move {
        let mut batch: Vec<Outbound> = Vec::with_capacity(64);
        let mut frames: Vec<Bytes> = Vec::with_capacity(128);
        loop {
            let n = rx.recv_many(&mut batch, 64).await;
            if n == 0 {
                return;
            }
            frames.clear();
            {
                let mut p = write_pending.borrow_mut();
                for out in batch.drain(..) {
                    p.push_back(Pending {
                        expect: out.expect,
                        seq: out.seq,
                        sink: out.sink,
                    });
                    if let Some(h) = out.head {
                        frames.push(h);
                    }
                    frames.push(out.frame);
                }
            }
            let mut slices: Vec<IoSlice<'_>> = frames.iter().map(|f| IoSlice::new(f)).collect();
            if write_vectored_all(&mut write_half, &mut slices)
                .await
                .is_err()
            {
                return;
            }
        }
    });

    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    loop {
        match resp::scan_value(&buf) {
            resp::Scan::Complete(len) => {
                let frame = buf.split_to(len).freeze();
                let done = {
                    let mut p = pending.borrow_mut();
                    match p.front_mut() {
                        Some(front) if front.expect > 1 => {
                            front.expect -= 1;
                            None
                        }
                        Some(_) => p.pop_front(),
                        None => None,
                    }
                };
                if let Some(d) = done {
                    deliver(d.sink, d.seq, frame);
                }
                continue;
            }
            resp::Scan::Invalid(e) => {
                log_debug!("backend {addr} protocol error: {e}");
                break;
            }
            resp::Scan::Incomplete => {}
        }
        match read_half.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }

    writer.abort();
    for p in pending.borrow_mut().drain(..) {
        deliver(p.sink, p.seq, Bytes::from_static(ERR_BACKEND_LOST));
    }
}

fn deliver(sink: Sink, seq: u64, frame: Bytes) {
    match sink {
        Sink::Client(tx) => {
            let _ = tx.send((seq, frame));
        }
        Sink::One(tx) => {
            let _ = tx.send(frame);
        }
    }
}

fn drain_channel(rx: &mut mpsc::Receiver<Outbound>) {
    rx.close();
    while let Ok(out) = rx.try_recv() {
        deliver(out.sink, out.seq, Bytes::from_static(ERR_BACKEND_LOST));
    }
}

/// Dials a raw backend connection with auth applied; used by pubsub relays
/// and the topology refresher.
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

async fn connect(addr: &str, keepalive_secs: u64) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let sock = socket2::SockRef::from(&stream);
    let ka = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(keepalive_secs.max(1)));
    sock.set_tcp_keepalive(&ka)?;
    Ok(stream)
}

async fn handshake(
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
    while expected > 0 {
        match resp::scan_value(&buf) {
            resp::Scan::Complete(len) => {
                let frame = buf.split_to(len);
                if frame.first() == Some(&b'-') {
                    return Err(String::from_utf8_lossy(&frame).trim_end().to_string());
                }
                expected -= 1;
                continue;
            }
            resp::Scan::Invalid(e) => return Err(e.to_string()),
            resp::Scan::Incomplete => {}
        }
        let n = reader.read_buf(&mut buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed during handshake".to_string());
        }
    }
    Ok(())
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

async fn write_vectored_all(
    w: &mut OwnedWriteHalf,
    slices: &mut [IoSlice<'_>],
) -> std::io::Result<()> {
    write_slices(w, slices).await
}

//! Worker startup: one acceptor round-robins connections onto per-core
//! runtimes, plus the topology refresher and shutdown.

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::backend::Backends;
use crate::client::{Shared, serve};
use crate::config::Config;
use crate::stats::Stats;
use crate::topology::Topology;
use crate::{log_notice, log_warn, resp};

pub const REFRESH_DEBOUNCE: Duration = Duration::from_millis(100);
pub const BOOTSTRAP_RETRY: Duration = Duration::from_secs(1);
pub const BOOTSTRAP_ROUNDS: usize = 30;
pub const LISTEN_BACKLOG: i32 = 1024;
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(5);
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const ACCEPT_POLL: Duration = Duration::from_millis(200);
const ACCEPT_QUEUE: usize = 1024;
const DRAIN_POLL: Duration = Duration::from_millis(50);

static TOPO_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Runs the proxy until SIGINT/SIGTERM; Err on fatal startup failure.
pub fn run(cfg: Config) -> Result<(), String> {
    crate::log::set_level(cfg.loglevel);
    let cfg = Arc::new(cfg);
    let stats = Stats::new(cfg.workers);
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let boot_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime: {e}"))?;
    let mut topo = boot_rt.block_on(bootstrap(&cfg))?;
    topo.epoch = next_epoch();
    log_notice!(
        "topology bootstrapped: {} nodes, {} masters",
        topo.nodes.len(),
        topo.masters.len()
    );
    let topo = Arc::new(ArcSwap::from_pointee(topo));

    let (refresh_tx, refresh_rx) = mpsc::unbounded_channel();
    let refresher_cfg = cfg.clone();
    let refresher_topo = topo.clone();
    std::thread::Builder::new()
        .name("mithril-topo".to_string())
        .spawn(move || refresher_thread(refresher_cfg, refresher_topo, refresh_rx))
        .map_err(|e| format!("spawn refresher: {e}"))?;

    let listener = bind_listener(&cfg.bind, cfg.port)
        .map_err(|e| format!("bind {}:{}: {e}", cfg.bind, cfg.port))?;
    let mut handles = Vec::with_capacity(cfg.workers);
    let mut conn_txs = Vec::with_capacity(cfg.workers);
    for worker in 0..cfg.workers {
        let (conn_tx, conn_rx) = mpsc::channel::<std::net::TcpStream>(ACCEPT_QUEUE);
        conn_txs.push(conn_tx);
        let cfg = cfg.clone();
        let topo = topo.clone();
        let stats = stats.clone();
        let refresh = refresh_tx.clone();
        let handle = std::thread::Builder::new()
            .name(format!("mithril-{worker}"))
            .spawn(move || worker_thread(cfg, topo, stats, refresh, worker, started, conn_rx))
            .map_err(|e| format!("spawn worker: {e}"))?;
        handles.push(handle);
    }
    let acceptor_cfg = cfg.clone();
    let acceptor_stats = stats.clone();
    std::thread::Builder::new()
        .name("mithril-accept".to_string())
        .spawn(move || acceptor_thread(listener, acceptor_cfg, acceptor_stats, conn_txs))
        .map_err(|e| format!("spawn acceptor: {e}"))?;

    wait_for_signal();
    log_notice!("shutting down: draining clients");
    SHUTTING_DOWN.store(true, Ordering::Relaxed);
    let deadline = std::time::Instant::now() + DRAIN_TIMEOUT;
    while stats.clients.load(Ordering::Relaxed) > 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(DRAIN_POLL);
    }
    let left = stats.clients.load(Ordering::Relaxed);
    if left > 0 {
        log_warn!("drain timeout with {left} clients still connected");
    }
    std::process::exit(0);
}

fn next_epoch() -> u64 {
    TOPO_EPOCH.fetch_add(1, Ordering::Relaxed) + 1
}

/// Set on SIGINT/SIGTERM; accept loops stop taking new connections.
pub static SHUTTING_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn wait_for_signal() {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => loop {
            std::thread::park();
        },
    };
    rt.block_on(async {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        match term.as_mut() {
            Some(term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            None => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    });
}

// kernel reuseport hashing skews connections across workers; a single
// acceptor round-robins them so no worker becomes the latency floor.
fn acceptor_thread(
    listener: std::net::TcpListener,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
    conn_txs: Vec<mpsc::Sender<std::net::TcpStream>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            log_warn!("acceptor: runtime: {e}");
            return;
        }
    };
    rt.block_on(async move {
        let listener = match TcpListener::from_std(listener) {
            Ok(l) => l,
            Err(e) => {
                log_warn!("acceptor: listener: {e}");
                return;
            }
        };
        let mut next = 0usize;
        loop {
            let accepted = tokio::select! {
                a = listener.accept() => a,
                _ = tokio::time::sleep(ACCEPT_POLL) => {
                    if SHUTTING_DOWN.load(Ordering::Relaxed) {
                        return;
                    }
                    continue;
                }
            };
            if SHUTTING_DOWN.load(Ordering::Relaxed) {
                return;
            }
            let (stream, _) = match accepted {
                Ok(v) => v,
                Err(_) => continue,
            };
            if stats.clients.fetch_add(1, Ordering::Relaxed) >= cfg.maxclients {
                stats.clients.fetch_sub(1, Ordering::Relaxed);
                reject_maxclients(stream);
                continue;
            }
            stats.total_connections.fetch_add(1, Ordering::Relaxed);
            let Ok(std_stream) = stream.into_std() else {
                stats.clients.fetch_sub(1, Ordering::Relaxed);
                continue;
            };
            // a full queue must not stall accepts for the rest
            let best = next;
            next = (next + 1) % conn_txs.len();
            let mut pending = Some(std_stream);
            for k in 0..conn_txs.len() {
                let Some(s) = pending.take() else { break };
                let i = (best + k) % conn_txs.len();
                match conn_txs[i].try_send(s) {
                    Ok(()) => {}
                    Err(e) => pending = Some(e.into_inner()),
                }
            }
            if let Some(s) = pending
                && conn_txs[best].send(s).await.is_err()
            {
                stats.clients.fetch_sub(1, Ordering::Relaxed);
            }
        }
    });
}

fn worker_thread(
    cfg: Arc<Config>,
    topo: Arc<ArcSwap<Topology>>,
    stats: Arc<Stats>,
    refresh: mpsc::UnboundedSender<()>,
    worker: usize,
    started: u64,
    mut conn_rx: mpsc::Receiver<std::net::TcpStream>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            log_warn!("worker {worker}: runtime: {e}");
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let local_cfg = Rc::new((*cfg).clone());
        let shared = Rc::new(Shared {
            cfg: local_cfg.clone(),
            topo,
            backends: Backends::new(local_cfg),
            stats: stats.clone(),
            worker,
            refresh,
            started,
        });
        let mut next_client: u64 = worker as u64;
        while let Some(std_stream) = conn_rx.recv().await {
            let Ok(stream) = TcpStream::from_std(std_stream) else {
                stats.clients.fetch_sub(1, Ordering::Relaxed);
                continue;
            };
            let shared = shared.clone();
            let id = next_client;
            next_client += shared.cfg.workers as u64;
            let session_stats = stats.clone();
            tokio::task::spawn_local(async move {
                serve(shared, stream, id).await;
                session_stats.clients.fetch_sub(1, Ordering::Relaxed);
            });
        }
        // channel closed: acceptor is gone; keep serving until drained
        let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
        while stats.clients.load(Ordering::Relaxed) > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(DRAIN_POLL).await;
        }
    });
}

fn reject_maxclients(stream: TcpStream) {
    tokio::spawn(async move {
        let mut stream = stream;
        let _ = stream
            .write_all(b"-ERR max number of clients reached\r\n")
            .await;
    });
}

fn bind_listener(bind: &str, port: u16) -> std::io::Result<std::net::TcpListener> {
    let addr: SocketAddr = format!("{bind}:{port}")
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}")))?;
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(LISTEN_BACKLOG)?;
    Ok(socket.into())
}

fn refresher_thread(
    cfg: Arc<Config>,
    topo: Arc<ArcSwap<Topology>>,
    mut notify: mpsc::UnboundedReceiver<()>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            log_warn!("refresher: runtime: {e}");
            return;
        }
    };
    rt.block_on(async move {
        let period = Duration::from_secs(cfg.topology_refresh_secs);
        loop {
            let notified = tokio::time::timeout(period, notify.recv()).await;
            if matches!(notified, Ok(None)) {
                return;
            }
            if notified.is_ok() {
                tokio::time::sleep(REFRESH_DEBOUNCE).await;
                while notify.try_recv().is_ok() {}
            }
            let seeds: Vec<String> = {
                let current = topo.load();
                current.nodes.iter().map(|n| n.addr.clone()).collect()
            };
            match fetch_topology(&cfg, seeds.iter().map(String::as_str)).await {
                Ok(mut new_topo) => {
                    new_topo.epoch = next_epoch();
                    topo.store(Arc::new(new_topo));
                }
                Err(e) => log_warn!("topology refresh failed: {e}"),
            }
        }
    });
}

async fn bootstrap(cfg: &Config) -> Result<Topology, String> {
    for round in 0..BOOTSTRAP_ROUNDS {
        match fetch_topology(cfg, cfg.bootstrap.iter().map(String::as_str)).await {
            Ok(t) => return Ok(t),
            Err(e) => {
                log_warn!("bootstrap round {round}: {e}");
                tokio::time::sleep(BOOTSTRAP_RETRY).await;
            }
        }
    }
    Err(format!("bootstrap failed after {BOOTSTRAP_ROUNDS} rounds"))
}

async fn fetch_topology<'a, I: Iterator<Item = &'a str>>(
    cfg: &Config,
    seeds: I,
) -> Result<Topology, String> {
    let mut last_err = "no seed addresses".to_string();
    for seed in seeds {
        let fetched = tokio::time::timeout(FETCH_TIMEOUT, fetch_from(cfg, seed))
            .await
            .unwrap_or_else(|_| Err("timed out".to_string()));
        match fetched {
            Ok(t) => return Ok(t),
            Err(e) => last_err = format!("{seed}: {e}"),
        }
    }
    Err(last_err)
}

async fn fetch_from(cfg: &Config, addr: &str) -> Result<Topology, String> {
    let mut stream = tokio::time::timeout(FETCH_TIMEOUT, crate::backend::dial_raw(addr, cfg))
        .await
        .map_err(|_| "dial timed out".to_string())?
        .map_err(|e| e.to_string())?;
    stream
        .write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = BytesMut::with_capacity(crate::backend::READ_CHUNK);
    loop {
        match resp::scan_value(&buf) {
            resp::Scan::Complete(len) => {
                let frame = buf.split_to(len);
                if frame.first() == Some(&b'-') {
                    return Err(String::from_utf8_lossy(&frame).trim_end().to_string());
                }
                let text = bulk_text(&frame).ok_or("unexpected CLUSTER NODES reply")?;
                return Topology::parse(text);
            }
            resp::Scan::Invalid(e) => return Err(e.to_string()),
            resp::Scan::Incomplete => {}
        }
        let n = stream.read_buf(&mut buf).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed while reading CLUSTER NODES".to_string());
        }
    }
}

fn bulk_text(frame: &[u8]) -> Option<&str> {
    std::str::from_utf8(resp::bulk_payload(frame)?).ok()
}

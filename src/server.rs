//! Acceptor, per-core worker runtimes, topology refresher, and shutdown.

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
use crate::config::{Config, Placement};
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
// worker-activity snapshot cadence for least-loaded placement
const SNAP_WINDOW: Duration = Duration::from_millis(10);
// below this many commands per window the proxy is idle: place by rotation
const QUIET_FLOOR: u64 = 128;
const DRAIN_POLL: Duration = Duration::from_millis(50);

static TOPO_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Set on SIGINT/SIGTERM; accept loops stop taking new connections.
pub static SHUTTING_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

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
    let mut conn_txs = Vec::with_capacity(cfg.workers);
    for worker in 0..cfg.workers {
        let (conn_tx, conn_rx) = mpsc::channel::<Admitted>(ACCEPT_QUEUE);
        conn_txs.push(conn_tx);
        let cfg = cfg.clone();
        let topo = topo.clone();
        let stats = stats.clone();
        let refresh = refresh_tx.clone();
        std::thread::Builder::new()
            .name(format!("mithril-{worker}"))
            .spawn(move || worker_thread(cfg, topo, stats, refresh, worker, started, conn_rx))
            .map_err(|e| format!("spawn worker: {e}"))?;
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

fn current_thread_rt(name: &str) -> Option<tokio::runtime::Runtime> {
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => Some(rt),
        Err(e) => {
            log_warn!("{name}: runtime: {e}");
            None
        }
    }
}

fn wait_for_signal() {
    let Some(rt) = current_thread_rt("signal") else {
        loop {
            std::thread::park();
        }
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

/// Admission ticket holding one maxclients slot; drop returns the slot.
struct Admitted {
    stream: Option<std::net::TcpStream>,
    stats: Arc<Stats>,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.stats.clients.fetch_sub(1, Ordering::Relaxed);
    }
}

// kernel reuseport hashing skews connections binomially; one acceptor round-robins
fn acceptor_thread(
    listener: std::net::TcpListener,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
    conn_txs: Vec<mpsc::Sender<Admitted>>,
) {
    let Some(rt) = current_thread_rt("acceptor") else {
        return;
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
        let mut cmd_snap = vec![0u64; conn_txs.len()];
        let mut cmd_rate = vec![0u64; conn_txs.len()];
        let mut placed = vec![0u64; conn_txs.len()];
        let mut order: Vec<usize> = Vec::with_capacity(conn_txs.len());
        let mut snap_at = tokio::time::Instant::now();
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
            let mut admitted = Admitted {
                stream: stream.into_std().ok(),
                stats: stats.clone(),
            };
            if admitted.stream.is_none() {
                continue;
            }
            // placement-key order per worker: round-robin keeps rotation;
            // least-loaded keys lexicographically on (in-window placement
            // imbalance, activity bucket, rotation), so a connect burst is
            // forced to spread evenly, sparse arrivals steer to the
            // least-active worker, a near-idle proxy never tiers on
            // stray-command noise, and a full queue falls back to the
            // next-best key instead of a cyclic neighbor
            // a full queue must not stall accepts for the rest
            'place: loop {
                if cfg.placement == Placement::LeastLoaded {
                    let now = tokio::time::Instant::now();
                    let elapsed = now.duration_since(snap_at);
                    if elapsed >= SNAP_WINDOW {
                        snap_at = now;
                        // normalize to per-window units: a long gap since the
                        // last accept must not read as current activity
                        let scale =
                            (elapsed.as_millis() as u64 / SNAP_WINDOW.as_millis() as u64).max(1);
                        for i in 0..conn_txs.len() {
                            let c = stats.workers[i].commands.load(Ordering::Relaxed);
                            cmd_rate[i] = (c - cmd_snap[i]) / scale;
                            cmd_snap[i] = c;
                            placed[i] = 0;
                        }
                    }
                }
                order.clear();
                order.extend(0..conn_txs.len());
                if cfg.placement == Placement::LeastLoaded {
                    let total: u64 = cmd_rate.iter().sum();
                    let share = (total / conn_txs.len() as u64).max(1);
                    let quiet = total < QUIET_FLOOR;
                    let floor = placed.iter().min().copied().unwrap_or(0);
                    order.sort_by_key(|&i| {
                        let bucket = if quiet { 0 } else { cmd_rate[i] / share };
                        let rot = (i + conn_txs.len() - next) % conn_txs.len();
                        (placed[i] - floor, bucket, rot)
                    });
                } else {
                    order.rotate_left(next);
                }
                for &i in &order {
                    match conn_txs[i].try_send(admitted) {
                        Ok(()) => {
                            placed[i] += 1;
                            next = (i + 1) % conn_txs.len();
                            break 'place;
                        }
                        Err(e) => admitted = e.into_inner(),
                    }
                }
                if SHUTTING_DOWN.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(DRAIN_POLL).await;
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
    mut conn_rx: mpsc::Receiver<Admitted>,
) {
    let Some(rt) = current_thread_rt("worker") else {
        return;
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
        while let Some(mut admitted) = conn_rx.recv().await {
            let stream = admitted
                .stream
                .take()
                .and_then(|s| TcpStream::from_std(s).ok());
            let Some(stream) = stream else {
                continue;
            };
            let shared = shared.clone();
            let id = next_client;
            next_client += shared.cfg.workers as u64;
            tokio::task::spawn_local(async move {
                serve(shared, stream, id).await;
                drop(admitted);
            });
        }
        // channel closed: keep the LocalSet alive so open sessions drain
        tokio::time::sleep(DRAIN_TIMEOUT).await;
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
    let Some(rt) = current_thread_rt("refresher") else {
        return;
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

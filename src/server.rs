//! Worker startup: SO_REUSEPORT listeners, per-core runtimes, the topology
//! refresher, and shutdown.

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

/// Runs the proxy until SIGINT/SIGTERM; returns an error message on fatal
/// startup failure.
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
    let topo = boot_rt.block_on(bootstrap(&cfg))?;
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

    let mut handles = Vec::with_capacity(cfg.workers);
    for worker in 0..cfg.workers {
        let cfg = cfg.clone();
        let topo = topo.clone();
        let stats = stats.clone();
        let refresh = refresh_tx.clone();
        let handle = std::thread::Builder::new()
            .name(format!("mithril-{worker}"))
            .spawn(move || worker_thread(cfg, topo, stats, refresh, worker, started))
            .map_err(|e| format!("spawn worker: {e}"))?;
        handles.push(handle);
    }

    wait_for_signal();
    log_notice!("shutting down");
    std::process::exit(0);
}

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

fn worker_thread(
    cfg: Arc<Config>,
    topo: Arc<ArcSwap<Topology>>,
    stats: Arc<Stats>,
    refresh: mpsc::UnboundedSender<()>,
    worker: usize,
    started: u64,
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
        let listener = match bind_reuseport(&cfg.bind, cfg.port) {
            Ok(l) => l,
            Err(e) => {
                log_warn!("worker {worker}: bind {}:{}: {e}", cfg.bind, cfg.port);
                return;
            }
        };
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
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => continue,
            };
            if stats.clients.load(Ordering::Relaxed) >= shared.cfg.maxclients {
                reject_maxclients(stream);
                continue;
            }
            stats.clients.fetch_add(1, Ordering::Relaxed);
            stats.total_connections.fetch_add(1, Ordering::Relaxed);
            let shared = shared.clone();
            let id = next_client;
            next_client += shared.cfg.workers as u64;
            let session_stats = stats.clone();
            tokio::task::spawn_local(async move {
                serve(shared, stream, id).await;
                session_stats.clients.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });
}

fn reject_maxclients(stream: TcpStream) {
    tokio::task::spawn_local(async move {
        let mut stream = stream;
        let _ = stream
            .write_all(b"-ERR max number of clients reached\r\n")
            .await;
    });
}

fn bind_reuseport(bind: &str, port: u16) -> std::io::Result<TcpListener> {
    let addr: SocketAddr = format!("{bind}:{port}")
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}")))?;
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    TcpListener::from_std(socket.into())
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
                Ok(new_topo) => topo.store(Arc::new(new_topo)),
                Err(e) => log_warn!("topology refresh failed: {e}"),
            }
        }
    });
}

async fn bootstrap(cfg: &Config) -> Result<Topology, String> {
    for round in 0..30 {
        match fetch_topology(cfg, cfg.bootstrap.iter().map(String::as_str)).await {
            Ok(t) => return Ok(t),
            Err(e) => {
                log_warn!("bootstrap round {round}: {e}");
                tokio::time::sleep(BOOTSTRAP_RETRY).await;
            }
        }
    }
    Err("bootstrap failed after 30 rounds".to_string())
}

async fn fetch_topology<'a, I: Iterator<Item = &'a str>>(
    cfg: &Config,
    seeds: I,
) -> Result<Topology, String> {
    let mut last_err = "no seed addresses".to_string();
    for seed in seeds {
        match fetch_from(cfg, seed).await {
            Ok(t) => return Ok(t),
            Err(e) => last_err = format!("{seed}: {e}"),
        }
    }
    Err(last_err)
}

async fn fetch_from(cfg: &Config, addr: &str) -> Result<Topology, String> {
    let mut stream = crate::backend::dial_raw(addr, cfg)
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = BytesMut::with_capacity(64 * 1024);
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
    if frame.first() != Some(&b'$') {
        return None;
    }
    let start = frame.iter().position(|&b| b == b'\n')? + 1;
    let payload = frame.get(start..frame.len().checked_sub(2)?)?;
    std::str::from_utf8(payload).ok()
}

//! Acceptor, per-core worker runtimes, topology refresher, and shutdown.

use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::backend::Backends;
use crate::client::{Shared, auto_tuner, serve};
use crate::config::{Config, Placement, Sharding};
use crate::shard;
use crate::stats::Stats;
use crate::topology::Topology;
use crate::{log_notice, log_warn, resp};

const REFRESH_DEBOUNCE: Duration = Duration::from_millis(100);
const BOOTSTRAP_RETRY: Duration = Duration::from_secs(1);
const BOOTSTRAP_ROUNDS: usize = 30;
const LISTEN_BACKLOG: i32 = 1024;
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const ACCEPT_POLL: Duration = Duration::from_millis(200);
const ACCEPT_QUEUE: usize = 1024;
// worker-activity snapshot cadence for least-loaded placement
const SNAP_WINDOW: Duration = Duration::from_millis(10);
// below this many commands per window the proxy is idle: place by rotation
const QUIET_FLOOR: u64 = 128;
const DRAIN_POLL: Duration = Duration::from_millis(50);

// invalidation batches a worker may fall behind before its cache flushes
const INVAL_QUEUE: usize = 4096;

static TOPO_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Set on SIGINT/SIGTERM; accept loops stop taking new connections.
static SHUTTING_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

type ShardWiring = (
    Arc<shard::Fabric>,
    mpsc::UnboundedReceiver<shard::NewConn>,
    mpsc::Receiver<shard::Invalidations>,
);

/// Admission ticket holding one maxclients slot; drop returns the slot.
struct Admitted {
    stream: Option<std::net::TcpStream>,
    peer: SocketAddr,
    stats: Arc<Stats>,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.stats.clients.fetch_sub(1, Ordering::Relaxed);
    }
}

struct WorkerCtx {
    cfg: Arc<Config>,
    topo: Arc<ArcSwap<Topology>>,
    stats: Arc<Stats>,
    refresh: mpsc::UnboundedSender<()>,
    worker: usize,
    started: u64,
    coverage: Option<Arc<crate::cache::Coverage>>,
}

struct Placer {
    snap: Vec<u64>,
    buckets: Vec<u64>,
    placed: Vec<u64>,
    order: Vec<usize>,
    next: usize,
    least_loaded: bool,
}

impl Placer {
    fn new(workers: usize, least_loaded: bool) -> Placer {
        Placer {
            snap: vec![0; workers],
            buckets: vec![0; workers],
            placed: vec![0; workers],
            order: Vec::with_capacity(workers),
            next: 0,
            least_loaded,
        }
    }

    // the full-queue fallback order
    fn rank(&mut self) {
        self.order.clear();
        self.order.extend(0..self.snap.len());
        self.order.rotate_left(self.next);
        if self.least_loaded {
            let (placed, buckets) = (&self.placed, &self.buckets);
            self.order.sort_by_key(|&i| (placed[i], buckets[i]));
        }
    }

    fn refresh(&mut self, stats: &Stats) {
        let mut total = 0u64;
        for i in 0..self.snap.len() {
            let c = stats.workers[i].commands.load(Ordering::Relaxed);
            self.buckets[i] = c - self.snap[i];
            self.snap[i] = c;
            self.placed[i] = 0;
            total += self.buckets[i];
        }
        if total < QUIET_FLOOR {
            self.buckets.fill(0);
            return;
        }
        let share = (total / self.snap.len() as u64).max(1);
        for b in self.buckets.iter_mut() {
            *b /= share;
        }
    }

    // the least (placed, bucket) key; rotation order breaks ties
    fn best(&self) -> usize {
        let n = self.snap.len();
        let mut best = self.next;
        if self.least_loaded {
            let key = |i: usize| (self.placed[i], self.buckets[i]);
            for k in 1..n {
                let i = (self.next + k) % n;
                if key(i) < key(best) {
                    best = i;
                }
            }
        }
        best
    }

    fn placed(&mut self, i: usize) {
        self.placed[i] += 1;
        self.next = (i + 1) % self.snap.len();
    }
}

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
    topo.epoch = 1;
    log_notice!(
        "topology bootstrapped: {} nodes, {} masters",
        topo.nodes.len(),
        topo.masters.len()
    );
    let topo = Arc::new(ArcSwap::from_pointee(topo));
    TOPO_EPOCH.store(1, Ordering::Release);

    let (refresh_tx, refresh_rx) = mpsc::unbounded_channel();
    let refresher_cfg = cfg.clone();
    let refresher_topo = topo.clone();
    std::thread::Builder::new()
        .name("mithril-topo".to_string())
        .spawn(move || refresher_thread(refresher_cfg, refresher_topo, refresh_rx))
        .map_err(|e| format!("spawn refresher: {e}"))?;

    let listener = bind_listener(&cfg.bind, cfg.port)
        .map_err(|e| format!("bind {}:{}: {e}", cfg.bind, cfg.port))?;
    let mut shard_parts = if cfg.backend_sharding != Sharding::Off {
        let mut ctl_txs = Vec::with_capacity(cfg.workers);
        let mut ctl_rxs = Vec::with_capacity(cfg.workers);
        for _ in 0..cfg.workers {
            let (ct, cr) = mpsc::unbounded_channel();
            let (it, ir) = mpsc::channel(INVAL_QUEUE);
            ctl_txs.push(shard::Controls {
                conns: ct,
                invals: it,
            });
            ctl_rxs.push((cr, ir));
        }
        Some((shard::Fabric::new(ctl_txs), ctl_rxs.into_iter()))
    } else {
        None
    };
    let coverage = (cfg.reply_cache && cfg.backend_sharding != Sharding::Off)
        .then(crate::cache::Coverage::new);
    let mut conn_txs = Vec::with_capacity(cfg.workers);
    for worker in 0..cfg.workers {
        let (conn_tx, conn_rx) = mpsc::channel::<Admitted>(ACCEPT_QUEUE);
        conn_txs.push(conn_tx);
        let cfg = cfg.clone();
        let topo = topo.clone();
        let stats = stats.clone();
        let refresh = refresh_tx.clone();
        let shard = shard_parts.as_mut().and_then(|(f, crs)| {
            let (cr, ir) = crs.next()?;
            Some((f.clone(), cr, ir))
        });
        let ctx = WorkerCtx {
            cfg,
            topo,
            stats,
            refresh,
            worker,
            started,
            coverage: coverage.clone(),
        };
        std::thread::Builder::new()
            .name(format!("mithril-{worker}"))
            .spawn(move || worker_thread(ctx, conn_rx, shard))
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

/// Latest published topology epoch; sessions compare before reloading their cache.
pub fn topo_epoch() -> u64 {
    TOPO_EPOCH.load(Ordering::Acquire)
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

// kernel reuseport hashing skews connections binomially; one acceptor places
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
        let least_loaded = cfg.placement == Placement::LeastLoaded;
        let mut placer = Placer::new(conn_txs.len(), least_loaded);
        let period = if least_loaded {
            SNAP_WINDOW
        } else {
            ACCEPT_POLL
        };
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let accepted = tokio::select! {
                a = listener.accept() => Some(a),
                _ = tick.tick() => {
                    if least_loaded {
                        placer.refresh(&stats);
                    }
                    None
                }
            };
            if SHUTTING_DOWN.load(Ordering::Relaxed) {
                return;
            }
            let Some(accepted) = accepted else {
                continue;
            };
            let (stream, peer) = match accepted {
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
                peer,
                stats: stats.clone(),
            };
            if admitted.stream.is_none() {
                continue;
            }
            // a full queue falls to the next-best key; accepts wait only when every queue is full
            'place: loop {
                let best = placer.best();
                match conn_txs[best].try_send(admitted) {
                    Ok(()) => {
                        placer.placed(best);
                        break 'place;
                    }
                    Err(e) => admitted = e.into_inner(),
                }
                placer.rank();
                for k in 0..conn_txs.len() {
                    let i = placer.order[k];
                    if i == best {
                        continue;
                    }
                    match conn_txs[i].try_send(admitted) {
                        Ok(()) => {
                            placer.placed(i);
                            break 'place;
                        }
                        Err(e) => admitted = e.into_inner(),
                    }
                }
                if SHUTTING_DOWN.load(Ordering::Relaxed) {
                    break;
                }
                if least_loaded {
                    tick.tick().await;
                    placer.refresh(&stats);
                } else {
                    tokio::time::sleep(DRAIN_POLL).await;
                }
            }
        }
    });
}

fn worker_thread(
    ctx: WorkerCtx,
    mut conn_rx: mpsc::Receiver<Admitted>,
    shard: Option<ShardWiring>,
) {
    let WorkerCtx {
        cfg,
        topo,
        stats,
        refresh,
        worker,
        started,
        coverage,
    } = ctx;
    let Some(rt) = current_thread_rt("worker") else {
        return;
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let local_cfg = Rc::new((*cfg).clone());
        let cache = cfg
            .reply_cache
            .then(|| crate::cache::ReplyCache::new(&cfg, stats.clone(), worker, coverage));
        let tracking = cache.as_ref().map(|c| c.tracking_frames());
        let fabric = shard.map(|(fabric, ctl_rx, inval_rx)| {
            tokio::task::spawn_local(shard::control_loop(
                ctl_rx,
                inval_rx,
                fabric.clone(),
                cfg.clone(),
                cache.clone(),
                tracking.clone(),
            ));
            fabric
        });
        let backends = Backends::new(local_cfg.clone(), tracking);
        if let Some(cache) = &cache {
            let wiring = Rc::new(crate::cache::Wiring {
                cache: cache.clone(),
                backends: backends.clone(),
                fabric: fabric.clone(),
                cfg: local_cfg.clone(),
            });
            tokio::task::spawn_local(crate::cache::run_trackers(wiring, topo.clone()));
        }
        let shared = Rc::new(Shared {
            cfg: local_cfg,
            topo,
            backends,
            wstats: stats.workers[worker].clone(),
            stats: stats.clone(),
            refresh,
            started,
            fabric,
            cache,
            inflight: Cell::new(0),
            prefer_shared: Cell::new(false),
        });
        if shared.cfg.backend_sharding == Sharding::Auto {
            tokio::task::spawn_local(auto_tuner(shared.clone()));
        }
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
            let peer = admitted.peer;
            tokio::task::spawn_local(async move {
                serve(shared, stream, peer, id).await;
                // the maxclients slot is held until the session ends
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
    let ip: std::net::IpAddr = bind
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let addr = SocketAddr::new(ip, port);
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
            let current = topo.load_full();
            match fetch_topology(&cfg, current.nodes.iter().map(|n| n.addr.as_str())).await {
                Ok(mut new_topo) => {
                    // the pointer lands before the epoch: a reader of the epoch sees this topology
                    let epoch = topo_epoch() + 1;
                    new_topo.epoch = epoch;
                    topo.store(Arc::new(new_topo));
                    TOPO_EPOCH.store(epoch, Ordering::Release);
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
    let frame = crate::backend::read_reply(&mut stream, &mut buf).await?;
    let text = bulk_text(&frame).ok_or("unexpected CLUSTER NODES reply")?;
    Topology::parse(text)
}

fn bulk_text(frame: &[u8]) -> Option<&str> {
    std::str::from_utf8(resp::bulk_payload(frame)?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_places_round_robin() {
        let mut p = Placer::new(3, false);
        assert_eq!(p.best(), 0);
        p.placed(0);
        assert_eq!(p.best(), 1);
        p.placed(1);
        p.placed(2);
        assert_eq!(p.best(), 0);
    }

    #[test]
    fn least_loaded_prefers_the_quiet_worker_then_spreads() {
        let mut p = Placer::new(3, true);
        p.buckets = vec![5, 0, 5];
        assert_eq!(p.best(), 1);
        p.placed(1);
        assert_eq!(p.best(), 2);
        p.placed(2);
        assert_eq!(p.best(), 0);
        p.placed(0);
        assert_eq!(p.best(), 1);
    }

    #[test]
    fn rank_orders_by_key_then_rotation() {
        let mut p = Placer::new(4, true);
        p.buckets = vec![1, 0, 0, 1];
        p.next = 2;
        p.rank();
        assert_eq!(p.order, vec![2, 1, 3, 0]);
    }

    #[test]
    fn refresh_normalizes_activity_and_idles_below_the_floor() {
        let stats = Stats::new(2);
        stats.workers[0].commands.store(1000, Ordering::Relaxed);
        stats.workers[1].commands.store(200, Ordering::Relaxed);
        let mut p = Placer::new(2, true);
        p.refresh(&stats);
        assert_eq!(p.buckets, vec![1, 0]);
        p.refresh(&stats);
        assert_eq!(p.buckets, vec![0, 0]);
    }
}

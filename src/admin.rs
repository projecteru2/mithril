//! Proxy-answered commands and single-virtual-node cluster emulation.

use std::borrow::Cow;
use std::fmt::Write;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::Spec;
use crate::config::Config;
use crate::crc16::{self, SLOTS};
use crate::resp;
use crate::stats::{ClientInfo, Stats};

pub const SERVER_VERSION: &str = "7.4.0";
const CLUSTER_BUS_OFFSET: u32 = 10000;
const HEX: &[u8; 16] = b"0123456789abcdef";
const CLUSTER_INFO: &str = "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\n\
     cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n\
     cluster_known_nodes:1\r\ncluster_size:1\r\n";
const CONFIG_KEYS: [&str; 18] = [
    "bind",
    "port",
    "announce-addr",
    "worker-threads",
    "maxclients",
    "bootstrap",
    "backend-conns",
    "backend-sharding",
    "reply-cache",
    "reply-cache-max-bytes",
    "reply-cache-max-age-secs",
    "requirepass",
    "slave-mode",
    "placement",
    "tcp-keepalive",
    "query-buffer-limit",
    "topology-refresh-secs",
    "loglevel",
];

pub fn ping(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    match args.len() {
        1 => out.extend_from_slice(resp::PONG),
        2 => resp::bulk(&mut out, args[1]),
        _ => resp::write_error(&mut out, "ERR wrong number of arguments for 'ping' command"),
    }
    out
}

pub fn echo(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    match args.len() {
        2 => resp::bulk(&mut out, args[1]),
        _ => resp::write_error(&mut out, "ERR wrong number of arguments for 'echo' command"),
    }
    out
}

pub fn select(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    if args.len() == 2 && args[1] == b"0" {
        out.extend_from_slice(resp::OK);
    } else {
        resp::write_error(&mut out, "ERR SELECT is not allowed in cluster mode");
    }
    out
}

pub fn time() -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut out = Vec::new();
    out.extend_from_slice(b"*2\r\n");
    let mut digits = [0u8; resp::DEC_BUF];
    resp::bulk(&mut out, resp::u64_digits(&mut digits, now.as_secs()));
    resp::bulk(
        &mut out,
        resp::u64_digits(&mut digits, u64::from(now.subsec_micros())),
    );
    out
}

/// CLIENT LIST over every worker's sessions, one line per connection.
pub fn client_list(stats: &Stats) -> Vec<u8> {
    let registry = stats.registry();
    let mut rows: Vec<(&u64, &ClientInfo)> = registry.iter().collect();
    rows.sort_unstable_by_key(|(id, _)| **id);
    let mut text = String::with_capacity(rows.len() * 96);
    for (id, c) in rows {
        let _ = writeln!(
            text,
            "id={id} addr={} fd={} name={} age={} cmd=",
            c.addr,
            c.fd,
            c.name,
            c.since.elapsed().as_secs()
        );
    }
    drop(registry);
    let mut out = Vec::with_capacity(text.len() + 16);
    resp::bulk(&mut out, text.as_bytes());
    out
}

pub fn command_reply(args: &[&[u8]], proto: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let table = crate::command::table();
    let sub = |name: &[u8]| sub_is(args, 1, name);
    if args.len() < 2 {
        resp::array_header(&mut out, table.len());
        for spec in table {
            command_entry(&mut out, spec);
        }
    } else if sub(b"count") {
        resp::integer(&mut out, table.len() as i64);
    } else if sub(b"docs") {
        out.extend_from_slice(if proto >= 3 { b"%0\r\n" } else { b"*0\r\n" });
    } else if sub(b"info") {
        resp::array_header(&mut out, args.len() - 2);
        for name in &args[2..] {
            match crate::command::lookup(name) {
                Some(spec) => command_entry(&mut out, spec),
                None if proto >= 3 => out.extend_from_slice(resp::NIL_RESP3),
                None => out.extend_from_slice(resp::NIL_ARRAY),
            }
        }
    } else {
        resp::write_error(&mut out, "ERR unknown COMMAND subcommand");
    }
    out
}

pub fn cluster(args: &[&[u8]], cfg: &Config, proto: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let sub = |name: &[u8]| sub_is(args, 1, name);
    if sub(b"info") {
        resp::bulk(&mut out, CLUSTER_INFO.as_bytes());
    } else if sub(b"myid") {
        resp::bulk(&mut out, node_id(&cfg.announce_addr).as_bytes());
    } else if sub(b"keyslot") {
        match args.get(2) {
            Some(key) => resp::integer(&mut out, i64::from(crc16::slot(key))),
            None => resp::write_error(&mut out, "ERR wrong number of arguments"),
        }
    } else if sub(b"nodes") {
        let (host, port) = split_announce(&cfg.announce_addr);
        let line = format!(
            "{} {host}:{port}@{} myself,master - 0 0 1 connected 0-16383\n",
            node_id(&cfg.announce_addr),
            port as u32 + CLUSTER_BUS_OFFSET,
        );
        resp::bulk(&mut out, line.as_bytes());
    } else if sub(b"slots") {
        let (host, port) = split_announce(&cfg.announce_addr);
        out.extend_from_slice(b"*1\r\n*3\r\n");
        resp::integer(&mut out, 0);
        resp::integer(&mut out, SLOTS as i64 - 1);
        out.extend_from_slice(b"*3\r\n");
        resp::bulk(&mut out, host.as_bytes());
        resp::integer(&mut out, i64::from(port));
        resp::bulk(&mut out, node_id(&cfg.announce_addr).as_bytes());
    } else if sub(b"shards") {
        let open = if proto >= 3 {
            b"%2\r\n".as_ref()
        } else {
            b"*4\r\n".as_ref()
        };
        out.extend_from_slice(b"*1\r\n");
        out.extend_from_slice(open);
        resp::bulk(&mut out, b"slots");
        out.extend_from_slice(b"*2\r\n");
        resp::integer(&mut out, 0);
        resp::integer(&mut out, SLOTS as i64 - 1);
        resp::bulk(&mut out, b"nodes");
        out.extend_from_slice(b"*1\r\n");
        shard_node(&mut out, cfg, proto);
    } else {
        resp::write_error(&mut out, "ERR unsupported CLUSTER subcommand");
    }
    out
}

pub fn info(cfg: &Config, stats: &Stats, started: u64) -> Vec<u8> {
    let (cpu_sys, cpu_user) = cpu_seconds();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let text = format!(
        "# Server\r\nredis_version:{SERVER_VERSION}\r\nredis_mode:cluster\r\n\
         mithril_version:{}\r\nprocess_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{}\r\n\
         config_file:{}\r\n\r\n\
         # Clients\r\nconnected_clients:{}\r\n\r\n\
         # CPU\r\nused_cpu_sys:{:.3}\r\nused_cpu_user:{:.3}\r\n\r\n\
         # Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\n\
         total_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\n\
         total_errors:{}\r\nredirections:{}\r\n\
         readers_exited:{}\r\nwriters_exited:{}\r\nsessions_closed:{}\r\n\r\n\
         # Mithril\r\nworker_threads:{}\r\nbackend_conns_per_node:{}\r\n\
         backend_sharding:{}\r\nslave_mode:{}\r\nreply_cache:{}\r\n\
         cache_hits:{}\r\ncache_misses:{}\r\ncache_invalidations:{}\r\n\
         cache_armed_workers:{}\r\ncache_entries:{}\r\ncache_bytes:{}\r\n\
         cache_flips:{}\r\nworker_commands:{}\r\n",
        crate::VERSION,
        std::process::id(),
        cfg.port,
        now.saturating_sub(started),
        cfg.config_file,
        stats.clients.load(Ordering::Relaxed),
        cpu_sys,
        cpu_user,
        stats.total_connections.load(Ordering::Relaxed),
        stats.sum(|w| &w.commands),
        stats.sum(|w| &w.bytes_in),
        stats.sum(|w| &w.bytes_out),
        stats.sum(|w| &w.errors),
        stats.sum(|w| &w.redirects),
        stats.sum(|w| &w.readers_exited),
        stats.sum(|w| &w.writers_exited),
        stats.sum(|w| &w.sessions_closed),
        cfg.workers,
        cfg.backend_conns,
        cfg.backend_sharding,
        cfg.slave_mode,
        yesno(cfg.reply_cache),
        stats.sum(|w| &w.cache_hits),
        stats.sum(|w| &w.cache_misses),
        stats.sum(|w| &w.cache_invalidations),
        stats.sum(|w| &w.cache_armed),
        stats.sum(|w| &w.cache_entries),
        stats.sum(|w| &w.cache_bytes),
        stats.sum(|w| &w.cache_flips),
        stats
            .workers
            .iter()
            .map(|w| w.commands.load(Ordering::Relaxed).to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    let mut out = Vec::new();
    resp::bulk(&mut out, text.as_bytes());
    out
}

pub fn config_cmd(args: &[&[u8]], cfg: &Config) -> Vec<u8> {
    let mut out = Vec::new();
    if sub_is(args, 1, b"get") && args.len() == 3 {
        let want = args[2];
        let wanted = |k: &&&str| want == b"*" || k.as_bytes().eq_ignore_ascii_case(want);
        resp::array_header(&mut out, CONFIG_KEYS.iter().filter(wanted).count() * 2);
        for k in CONFIG_KEYS.iter().filter(wanted) {
            resp::bulk(&mut out, k.as_bytes());
            resp::bulk(&mut out, config_value(cfg, k).as_bytes());
        }
    } else if sub_is(args, 1, b"set") && args.len() == 4 {
        if !sub_is(args, 2, b"loglevel") {
            resp::write_error(&mut out, "ERR unsupported CONFIG SET parameter");
        } else {
            match crate::log::parse_level(&String::from_utf8_lossy(args[3])) {
                Ok(level) => {
                    crate::log::set_level(level);
                    out.extend_from_slice(resp::OK);
                }
                Err(e) => resp::write_error(&mut out, &format!("ERR {e}")),
            }
        }
    } else {
        resp::write_error(&mut out, "ERR unsupported CONFIG subcommand");
    }
    out
}

/// Emulated node id: 40 hex chars derived from the announce address.
fn node_id(announce: &str) -> String {
    let mut id = String::with_capacity(40);
    let mut x = crate::shard::fnv(announce.as_bytes());
    for _ in 0..40 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        id.push(HEX[(x >> 60) as usize & 0xf] as char);
    }
    id
}

fn command_entry(out: &mut Vec<u8>, spec: &Spec) {
    out.extend_from_slice(b"*6\r\n");
    resp::bulk(out, spec.name.as_bytes());
    resp::integer(out, i64::from(spec.arity));
    let mut flags: Vec<&[u8]> = Vec::new();
    if spec.is_write() {
        flags.push(b"write");
        flags.push(b"denyoom");
    }
    if spec.is_readonly() {
        flags.push(b"readonly");
    }
    resp::array_header(out, flags.len());
    for f in flags {
        out.push(b'+');
        out.extend_from_slice(f);
        out.extend_from_slice(b"\r\n");
    }
    resp::integer(out, i64::from(spec.first_key));
    resp::integer(out, i64::from(spec.last_key));
    resp::integer(out, i64::from(spec.step));
}

fn shard_node(out: &mut Vec<u8>, cfg: &Config, proto: u8) {
    let (host, port) = split_announce(&cfg.announce_addr);
    out.extend_from_slice(if proto >= 3 { b"%6\r\n" } else { b"*12\r\n" });
    resp::bulk(out, b"id");
    resp::bulk(out, node_id(&cfg.announce_addr).as_bytes());
    resp::bulk(out, b"endpoint");
    resp::bulk(out, host.as_bytes());
    resp::bulk(out, b"port");
    resp::integer(out, i64::from(port));
    resp::bulk(out, b"role");
    resp::bulk(out, b"master");
    resp::bulk(out, b"replication-offset");
    resp::integer(out, 0);
    resp::bulk(out, b"health");
    resp::bulk(out, b"online");
}

fn cpu_seconds() -> (f64, f64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: RUSAGE_SELF with a pointer to writable rusage-sized storage;
    // the struct is read only after getrusage reported success
    let usage = unsafe {
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) != 0 {
            return (0.0, 0.0);
        }
        usage.assume_init()
    };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (secs(usage.ru_stime), secs(usage.ru_utime))
}

fn split_announce(announce: &str) -> (&str, u16) {
    match announce.rsplit_once(':') {
        Some((h, p)) => (h, p.parse().unwrap_or(crate::config::DEFAULT_PORT)),
        None => (announce, crate::config::DEFAULT_PORT),
    }
}

fn yesno(v: bool) -> &'static str {
    if v { "yes" } else { "no" }
}

fn config_value<'a>(cfg: &'a Config, key: &str) -> Cow<'a, str> {
    match key {
        "bind" => Cow::Borrowed(&cfg.bind),
        "port" => cfg.port.to_string().into(),
        "announce-addr" => Cow::Borrowed(&cfg.announce_addr),
        "worker-threads" => cfg.workers.to_string().into(),
        "maxclients" => cfg.maxclients.to_string().into(),
        "bootstrap" => cfg.bootstrap.join(",").into(),
        "backend-conns" => cfg.backend_conns.to_string().into(),
        "backend-sharding" => cfg.backend_sharding.to_string().into(),
        "reply-cache" => yesno(cfg.reply_cache).into(),
        "reply-cache-max-bytes" => cfg.reply_cache_max_bytes.to_string().into(),
        "reply-cache-max-age-secs" => cfg.reply_cache_max_age_secs.to_string().into(),
        "requirepass" if cfg.requirepass.is_empty() => "".into(),
        "requirepass" => "*****".into(),
        "slave-mode" => cfg.slave_mode.to_string().into(),
        "placement" => cfg.placement.to_string().into(),
        "tcp-keepalive" => cfg.tcp_keepalive_secs.to_string().into(),
        "query-buffer-limit" => cfg.query_buffer_limit.to_string().into(),
        "topology-refresh-secs" => cfg.topology_refresh_secs.to_string().into(),
        _ => crate::log::level_name(crate::log::level()).into(),
    }
}

fn sub_is(args: &[&[u8]], i: usize, name: &[u8]) -> bool {
    args.get(i).is_some_and(|s| s.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn info_fields_line_up() {
        let cfg = test_cfg();
        let stats = crate::stats::Stats::new(2);
        stats.clients.store(7, Ordering::Relaxed);
        let out = info(&cfg, &stats, 0);
        let text = String::from_utf8_lossy(&out);
        let field = |k: &str| {
            text.lines()
                .find_map(|l| l.strip_prefix(&format!("{k}:")))
                .map(|v| v.trim_end().to_string())
        };
        assert_eq!(field("connected_clients").as_deref(), Some("7"));
        assert_eq!(field("mithril_version").as_deref(), Some(crate::VERSION));
        assert_eq!(field("cache_flips").as_deref(), Some("0"));
        assert_eq!(
            field("worker_threads").as_deref(),
            Some(cfg.workers.to_string().as_str())
        );
        assert!(field("used_cpu_sys").unwrap().parse::<f64>().is_ok());
        assert!(field("used_cpu_user").unwrap().parse::<f64>().is_ok());
        assert_eq!(
            field("config_file").as_deref(),
            Some(cfg.config_file.as_str())
        );
    }

    fn test_cfg() -> Config {
        let mut cfg = Config::default();
        cfg.set("bootstrap", "127.0.0.1:7001").unwrap();
        cfg.announce_addr = "10.1.2.3:7979".to_string();
        cfg
    }

    #[test]
    fn node_id_is_stable_40_hex() {
        let id = node_id("1.2.3.4:7979");
        assert_eq!(id.len(), 40);
        assert_eq!(id, node_id("1.2.3.4:7979"));
        assert_ne!(id, node_id("1.2.3.4:7980"));
    }

    #[test]
    fn cluster_emulation_owns_all_slots() {
        let cfg = test_cfg();
        let nodes = cluster(&[b"cluster", b"nodes"], &cfg, 2);
        let text = String::from_utf8_lossy(&nodes);
        assert!(text.contains("0-16383"), "{text}");
        assert!(text.contains("myself,master"), "{text}");
        let slots = cluster(&[b"cluster", b"slots"], &cfg, 2);
        assert!(String::from_utf8_lossy(&slots).contains("10.1.2.3"));
        let ks = cluster(&[b"cluster", b"keyslot", b"123456789"], &cfg, 2);
        assert_eq!(ks, format!(":{}\r\n", 0x31c3).into_bytes());
    }

    #[test]
    fn config_get_redacts_requirepass() {
        let mut cfg = test_cfg();
        cfg.requirepass = "secret".to_string();
        let reply = config_cmd(&[b"config", b"get", b"requirepass"], &cfg);
        let text = String::from_utf8_lossy(&reply);
        assert!(!text.contains("secret"), "{text}");
    }
}

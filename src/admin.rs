//! Proxy-answered commands and single-virtual-node cluster emulation.

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::Spec;
use crate::config::Config;
use crate::crc16::{self, SLOTS};
use crate::resp;
use crate::stats::Stats;

pub const SERVER_VERSION: &str = "7.4.0";
pub const CLUSTER_BUS_OFFSET: u32 = 10000;
pub const OK: &[u8] = b"+OK\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";

/// Emulated node id: 40 hex chars derived from the announce address.
pub fn node_id(announce: &str) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in announce.as_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    let mut id = String::with_capacity(40);
    let mut x = h;
    for _ in 0..40 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        id.push(char::from_digit((x >> 60) as u32 & 0xf, 16).unwrap_or('0'));
    }
    id
}

pub fn bulk(out: &mut Vec<u8>, payload: &[u8]) {
    out.push(b'$');
    resp::push_usize(out, payload.len());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
}

pub fn integer(out: &mut Vec<u8>, n: i64) {
    out.push(b':');
    resp::push_i64(out, n);
    out.extend_from_slice(b"\r\n");
}

pub fn ping(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    match args.len() {
        1 => out.extend_from_slice(PONG),
        2 => bulk(&mut out, args[1]),
        _ => resp::write_error(&mut out, "ERR wrong number of arguments for 'ping' command"),
    }
    out
}

pub fn echo(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    match args.len() {
        2 => bulk(&mut out, args[1]),
        _ => resp::write_error(&mut out, "ERR wrong number of arguments for 'echo' command"),
    }
    out
}

pub fn select(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    if args.len() == 2 && args[1] == b"0" {
        out.extend_from_slice(OK);
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
    bulk(&mut out, now.as_secs().to_string().as_bytes());
    bulk(&mut out, now.subsec_micros().to_string().as_bytes());
    out
}

pub fn command_reply(args: &[&[u8]], table: &[Spec], proto: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let sub = args.get(1).map(|s| s.to_ascii_lowercase());
    match sub.as_deref() {
        None => {
            resp::array_header(&mut out, table.len());
            for spec in table {
                command_entry(&mut out, spec);
            }
        }
        Some(b"count") => integer(&mut out, table.len() as i64),
        Some(b"docs") => {
            out.extend_from_slice(if proto >= 3 { b"%0\r\n" } else { b"*0\r\n" });
        }
        Some(b"info") => {
            resp::array_header(&mut out, args.len() - 2);
            for name in &args[2..] {
                match crate::command::lookup(name) {
                    Some(spec) => command_entry(&mut out, spec),
                    None => out.extend_from_slice(b"*-1\r\n"),
                }
            }
        }
        Some(_) => resp::write_error(&mut out, "ERR unknown COMMAND subcommand"),
    }
    out
}

pub fn cluster(args: &[&[u8]], cfg: &Config, proto: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let sub = args
        .get(1)
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    match sub.as_slice() {
        b"info" => {
            let text = "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\n\
                 cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n\
                 cluster_known_nodes:1\r\ncluster_size:1\r\n";
            bulk(&mut out, text.as_bytes());
        }
        b"myid" => bulk(&mut out, node_id(&cfg.announce_addr).as_bytes()),
        b"keyslot" => match args.get(2) {
            Some(key) => integer(&mut out, i64::from(crc16::slot(key))),
            None => resp::write_error(&mut out, "ERR wrong number of arguments"),
        },
        b"nodes" => {
            let (host, port) = split_announce(&cfg.announce_addr);
            let line = format!(
                "{} {host}:{port}@{} myself,master - 0 0 1 connected 0-16383\n",
                node_id(&cfg.announce_addr),
                port as u32 + CLUSTER_BUS_OFFSET,
            );
            bulk(&mut out, line.as_bytes());
        }
        b"slots" => {
            let (host, port) = split_announce(&cfg.announce_addr);
            out.extend_from_slice(b"*1\r\n*3\r\n");
            integer(&mut out, 0);
            integer(&mut out, SLOTS as i64 - 1);
            out.extend_from_slice(b"*3\r\n");
            bulk(&mut out, host.as_bytes());
            integer(&mut out, i64::from(port));
            bulk(&mut out, node_id(&cfg.announce_addr).as_bytes());
        }
        b"shards" => {
            let open = if proto >= 3 {
                b"%2\r\n".as_ref()
            } else {
                b"*4\r\n".as_ref()
            };
            out.extend_from_slice(b"*1\r\n");
            out.extend_from_slice(open);
            bulk(&mut out, b"slots");
            out.extend_from_slice(b"*2\r\n");
            integer(&mut out, 0);
            integer(&mut out, SLOTS as i64 - 1);
            bulk(&mut out, b"nodes");
            out.extend_from_slice(b"*1\r\n");
            shard_node(&mut out, cfg, proto);
        }
        _ => resp::write_error(&mut out, "ERR unsupported CLUSTER subcommand"),
    }
    out
}

pub fn info(cfg: &Config, stats: &Stats, started: u64) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let text = format!(
        "# Server\r\nredis_version:{SERVER_VERSION}\r\nredis_mode:cluster\r\n\
         mithril_version:{}\r\nprocess_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{}\r\n\r\n\
         # Clients\r\nconnected_clients:{}\r\n\r\n\
         # Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\n\
         total_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\n\
         total_errors:{}\r\nredirections:{}\r\n\
         readers_exited:{}\r\nwriters_exited:{}\r\nsessions_closed:{}\r\n\r\n\
         # Mithril\r\nworker_threads:{}\r\nbackend_conns_per_node:{}\r\nslave_mode:{}\r\n\
         worker_commands:{}\r\n",
        env!("CARGO_PKG_VERSION"),
        std::process::id(),
        cfg.port,
        now.saturating_sub(started),
        stats.clients.load(Ordering::Relaxed),
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
        cfg.slave_mode,
        stats
            .workers
            .iter()
            .map(|w| w.commands.load(Ordering::Relaxed).to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    let mut out = Vec::new();
    bulk(&mut out, text.as_bytes());
    out
}

pub fn config_cmd(args: &[&[u8]], cfg: &Config) -> Vec<u8> {
    let mut out = Vec::new();
    let sub = args
        .get(1)
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    match sub.as_slice() {
        b"get" if args.len() == 3 => {
            let pairs = config_pairs(cfg);
            let pattern = String::from_utf8_lossy(args[2]);
            let matched: Vec<_> = pairs
                .iter()
                .filter(|(k, _)| pattern == "*" || *k == pattern.as_ref())
                .collect();
            resp::array_header(&mut out, matched.len() * 2);
            for (k, v) in matched {
                bulk(&mut out, k.as_bytes());
                bulk(&mut out, v.as_bytes());
            }
        }
        b"set" if args.len() == 4 => match (args[2], args[3]) {
            (b"loglevel", v) => match crate::log::parse_level(&String::from_utf8_lossy(v)) {
                Ok(level) => {
                    crate::log::set_level(level);
                    out.extend_from_slice(OK);
                }
                Err(e) => resp::write_error(&mut out, &format!("ERR {e}")),
            },
            _ => resp::write_error(&mut out, "ERR unsupported CONFIG SET parameter"),
        },
        _ => resp::write_error(&mut out, "ERR unsupported CONFIG subcommand"),
    }
    out
}

fn command_entry(out: &mut Vec<u8>, spec: &Spec) {
    out.extend_from_slice(b"*6\r\n");
    bulk(out, spec.name.as_bytes());
    integer(out, i64::from(spec.arity));
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
    integer(out, i64::from(spec.first_key));
    integer(out, i64::from(spec.last_key));
    integer(out, i64::from(spec.step));
}

fn shard_node(out: &mut Vec<u8>, cfg: &Config, proto: u8) {
    let (host, port) = split_announce(&cfg.announce_addr);
    out.extend_from_slice(if proto >= 3 { b"%6\r\n" } else { b"*12\r\n" });
    bulk(out, b"id");
    bulk(out, node_id(&cfg.announce_addr).as_bytes());
    bulk(out, b"endpoint");
    bulk(out, host.as_bytes());
    bulk(out, b"port");
    integer(out, i64::from(port));
    bulk(out, b"role");
    bulk(out, b"master");
    bulk(out, b"replication-offset");
    integer(out, 0);
    bulk(out, b"health");
    bulk(out, b"online");
}

fn split_announce(announce: &str) -> (String, u16) {
    match announce.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse().unwrap_or(crate::config::DEFAULT_PORT),
        ),
        None => (announce.to_string(), crate::config::DEFAULT_PORT),
    }
}

fn config_pairs(cfg: &Config) -> Vec<(&'static str, String)> {
    vec![
        ("bind", cfg.bind.clone()),
        ("port", cfg.port.to_string()),
        ("announce-addr", cfg.announce_addr.clone()),
        ("worker-threads", cfg.workers.to_string()),
        ("maxclients", cfg.maxclients.to_string()),
        ("bootstrap", cfg.bootstrap.join(",")),
        ("backend-conns", cfg.backend_conns.to_string()),
        (
            "requirepass",
            if cfg.requirepass.is_empty() {
                ""
            } else {
                "*****"
            }
            .to_string(),
        ),
        ("slave-mode", cfg.slave_mode.to_string()),
        ("tcp-keepalive", cfg.tcp_keepalive_secs.to_string()),
        ("query-buffer-limit", cfg.query_buffer_limit.to_string()),
        (
            "topology-refresh-secs",
            cfg.topology_refresh_secs.to_string(),
        ),
        (
            "loglevel",
            crate::log::level_name(crate::log::level()).to_string(),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

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

//! Configuration: `key value` lines, `#` comments, CLI overrides.

use std::fmt;
use std::fs;

pub const DEFAULT_PORT: u16 = 7979;

const BYTE_UNITS: &[(&str, usize)] = &[
    ("gb", 1 << 30),
    ("g", 1 << 30),
    ("mb", 1 << 20),
    ("m", 1 << 20),
    ("kb", 1 << 10),
    ("k", 1 << 10),
];

/// Replica read routing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    RoundRobin,
    LeastLoaded,
}

impl fmt::Display for Placement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Placement::RoundRobin => "round-robin",
            Placement::LeastLoaded => "least-loaded",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlaveMode {
    Off,
    /// Reads balance across master and replicas.
    ReadWrite,
    /// Reads go to replicas only.
    WriteOnly,
}

impl fmt::Display for SlaveMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SlaveMode::Off => "off",
            SlaveMode::ReadWrite => "master_readwrite",
            SlaveMode::WriteOnly => "master_writeonly",
        })
    }
}

/// Proxy configuration, immutable after startup except where noted.
#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub announce_addr: String,
    pub workers: usize,
    pub maxclients: usize,
    pub bootstrap: Vec<String>,
    pub backend_conns: usize,
    pub requirepass: String,
    pub backend_user: String,
    pub backend_pass: String,
    pub slave_mode: SlaveMode,
    pub placement: Placement,
    pub tcp_keepalive_secs: u64,
    pub query_buffer_limit: usize,
    pub topology_refresh_secs: u64,
    pub loglevel: u8,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: "0.0.0.0".to_string(),
            port: DEFAULT_PORT,
            announce_addr: String::new(),
            workers: 0,
            maxclients: 10000,
            bootstrap: Vec::new(),
            backend_conns: 1,
            requirepass: String::new(),
            backend_user: String::new(),
            backend_pass: String::new(),
            slave_mode: SlaveMode::Off,
            placement: Placement::LeastLoaded,
            tcp_keepalive_secs: 300,
            query_buffer_limit: 1024 * 1024 * 1024,
            topology_refresh_secs: 15,
            loglevel: crate::log::NOTICE,
        }
    }
}

impl Config {
    /// Parses a config file; call [`Config::finish`] after CLI overrides.
    pub fn load(path: &str) -> Result<Config, String> {
        let text = fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
        let mut cfg = Config::default();
        for (ln, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line.split_once(char::is_whitespace).unwrap_or((line, ""));
            cfg.set(key, value.trim())
                .map_err(|e| format!("{path}:{}: {e}", ln + 1))?;
        }
        Ok(cfg)
    }

    /// Applies one key/value pair; used by both file load and CLI flags.
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), String> {
        match key {
            "bind" => self.bind = value.to_string(),
            "port" => self.port = parse(key, value)?,
            "announce-addr" => self.announce_addr = value.to_string(),
            "worker-threads" => self.workers = parse(key, value)?,
            "maxclients" => self.maxclients = parse(key, value)?,
            "bootstrap" => {
                self.bootstrap = value.split(',').map(|s| s.trim().to_string()).collect();
            }
            "backend-conns" => self.backend_conns = parse_bounded(key, value, 1, 512)?,
            "requirepass" => self.requirepass = value.to_string(),
            "backend-auth-user" => self.backend_user = value.to_string(),
            "backend-auth-pass" => self.backend_pass = value.to_string(),
            "slave-mode" => self.slave_mode = parse_slave_mode(value)?,
            "placement" => self.placement = parse_placement(value)?,
            "tcp-keepalive" => self.tcp_keepalive_secs = parse(key, value)?,
            "query-buffer-limit" => self.query_buffer_limit = parse_bytes(key, value)?,
            "topology-refresh-secs" => {
                self.topology_refresh_secs = parse_bounded(key, value, 1, 3600)? as u64;
            }
            "loglevel" => self.loglevel = crate::log::parse_level(value)?,
            _ => return Err(format!("unknown parameter '{key}'")),
        }
        Ok(())
    }

    /// Validates and resolves derived defaults.
    pub fn finish(mut self) -> Result<Config, String> {
        if self.bootstrap.is_empty() {
            return Err("bootstrap requires at least one seed address".to_string());
        }
        if self.workers == 0 {
            self.workers = std::thread::available_parallelism().map_or(4, |n| n.get());
        }
        if self.announce_addr.is_empty() {
            self.announce_addr = format!("{}:{}", self.bind, self.port);
        }
        Ok(self)
    }
}

fn parse<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("bad value for '{key}': {value}"))
}

fn parse_bounded(key: &str, value: &str, min: usize, max: usize) -> Result<usize, String> {
    let n: usize = parse(key, value)?;
    if n < min || n > max {
        return Err(format!("'{key}' must be in [{min}, {max}]"));
    }
    Ok(n)
}

fn parse_placement(value: &str) -> Result<Placement, String> {
    match value {
        "round-robin" => Ok(Placement::RoundRobin),
        "least-loaded" => Ok(Placement::LeastLoaded),
        _ => Err(format!("bad placement '{value}'")),
    }
}

fn parse_slave_mode(value: &str) -> Result<SlaveMode, String> {
    match value {
        "off" => Ok(SlaveMode::Off),
        "master_readwrite" => Ok(SlaveMode::ReadWrite),
        "master_writeonly" => Ok(SlaveMode::WriteOnly),
        _ => Err(format!("bad slave-mode '{value}'")),
    }
}

fn parse_bytes(key: &str, value: &str) -> Result<usize, String> {
    let lower = value.to_ascii_lowercase();
    let (digits, mult) = BYTE_UNITS
        .iter()
        .find_map(|&(suffix, mult)| lower.strip_suffix(suffix).map(|d| (d, mult)))
        .unwrap_or((lower.as_str(), 1));
    let n: usize = parse(key, digits.trim())?;
    Ok(n * mult)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sizes_and_modes() {
        let mut cfg = Config::default();
        cfg.set("query-buffer-limit", "512mb").unwrap();
        assert_eq!(cfg.query_buffer_limit, 512 << 20);
        cfg.set("slave-mode", "master_writeonly").unwrap();
        assert_eq!(cfg.slave_mode, SlaveMode::WriteOnly);
        assert!(cfg.set("slave-mode", "sideways").is_err());
        assert!(cfg.set("no-such-key", "1").is_err());
        assert!(cfg.set("backend-conns", "0").is_err());
    }
}

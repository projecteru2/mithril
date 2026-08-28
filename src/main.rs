//! Thin CLI wrapper: parses arguments, loads config, runs the server.

use mithril::config::Config;

const USAGE: &str = "usage: mithril <conf-file> [--<key> <value>]...";

fn main() {
    let mut args = std::env::args().skip(1);
    let mut cfg: Option<Config> = None;
    let mut overrides: Vec<(String, String)> = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" => {
                println!("mithril {}", env!("CARGO_PKG_VERSION"));
                return;
            }
            "--help" => {
                println!("{USAGE}");
                return;
            }
            flag if flag.starts_with("--") => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for {flag}");
                    std::process::exit(1);
                };
                overrides.push((flag[2..].to_string(), value));
            }
            path => match Config::load(path) {
                Ok(c) => cfg = Some(c),
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
            },
        }
    }
    let mut cfg = match cfg {
        Some(c) => c,
        None => {
            eprintln!("{USAGE}");
            std::process::exit(1);
        }
    };
    for (key, value) in &overrides {
        if let Err(e) = cfg.set(key, value) {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
    let cfg = match cfg.finish() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = mithril::server::run(cfg) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

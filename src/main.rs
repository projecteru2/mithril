//! Thin CLI wrapper: parses arguments, loads config, runs the server.

use mithril::config::Config;

const USAGE: &str = "usage: mithril <conf-file> [--<key> <value>]...";
const VERSION: &str = match option_env!("MITHRIL_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};
const REVISION: &str = match option_env!("MITHRIL_REVISION") {
    Some(v) => v,
    None => "unknown",
};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut cfg: Option<Config> = None;
    let mut overrides: Vec<(String, String)> = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" => {
                println!("mithril {VERSION} ({REVISION})");
                return;
            }
            "--help" => {
                println!("{USAGE}");
                return;
            }
            flag if flag.starts_with("--") => {
                let Some(value) = args.next() else {
                    die(format!("missing value for {flag}"));
                };
                overrides.push((flag[2..].to_string(), value));
            }
            path => match Config::load(path) {
                Ok(c) => cfg = Some(c),
                Err(e) => die(e),
            },
        }
    }
    let Some(mut cfg) = cfg else {
        die(USAGE);
    };
    for (key, value) in &overrides {
        if let Err(e) = cfg.set(key, value) {
            die(e);
        }
    }
    let cfg = match cfg.finish() {
        Ok(c) => c,
        Err(e) => die(e),
    };
    if let Err(e) = mithril::server::run(cfg) {
        die(e);
    }
}

fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("{msg}");
    std::process::exit(1)
}

//! Leveled stdout logger with a runtime-adjustable threshold.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEBUG: u8 = 0;
pub const VERBOSE: u8 = 1;
pub const NOTICE: u8 = 2;
pub const WARNING: u8 = 3;

static LEVEL: AtomicU8 = AtomicU8::new(NOTICE);

pub fn set_level(level: u8) {
    LEVEL.store(level, Ordering::Relaxed);
}

pub fn level() -> u8 {
    LEVEL.load(Ordering::Relaxed)
}

/// Returns the level's config-file name.
pub fn level_name(level: u8) -> &'static str {
    match level {
        DEBUG => "debug",
        VERBOSE => "verbose",
        NOTICE => "notice",
        _ => "warning",
    }
}

/// Parses a config-file level name.
pub fn parse_level(value: &str) -> Result<u8, String> {
    match value {
        "debug" => Ok(DEBUG),
        "verbose" => Ok(VERBOSE),
        "notice" => Ok(NOTICE),
        "warning" => Ok(WARNING),
        _ => Err(format!("bad loglevel '{value}'")),
    }
}

pub fn emit(level: u8, tag: char, args: std::fmt::Arguments<'_>) {
    if level < LEVEL.load(Ordering::Relaxed) {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    println!("{}.{:03} {tag} {args}", now.as_secs(), now.subsec_millis());
}

#[macro_export]
macro_rules! log_notice {
    ($($arg:tt)*) => { $crate::log::emit($crate::log::NOTICE, 'N', format_args!($($arg)*)) };
}

#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => { $crate::log::emit($crate::log::WARNING, 'W', format_args!($($arg)*)) };
}

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => { $crate::log::emit($crate::log::DEBUG, 'D', format_args!($($arg)*)) };
}

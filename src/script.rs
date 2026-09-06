//! Script bodies loaded through the proxy, replayed to a node that lost them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;

use crate::resp;

/// Process-wide memory of every script SCRIPT LOAD carried.
pub struct Scripts {
    bodies: Mutex<HashMap<Box<[u8]>, Bytes>>,
}

impl Scripts {
    pub fn new() -> Arc<Scripts> {
        Arc::new(Scripts {
            bodies: Mutex::new(HashMap::new()),
        })
    }

    /// Keeps `body` under the sha the nodes returned for it.
    pub fn remember(&self, sha: &[u8], body: Bytes) {
        self.lock().insert(Box::from(sha), body);
    }

    pub fn forget_all(&self) {
        self.lock().clear();
    }

    /// A SCRIPT LOAD frame for the script an EVALSHA request names, when the proxy loaded it.
    pub fn load_frame(&self, evalsha: &[u8]) -> Option<Bytes> {
        let (argc, _) = resp::scan_int_line(evalsha, 1)?;
        let sha = resp::Args::new(evalsha, argc as usize).nth(1)?;
        let body = self.lock().get(sha)?.clone();
        let mut out = Vec::with_capacity(body.len() + 32);
        resp::write_command(&mut out, &[b"SCRIPT", b"LOAD", &body]);
        Some(Bytes::from(out))
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Box<[u8]>, Bytes>> {
        self.bodies.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_frame_replays_a_remembered_body() {
        let scripts = Scripts::new();
        let mut req = Vec::new();
        resp::write_command(&mut req, &[b"EVALSHA", b"abc123", b"1", b"k"]);
        assert!(scripts.load_frame(&req).is_none());
        scripts.remember(b"abc123", Bytes::from_static(b"return 1"));
        let mut want = Vec::new();
        resp::write_command(&mut want, &[b"SCRIPT", b"LOAD", b"return 1"]);
        assert_eq!(scripts.load_frame(&req).as_deref(), Some(want.as_slice()));
        scripts.forget_all();
        assert!(scripts.load_frame(&req).is_none());
    }
}

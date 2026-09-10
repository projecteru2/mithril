//! SCRIPT LOAD frames carried through the proxy, replayed to a node that lost the script.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;

use crate::resp;

/// Process-wide memory of every SCRIPT LOAD frame carried.
pub struct Scripts {
    inner: Mutex<Inner>,
}

impl Scripts {
    pub fn new() -> Arc<Scripts> {
        Arc::new(Scripts {
            inner: Mutex::new(Inner {
                flushes: 0,
                loads: HashMap::new(),
            }),
        })
    }

    /// How many flushes happened so far; a LOAD is remembered only if none followed it.
    pub fn flushes(&self) -> u64 {
        self.lock().flushes
    }

    /// Keeps `load` under the sha the nodes returned for it, unless a flush came after it.
    pub fn remember(&self, sha: &[u8], load: Bytes, flushes: u64) {
        let mut inner = self.lock();
        if inner.flushes == flushes {
            inner.loads.insert(Box::from(sha), load);
        }
    }

    pub fn forget_all(&self) {
        let mut inner = self.lock();
        inner.flushes += 1;
        inner.loads = HashMap::new();
    }

    /// The SCRIPT LOAD frame for the script an EVALSHA request names, when the proxy carried it.
    pub fn load_frame(&self, evalsha: &[u8]) -> Option<Bytes> {
        let (argc, _) = resp::scan_int_line(evalsha, 1)?;
        let sha = resp::Args::new(evalsha, argc as usize).nth(1)?;
        self.lock().loads.get(sha).cloned()
    }

    /// The frame a SCRIPT LOAD of `body` sends, copied out of the request it came in.
    pub fn load_of(body: &[u8]) -> Bytes {
        let mut out = Vec::with_capacity(body.len() + 32);
        resp::write_command(&mut out, &[b"SCRIPT", b"LOAD", body]);
        Bytes::from(out)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Inner {
    flushes: u64,
    loads: HashMap<Box<[u8]>, Bytes>,
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
        scripts.remember(b"abc123", Scripts::load_of(b"return 1"), scripts.flushes());
        let mut want = Vec::new();
        resp::write_command(&mut want, &[b"SCRIPT", b"LOAD", b"return 1"]);
        assert_eq!(scripts.load_frame(&req).as_deref(), Some(want.as_slice()));
        let before = scripts.flushes();
        scripts.forget_all();
        assert!(scripts.load_frame(&req).is_none());
        scripts.remember(b"abc123", Scripts::load_of(b"return 1"), before);
        assert!(scripts.load_frame(&req).is_none());
        scripts.remember(b"abc123", Scripts::load_of(b"return 1"), scripts.flushes());
        assert!(scripts.load_frame(&req).is_some());
    }
}

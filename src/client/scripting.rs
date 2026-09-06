//! SCRIPT and FUNCTION management across the cluster, and where an EVALSHA reruns after NOSCRIPT.

use bytes::Bytes;

use super::broadcast::{Effect, Gather, Targets};
use super::local::display_name;
use super::session::Session;
use crate::command::{self, Spec};
use crate::crc16;
use crate::resp;
use crate::topology::Topology;

impl Session {
    pub(super) async fn run_script(&self, spec: &Spec, frame: Bytes, argc: usize) {
        let mut args = resp::Args::new(&frame, argc).skip(1);
        let Some(sub) = args.next() else {
            self.emit_error("ERR wrong number of arguments");
            return;
        };
        let is = |name: &[u8]| sub.eq_ignore_ascii_case(name);
        let (targets, gather, effect) = match spec.name {
            "script" if is(b"load") => {
                let Some(body) = args.next() else {
                    self.emit_error("ERR wrong number of arguments for 'script|load' command");
                    return;
                };
                // copied: the request frame is a slice of the session's read buffer
                let body = Bytes::copy_from_slice(body);
                let effect = Effect::RememberScript(body, self.shared.scripts.flushes());
                (Targets::LiveNodes, Gather::Same, Some(effect))
            }
            "script" if is(b"exists") => (Targets::Masters, Gather::Every, None),
            "script" if is(b"flush") => {
                let mode = args.next();
                let valid = mode.is_none_or(|m| {
                    m.eq_ignore_ascii_case(b"async") || m.eq_ignore_ascii_case(b"sync")
                }) && args.next().is_none();
                // forgotten at dispatch: a pipelined EVALSHA behind the flush must not reload
                if valid {
                    self.shared.scripts.forget_all();
                }
                (Targets::AllNodes, Gather::Ok, None)
            }
            "script" if is(b"help") || is(b"show") => {
                return self.forward_any_master(frame).await;
            }
            "function" if is(b"load") => (Targets::Masters, Gather::Same, None),
            "function" if is(b"delete") || is(b"flush") || is(b"restore") => {
                (Targets::Masters, Gather::Ok, None)
            }
            "function" if is(b"list") || is(b"dump") || is(b"stats") || is(b"help") => {
                return self.forward_any_master(frame).await;
            }
            _ => {
                let sub = display_name(sub);
                self.emit_error(&format!(
                    "ERR unknown subcommand '{sub}'. Try {} HELP.",
                    spec.name.to_uppercase()
                ));
                return;
            }
        };
        self.run_broadcast(frame, targets, gather, effect).await;
    }
}

/// Where an EVALSHA reloads and reruns after NOSCRIPT: the key's owner, or the first master.
pub(super) fn evalsha_target<'t>(topo: &'t Topology, req: &Bytes) -> Option<&'t str> {
    let (argc, _) = resp::scan_int_line(req, 1)?;
    let mut args = resp::Args::new(req, argc as usize);
    let spec = command::lookup(args.next()?)?;
    match spec.first_key(&mut args) {
        Some(key) => topo.owner_addr(crc16::slot(key)),
        None => topo
            .masters
            .first()
            .map(|&i| topo.nodes[i as usize].addr.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODES: &str = "\
a1 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-8191
b2 127.0.0.1:7002@17002 master - 0 0 2 connected 8192-16383
";

    fn frame(args: &[&[u8]]) -> Bytes {
        let mut out = Vec::new();
        resp::write_command(&mut out, args);
        Bytes::from(out)
    }

    #[test]
    fn evalsha_reruns_on_the_key_owner_or_a_master() {
        let topo = Topology::parse(NODES).unwrap();
        let keyed = frame(&[b"EVALSHA", b"sha", b"1", b"k"]);
        let owner = topo.owner_addr(crc16::slot(b"k")).unwrap();
        assert_eq!(evalsha_target(&topo, &keyed), Some(owner));
        let keyless = frame(&[b"EVALSHA_RO", b"sha", b"0"]);
        assert_eq!(evalsha_target(&topo, &keyless), Some("127.0.0.1:7001"));
        assert_eq!(evalsha_target(&topo, &frame(&[b"NOPE"])), None);
    }
}

# Operations

## Observability

`INFO` reports four sections. Counters are cumulative since start and
aggregated across workers; `worker_commands` breaks commands down per worker
so placement skew is visible at a glance.

| section | fields |
|---|---|
| Server | version, process id, uptime |
| Clients | `connected_clients`, `total_connections` |
| Stats | `total_commands`, `total_errors`, `redirects`, `bytes_in`, `bytes_out` |
| Mithril | `worker_threads`, `worker_commands` (per-worker), session lifecycle counters (`readers_exited`, `writers_exited`, `sessions_closed`) |

The lifecycle counters exist for deploy verification: after a binary swap,
`readers_exited`/`sessions_closed` moving under load proves which binary is
actually serving — a lesson from a benchmark campaign where three different
deploy-chain failures each silently kept an old binary running.

`CONFIG SET loglevel <level>` changes log verbosity at runtime; every other
parameter requires a restart.

## Shutdown

`SIGTERM` or `SIGINT` stops accepting, then serves open sessions until they
finish or a five-second drain deadline passes, then exits 0. Load balancers
should stop routing before signaling.

## Topology events

MOVED/ASK redirects are absorbed transparently (one retry against the named
target) and each one schedules a topology refresh, so a live slot migration
or a failover converges within the refresh debounce plus one round trip —
verified against a live `CLUSTER SETSLOT` migration under traffic. If a slot
has no known owner the client receives `-CLUSTERDOWN`; if a retry is not
possible the client receives `-TRYAGAIN` and should back off and retry.

## Deployment notes

- One mithril per host or per availability zone in front of the cluster;
  instances are stateless and independent, so run as many as needed.
- `announce-addr` must be what clients can dial — it is the address the
  cluster emulation hands out.
- Size `worker-threads` to the cores you can pin; four workers saturate a
  six-node cluster on commodity hardware before the proxy does.
- The proxy converts backend redirects it cannot retry into `-TRYAGAIN`:
  clients built for real clusters already handle it.

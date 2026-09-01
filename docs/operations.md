# Operations

## Observability

`INFO` reports five sections. Counters are cumulative since start and
aggregated across workers; `worker_commands` breaks commands down per worker
so placement skew is visible at a glance.

| section | fields |
|---|---|
| Server | `mithril_version`, `process_id`, `tcp_port`, `uptime_in_seconds`, `config_file` |
| Clients | `connected_clients` |
| CPU | `used_cpu_sys`, `used_cpu_user` |
| Stats | `total_connections_received`, `total_commands_processed`, `total_net_input_bytes`, `total_net_output_bytes`, `total_errors`, `redirections`, session lifecycle counters (`readers_exited`, `writers_exited`, `sessions_closed`) |
| Mithril | `worker_threads`, `backend_conns_per_node`, `backend_sharding`, `slave_mode`, `reply_cache`, `cache_hits`, `cache_misses`, `cache_invalidations`, `cache_entries`, `cache_bytes`, `cache_flips`, `cache_armed_workers`, `worker_commands` (per-worker) |

`CLIENT LIST` lists every connection across workers (id, addr, fd, name,
age).

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
verified against a live `CLUSTER SETSLOT` migration under traffic. Multi-key
commands that the server refuses mid-migration (`TRYAGAIN`, keys split
across source and target) are re-issued key by key, so they complete too.
If a slot has no known owner the client receives `-CLUSTERDOWN`; if a retry
is not possible the client receives `-TRYAGAIN` and should back off and
retry.

## Deployment notes

- One mithril per host or per availability zone in front of the cluster;
  instances are stateless and independent, so run as many as needed.
- `announce-addr` must be what clients can dial — it is the address the
  cluster emulation hands out.
- Size `worker-threads` to the cores you can pin; four workers saturate a
  six-node cluster on commodity hardware before the proxy does.
- The proxy converts backend redirects it cannot retry into `-TRYAGAIN`:
  clients built for real clusters already handle it.

With `reply-cache yes` every server tracks the keys the proxy caches, and a
server whose tracking table is full (`tracking-table-max-keys`, 1M by
default) spends its CPU evicting entries on every tracked command. Keep
that limit above the keys the proxy can hold per node — roughly
`reply-cache-max-bytes × workers ÷ masters ÷ entry size` — or lower the
cache budget; `INFO` on the server reports `tracking_total_keys`.

Sizing the cache: each worker caches on its own, so a hot set is held once
per worker and the memory is `reply-cache-max-bytes × workers`. Entries live
in two generations that flip when the live one reaches half the budget, so a
hot set only keeps hitting when it fits in half the per-worker budget; a
uniformly random keyspace larger than that caches at the capacity ratio
(1M keys of 64 B on 64 workers with 64mb each hit 22%) while every write
still pays the invalidation lookups, and is better served with the cache
off. Watch `cache_flips` against `cache_hits`: flips climbing while the hit
ratio stays flat means the live generation is thrashing. `cache_bytes` is
the accounted size; the process RSS runs above it by the allocator and
hash-table overhead per entry.

# Operations

## Observability

`INFO` reports seven sections. Counters are cumulative since start and
aggregated across workers; `worker_commands` breaks commands down per worker
so placement skew is visible at a glance. `total_commands_processed` and
the `cmdstat_*` calls count accepted commands (known, well-formed,
permitted); `total_error_replies` counts every error reply the proxy
writes to a client, the servers' included. CLIENT LIST names each
client's current command in `cmd`.

| section | fields |
|---|---|
| Server | `mithril_version`, `process_id`, `tcp_port`, `uptime_in_seconds`, `config_file` |
| Clients | `connected_clients` |
| CPU | `used_cpu_sys`, `used_cpu_user` |
| Stats | `total_connections_received`, `total_commands_processed`, `total_net_input_bytes`, `total_net_output_bytes`, `total_error_replies`, `redirections`, `redirect_waits`, session lifecycle counters (`readers_exited`, `writers_exited`, `sessions_closed`) |
| Mithril | `worker_threads`, `backend_conns_per_node`, `backend_sharding`, `slave_mode`, `reply_cache`, `cache_hits`, `cache_misses`, `cache_invalidations`, `cache_entries`, `cache_bytes`, `cache_flips`, `cache_armed_workers`, `worker_commands` (per-worker) |
| Cluster | `cluster_enabled` (always 1) |
| Commandstats | `cmdstat_<command>:calls=<n>` for every command run at least once, subcommands as `cmdstat_client\|list`; counted once accepted (known, well-formed, permitted), summed over workers |

`CLIENT LIST` lists every connection across workers (id, addr, fd, name,
age).

The lifecycle counters exist for deploy verification: after a binary swap,
`readers_exited`/`sessions_closed` moving under load proves which binary is
actually serving — a lesson from a benchmark campaign where three different
deploy-chain failures each silently kept an old binary running.

`CONFIG SET` changes `loglevel`, `acl-pubsub-default`, `acllog-max-len`,
`slowlog-log-slower-than` and `slowlog-max-len` at runtime; every other
parameter requires a restart.

The proxy keeps its own slow log: a command whose reply took at least
`slowlog-log-slower-than` microseconds from the moment it was read to the
moment its reply was queued for the client (the backend round trip included)
is kept, newest `slowlog-max-len` entries, and `SLOWLOG GET [count]`,
`SLOWLOG LEN` and `SLOWLOG RESET` read it in the Redis format (id, unix time,
microseconds, up to 32 arguments of up to 128 bytes, client address, client
name). The default threshold is 10 ms as in Redis; `-1` switches timing
off, `0` keeps every command. Commands the proxy answers itself are timed
too; a blocking command, a pubsub command and a cluster-wide command are not.

## Shutdown

`SIGTERM` or `SIGINT` stops accepting, then serves open sessions until they
finish or a five-second drain deadline passes, then exits 0. Load balancers
should stop routing before signaling.

## Topology events

MOVED/ASK redirects are absorbed transparently (one retry against the named
target) and each one schedules a topology refresh, so a live slot migration
or a failover converges within the refresh debounce plus one round trip —
verified against a live `CLUSTER SETSLOT` migration under traffic and
against atomic slot migrations (Redis 8.4+ `CLUSTER MIGRATION`, Valkey 9
`CLUSTER MIGRATESLOTS`), whose handoff can redirect a request back and
forth for a moment: such a request follows the redirects with short waits
between them, and `INFO` counts these as `redirect_waits`. Multi-key
commands that the server refuses mid-migration (`TRYAGAIN`) are first
retried whole with the same waits, which keeps a same-slot MSET/DEL atomic
under an atomic migration, and only re-issued key by key when the refusal
outlasts the waits (a legacy migration with the keys split across source
and target). A keyless write broadcast (FLUSHALL, FUNCTION LOAD) that a
demoted master answers `READONLY` after a failover is resent, after a
topology refresh, to the new master of that shard, with the same waits;
every such wait counts under `redirect_waits`.
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
the accounted size and tracks RSS closely while entries only accumulate
(measured: 58.9M entries on 64 workers cost 13.7 GB of RSS, which the
current accounting puts at 12.7 GB); under steady eviction churn the
allocator keeps freed pieces, and RSS ran at about twice
`reply-cache-max-bytes × workers` in a saturated run — budget for that.

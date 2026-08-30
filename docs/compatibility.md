# Compatibility

The 48-test integration suite runs against each backend, in every mode
combination (`backend-sharding`, `reply-cache`), with full cluster
teardown/recreate between versions; it includes a live slot migration
(CLUSTER SETSLOT MIGRATING/IMPORTING + MIGRATE) under multi-key commands:

| backend | server version | result |
|---|---|---|
| redis:6.2 | 6.2.24 | pass |
| redis:7.4 | 7.4.11 | pass |
| redis:8.2 | 8.2.9 | pass |
| valkey/valkey:9.1 | 9.1.1 | pass |
| redis-stable (source) | 8.10.1 | pass |

Clients verified: redis-py (cluster and standalone mode), redis-cli,
memtier_benchmark, redis-benchmark.

## Known limitations

- AUTH is single-password (`requirepass`, default user); no ACL user table.
- MULTI queues key-addressed commands only (EVAL/PING inside MULTI are
  rejected; real Redis queues them). WATCH is not supported.
- Aggregate replies to RESP3 clients keep RESP2 shape (flat arrays, not
  maps); every mainstream client parses by wire type and accepts this.
- Pubsub delivery to a slow subscriber is windowed (4096 pushes).
- Config hot-reload covers `loglevel` only; CONFIG SET rejects every other
  parameter.
- Scripting is bare EVAL only: EVALSHA, SCRIPT, FUNCTION and FCALL are not
  implemented.
- Shard pubsub (SSUBSCRIBE/SPUBLISH/SUNSUBSCRIBE) is not implemented.
- Blocking commands are the five classic ones (BLPOP, BRPOP, BRPOPLPUSH,
  BZPOPMAX, BZPOPMIN) plus blocking XREAD; BLMOVE, BLMPOP and BZMPOP are
  not implemented. Blocking commands always run on the slot's master and
  never use replica routing.
- FLUSHDB and cluster-wide KEYS are not implemented; RANDOMKEY samples one
  random master's keyspace, not the whole cluster.
- Multi-key commands with single-node semantics (MSETNX, RENAME, SMOVE,
  BITOP, the *STORE family, ...) route by their first key; the owning node
  enforces same-slot, exactly as a direct cluster connection would.
- PFCOUNT over keys in different slots sums the per-slot cardinalities
  (elements present in several slots count more than once); within one slot
  it is the server's exact union.
- During a slot migration a same-slot MSET or DEL that the server refuses
  with `TRYAGAIN` executes as independent single-key commands; a
  concurrent reader can observe it half applied.
- After a failover, until the next topology refresh (at most
  `topology-refresh-secs` or the first redirect seen), a cluster-wide
  command may reach a demoted node and return `READONLY`.
- Server-management commands are not proxied: WAIT, DEBUG, LATENCY, MEMORY,
  SHUTDOWN, FAILOVER, REPLICAOF, SAVE/BGSAVE, DUMP/RESTORE, MIGRATE and
  similar return unknown-command. OBJECT routes by its key.
- CLIENT supports ID, SETNAME, GETNAME and LIST (id, addr, fd, name, age).
- No TLS, no unix-domain listener, no slowlog, no keyspace notifications,
  no Prometheus endpoint (stats via INFO).

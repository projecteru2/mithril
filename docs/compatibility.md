# Compatibility

The 80-test integration suite runs against each backend, in every mode
combination (`backend-sharding`, `reply-cache`), with full cluster
teardown/recreate between versions; it includes a live slot migration
(CLUSTER SETSLOT MIGRATING/IMPORTING + MIGRATE) under multi-key commands:

| backend | server version | result |
|---|---|---|
| redis:6.2 | 6.2.24 | pass |
| redis:7.4 | 7.4.11 | pass |
| redis:8.2 | 8.2.9 | pass |
| redis:8.10 | 8.10.1 | pass |
| valkey/valkey:9.1 | 9.1.2 | pass |
| redis-stable (source) | 8.10.1 | pass |

Clients verified: redis-py (cluster and standalone mode), redis-cli,
memtier_benchmark, redis-benchmark.

## Known limitations

- ACL: users, passwords, command/category/subcommand rules, key and
  channel patterns, ACL LOG. Command rules apply per subcommand as in Redis
  7 (`+@all -@dangerous` removes ACL SETUSER and CONFIG SET; `+cmd|sub`
  and `-cmd|sub` both work). Not supported: selectors, ACL LOAD/SAVE (no
  ACL file), the `sanitize-payload` flags, and `CONFIG SET requirepass`
  at runtime. ACL GETUSER and ACL LOG use the Redis 6.2 reply shapes;
  GETUSER renders command rules in canonical form (`-@all +@cat...
  +cmd... +cmd|sub`).
- MULTI queues key-addressed commands only (EVAL/PING inside MULTI are
  rejected; real Redis queues them). WATCH keys and the transaction that
  follows must share one slot, and a multi-key command spanning the
  watched slot and others is refused while the watch is held; UNWATCH
  inside MULTI answers OK at once instead of being queued.
- Aggregate replies to RESP3 clients keep RESP2 shape (flat arrays, not
  maps); every mainstream client parses by wire type and accepts this.
- Pubsub delivery to a slow subscriber is windowed (4096 pushes).
- Config hot-reload covers `loglevel` only; CONFIG SET rejects every other
  parameter.
- SCRIPT KILL, SCRIPT DEBUG and FUNCTION KILL are not proxied. The
  transparent reload on `NOSCRIPT` covers scripts the proxy loaded itself,
  and only while nothing later from the same session is in flight (a
  pipelined EVALSHA, or one that was already redirected, gets the
  `NOSCRIPT` and the client's own EVAL fallback applies). FUNCTION
  commands reach the masters that own slots; a node needs its libraries
  loaded before slots move to it, as with any client.
- Shard pubsub (SSUBSCRIBE/SPUBLISH/SUNSUBSCRIBE) is not implemented.
- Blocking commands (BLPOP, BRPOP, BRPOPLPUSH, BLMOVE, BLMOVEM, BLMPOP,
  BZPOPMAX, BZPOPMIN, BZMPOP, blocking XREAD) always run on the slot's master and
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
  SHUTDOWN, FAILOVER, REPLICAOF, SAVE/BGSAVE, MIGRATE and similar return
  unknown-command. OBJECT routes by its key.
- CLIENT supports ID, SETNAME, GETNAME and LIST (id, addr, fd, name, age).
- No TLS, no unix-domain listener, no slowlog, no keyspace notifications,
  no Prometheus endpoint (stats via INFO).

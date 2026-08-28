# Compatibility

The 37-test integration suite runs against each backend with full cluster
teardown/recreate between versions:

| backend | server version | result |
|---|---|---|
| redis:6.2 | 6.2.24 | pass |
| redis:7.4 | 7.4.11 | pass |
| redis:8.2 | 8.2.9 | pass |
| valkey/valkey:9.1 | 9.1.1 | pass |

Clients verified: redis-py (cluster and standalone mode), redis-cli,
memtier_benchmark, redis-benchmark.

## Known limitations

- AUTH is single-password (`requirepass`, default user); no ACL user table.
- MULTI queues key-addressed commands only (EVAL/PING inside MULTI are
  rejected; real Redis queues them). WATCH is not supported.
- Aggregate replies to RESP3 clients keep RESP2 shape (flat arrays, not
  maps); every mainstream client parses by wire type and accepts this.
- Pubsub delivery to a slow subscriber is windowed (4096 pushes).
- Config hot-reload covers `loglevel` only.

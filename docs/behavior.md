# Behavior contract

- Single-key commands route by slot; reads optionally balance to replicas
  (`slave-mode master_readwrite|master_writeonly`). Replica reads trade
  read-your-write consistency for throughput, as with any replica routing.
- MGET/MSET/DEL/UNLINK/EXISTS/TOUCH/PFCOUNT split per slot and merge
  (order-preserving for MGET; PFCOUNT sums per-slot counts, which
  double-counts elements shared across slots — inherent to fan-out).
- MULTI/EXEC: queued locally, all keys must hash to one slot (checked at
  queue time), executed as one native transaction on the owning master.
  WATCH is not supported.
- Blocking commands and pubsub use dedicated backend connections.
- MOVED/ASK are absorbed: one transparent retry against the named target,
  plus a debounced topology refresh.
- SCAN iterates the whole cluster with synthetic cursors (master index packed
  into the high bits). DBSIZE sums masters; FLUSHALL broadcasts.
- CLUSTER NODES/SLOTS/SHARDS advertise the proxy itself as a single node
  owning all slots, so cluster-aware clients work unchanged.
- RESP3: `HELLO 3` negotiates per client; backends stay RESP2. Top-level
  nulls convert to `_`; pubsub frames convert to push type. Aggregate replies
  keep RESP2 shape (flat arrays, not maps) — every mainstream client parses
  by wire type and accepts this.
- AUTH is single-password (`requirepass`, default user). No ACL user table.
- `reply-cache yes` serves GET from a worker-local cache. Coherence: each
  worker holds one RESP3 `CLIENT TRACKING ON BCAST` connection per master and
  drops entries the moment any write is broadcast; writes through the proxy
  also drop their keys synchronously on their worker, so a session always
  reads its own writes. Residual staleness is the invalidation push latency
  (cross-client, sub-millisecond) plus, for keys expiring server-side, the
  active-expire cycle; `reply-cache-max-age-secs` caps both. The cache only
  serves while every master's tracking connection is up — coverage loss
  flushes it and pauses fills. Enable it for read-dominant traffic: the
  BCAST stream delivers every cluster write to every worker, so at
  write-heavy ratios the invalidation processing outweighs the hits.


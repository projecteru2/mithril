# Behavior contract

- Single-key commands route by slot; reads optionally balance to replicas
  (`slave-mode master_readwrite|master_writeonly`). Replica reads trade
  read-your-write consistency for throughput, as with any replica routing.
- MGET/MSET/DEL/UNLINK/EXISTS/TOUCH/PFCOUNT split per slot and merge
  (order-preserving for MGET; PFCOUNT sums per-slot counts, which
  double-counts elements shared across slots — inherent to fan-out).
- MULTI/EXEC: queued locally, all keys must hash to one slot (checked at
  queue time), executed as one native transaction on the owning master.
  WATCH holds its keys on an exclusive connection to the slot's master;
  every command whose keys all live in that slot, and the EXEC, run there
  too (uncached, never on a replica). The WATCH takes effect once every
  earlier request of the session has answered (blocking commands, fan-outs
  and redirect retries included), so the optimistic-locking pattern works
  unchanged; the session keeps reading meanwhile, and requests for the
  slot queue behind the WATCH. A watching connection that dies is dropped
  at the session's next command on that slot (or with the session); from
  then on the slot answers a lost-connection error until UNWATCH, DISCARD
  or RESET, or a new WATCH replaces it, so EXEC never runs unguarded.
  UNWATCH, DISCARD, RESET and a refused EXEC send UNWATCH behind the
  requests the connection already accepted; the slot keeps routing there
  until the connection is quiet, as it does around an EXEC, so pipelined
  neighbours stay ordered and the connection returns to the pool only once
  everything it accepted has answered. The EXEC or release reply arrives
  when the connection is quiet, or at once when the client already queued
  more behind it. A FLUSHALL issued while watching makes the EXEC answer
  nil, as it would on one node.
- Blocking commands and pubsub use dedicated backend connections.
- SELECT n (a Valkey 9 cluster with `cluster-databases`) moves the session
  onto connections bound to that database; the reply cache serves
  database 0 only.
- MOVED/ASK are absorbed: one transparent retry against the named target,
  plus a debounced topology refresh.
- An atomic slot migration (Valkey 9 `CLUSTER MIGRATESLOTS`, Redis 8.4
  `CLUSTER MIGRATION`) hands the slot over while its two nodes briefly
  disagree on the owner, so a request can be redirected back and forth: a
  single-slot request redirected a second time follows up to six further
  redirects with a wait before each (2 ms, doubling) instead of failing,
  and the client sees only its reply. A pipelined client with later
  requests already in flight receives `TRYAGAIN` for it instead, as with
  the multi-key case below.
- A keyless write broadcast (FLUSHALL, FUNCTION LOAD) that a master demoted
  by a failover answers `READONLY` is resent, after a topology refresh, to
  the new master of that node's shard, with the same waits; the replies of
  the other masters stand, so a FUNCTION LOAD is never repeated where it
  already took. The session waits for a broadcast's replies, so a later
  command of the same client never overtakes it.
- A multi-key command the server refuses mid-migration (`TRYAGAIN`) is
  first retried whole with the same waits, so under an atomic migration a
  same-slot MSET/DEL stays atomic; only when the refusal outlasts the waits
  (a legacy migration with the keys split across source and target) is it
  re-issued key by key, no longer atomic. A redirect that outlasts the waits
  ends the command with `TRYAGAIN` instead, never split. A pipelined client
  with requests queued behind such a command receives one `TRYAGAIN` for it
  before the session switches that slot to the ordered path. PFCOUNT is
  never split this way.
- An EVALSHA that meets `NOSCRIPT` after a redirect is reloaded at the node
  the redirect named, while the topology refresh is still pending.
- After a failover the proxy's topology lags by at most one refresh
  (`topology-refresh-secs`, or the first redirect it sees): in that window a
  cluster-wide command can reach a demoted node and return `READONLY`, and a
  keyed command is redirected transparently.
- SCAN iterates the whole cluster with synthetic cursors (master index packed
  into the high bits). DBSIZE sums masters; FLUSHALL broadcasts.
- CLUSTER NODES/SLOTS/SHARDS advertise the proxy itself as a single node
  owning all slots, so cluster-aware clients work unchanged.
- RESP3: `HELLO 3` negotiates per client; backends stay RESP2. Top-level
  nulls convert to `_`; pubsub frames convert to push type. Aggregate replies
  keep RESP2 shape (flat arrays, not maps) — every mainstream client parses
  by wire type and accepts this.
- AUTH and `HELLO ... AUTH` resolve users from the proxy's ACL table:
  `requirepass` is the `default` user's password, `user` lines and ACL
  SETUSER define the rest. An unrestricted user (all commands, keys and
  channels — the default user as configured) costs nothing per command; a
  restricted user is checked before dispatch: the command or its allowed
  subcommand, every key the request touches (the same walker the reply
  cache and MULTI use), and the channels of PUBLISH/SUBSCRIBE/PSUBSCRIBE.
  AUTH, HELLO, QUIT and RESET are never subject to rules (as in Redis), so
  a restricted session can switch users with valid credentials. A denial
  answers NOPERM, aborts an open MULTI and lands in ACL LOG. Rule
  changes reach connected sessions at their next command; a deleted user's
  sessions close.
- `reply-cache yes` serves GET and MGET (up to 64 keys) from a worker-local cache. Coherence: every
  backend connection redirects RESP3 key tracking to a per-worker tracker
  connection and opts each cached read in (`CLIENT CACHING YES`), so a server
  invalidates exactly the keys this proxy holds, once, when they change;
  writes through the proxy also drop their keys synchronously on their
  worker, so a session always reads its own writes. Residual staleness is
  the invalidation push latency (cross-client, sub-millisecond) plus, for
  keys expiring server-side, the active-expire cycle — the server sends an
  invalidation when it deletes the expired key, measured at ≤105 ms on Redis
  8.0 (`hz 10`) and ~2 ms on Redis 8.10 / Valkey 9.1; `reply-cache-max-age-secs`
  caps both. The cache serves only while every master's tracker is up —
  coverage loss flushes it and pauses fills. Replica reads
  (`slave-mode`) are never cached.


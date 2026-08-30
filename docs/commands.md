# Command handling

Every command mithril accepts is in a static table carrying its arity,
flags, key positions and routing kind. Unknown commands get
`ERR unknown command` without touching a backend.

## Routed by key slot

Single-key commands (strings, hashes, lists, sets, sorted sets, streams,
geo, bitmaps, HyperLogLog updates, EXPIRE family, SORT, per-key scans
HSCAN/SSCAN/ZSCAN, OBJECT subcommands, ...) hash their key and go to the owning master — or to a
replica when [read splitting](configuration.md) applies and the command is
read-only. Multi-key commands with single-node semantics (RENAME, SMOVE,
MSETNX, BITOP, the *STORE family, RPOPLPUSH, ...) route by their first key;
the owning node enforces same-slot, exactly as a direct cluster connection
would. EVAL routes by its first key (or any master when it has none).

## Fanned out per slot

MGET, MSET, DEL, UNLINK, EXISTS, TOUCH and PFCOUNT split per slot, execute
in parallel, and merge: MGET order-preserving, MSET all-OK, the rest summed.
A redirected part is retried once against its new owner before merging.
A part answered `TRYAGAIN` (its keys split across a migrating slot) is
re-issued key by key so each request follows `ASK`; a single-slot multi-key
command degrades the same way when nothing is queued behind it, otherwise
the `TRYAGAIN` reaches the client and the session routes that slot's
multi-key commands through the ordered fan-out path until the topology
changes. During a migration a same-slot MSET or DEL therefore executes as
independent single-key commands rather than one atomic command. PFCOUNT is
the exception: over several keys it counts one union, so it is not
re-issued key by key and the `TRYAGAIN` reaches the client. Cluster-wide
commands (DBSIZE, FLUSHALL, SCAN) wait for every pending fan-out first.
Keys that all hash to one slot (hash tags) skip the split: the command
routes as a single request with no merge step.

## Cluster-wide

SCAN iterates the whole cluster with synthetic cursors (master index packed
into the cursor's high bits), DBSIZE sums all masters, FLUSHALL broadcasts
and requires every master to acknowledge.

## Transactions

MULTI queues key-addressed commands locally, enforcing single-slot at queue
time (`CROSSSLOT` otherwise); EXEC ships the whole transaction as one native
MULTI/EXEC to the owning master. DISCARD is supported; WATCH is not.

## Blocking and pubsub

BLPOP, BRPOP, BRPOPLPUSH, BZPOPMAX, BZPOPMIN and blocking XREAD run on
dedicated backend connections and always on the slot's master.
(P)SUBSCRIBE/(P)UNSUBSCRIBE run over a per-client pubsub connection with
fully ordered confirmations; PUBLISH and PUBSUB forward to a master. Under
RESP3, subscribed clients may keep issuing regular commands.

## Answered by the proxy

PING, ECHO, SELECT (db 0 only), TIME, AUTH, HELLO, RESET, QUIT, INFO,
CONFIG (GET; SET accepts only `loglevel`), CLIENT (ID/SETNAME/GETNAME/LIST),
COMMAND (table introspection), ACL WHOAMI, MULTI/EXEC/DISCARD, and the
CLUSTER family. CLUSTER INFO/MYID/KEYSLOT/NODES/SLOTS/SHARDS describe a
single virtual node owning slots 0-16383 — the emulation that lets
cluster-aware clients treat the proxy as the whole cluster.

## Not implemented

Requests for these return unknown-command; none of them silently
misbehaves:

- scripting beyond bare EVAL: EVALSHA, SCRIPT, FUNCTION, FCALL
- shard pubsub: SSUBSCRIBE, SPUBLISH, SUNSUBSCRIBE
- newer blocking forms: BLMOVE, BLMPOP, BZMPOP
- WATCH/UNWATCH, FLUSHDB, cluster-wide KEYS
- server management: WAIT, DEBUG, LATENCY, MEMORY, SHUTDOWN,
  FAILOVER, REPLICAOF, SAVE/BGSAVE, DUMP/RESTORE, MIGRATE and similar

RANDOMKEY samples one random master's keyspace, not the whole cluster.

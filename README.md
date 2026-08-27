# Mithril

A Redis Cluster proxy in Rust: clients speak RESP2 or RESP3 to a single
endpoint; mithril routes to the cluster, absorbs topology changes, and fans
out multi-key commands.

## Architecture

Thread-per-core: each worker thread runs a single-threaded tokio runtime with
its own SO_REUSEPORT listener, its own backend connection pools, and no
cross-worker communication on the request path. The cluster topology is a
shared immutable snapshot behind `arc-swap`, refreshed by a dedicated thread
every `topology-refresh-secs` and on demand after MOVED/ASK redirects.

Requests are forwarded as zero-copy byte ranges (`bytes::Bytes` slices of the
input buffer); the RESP layer locates frame boundaries and argument positions
without materializing values. Backend replies flow back as frame slices and
are re-ordered per client by sequence number, so pipelining is preserved
end to end.

## Module map

| module | role |
|---|---|
| `resp` | RESP2/RESP3 frame scanner, argument iterator, command writer |
| `crc16` | cluster CRC16 and hash-tag slot mapping |
| `command` | static command table: arity, flags, key positions, routing kind |
| `topology` | CLUSTER NODES parsing, slot-to-node map |
| `route` | node selection incl. replica read splitting |
| `backend` | per-node pipelined connections, exclusive conns for blocking/pubsub |
| `client` | session state machine: dispatch, MULTI, pubsub relay, redirects |
| `multikey` | multi-key split by slot and reply merging |
| `admin` | proxy-answered commands, single-virtual-node cluster emulation |
| `server` | worker startup, listeners, topology refresher, shutdown |
| `config`, `stats`, `log` | configuration, counters, logging |

## Behavior contract

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

## Build and test

```
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
cargo build --release
```

Integration tests (`it/`) and the benchmark/cluster environment live in the
sibling `testenv/` directory; see `testenv/README.md`.

## Configuration

`mithril <conf-file> [--<key> <value>]...` — `key value` lines, `#` comments.

| key | default | notes |
|---|---|---|
| bind / port | 0.0.0.0 / 7979 | SO_REUSEPORT, one listener per worker |
| announce-addr | bind:port | address advertised by cluster emulation |
| bootstrap | (required) | comma-separated seed node list |
| worker-threads | CPU count | |
| backend-conns | 1 | shared pipelined conns per node per worker |
| maxclients | 10000 | |
| requirepass | empty | client auth |
| backend-auth-user/-pass | empty | auth towards backends |
| slave-mode | off | off / master_readwrite / master_writeonly |
| query-buffer-limit | 1gb | per-client input cap |
| topology-refresh-secs | 15 | |
| tcp-keepalive | 300 | |
| loglevel | notice | debug/verbose/notice/warning; hot via CONFIG SET |

Config changes other than `loglevel` require a restart.

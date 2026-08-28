# Architecture

Thread-per-core: each worker thread runs a single-threaded tokio runtime with
its own backend connection pools and no cross-worker communication on the
request path; a central acceptor thread owns the listener and places
connections round-robin (see below). The cluster topology is a
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

## Reply ordering

Each client session runs a reader task and a writer task. Every command is
assigned a monotonically increasing sequence; backend replies carry their
sequence back and the writer emits them in order, parking out-of-order
arrivals. Pubsub confirmations are sequenced the same way, while published
messages flow out-of-band behind an explicit ordering barrier (a push never
overtakes the confirmation it followed) under a bounded push window.

## Accept model

One acceptor thread owns the listener and hands accepted sockets to workers
round-robin over bounded channels: kernel SO_REUSEPORT hashing distributes
connections binomially (measured 26% worker skew at 100 connections), and at
saturation the heaviest worker sets the throughput floor.

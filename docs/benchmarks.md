# Benchmarks

## Realistic deployment matrix (cocoon-test, 2026-08-30)

Host: 384-core Linux box shared with other work. Backing cluster: 32 nodes
(16 masters + 16 replicas, redis 8.10) pinned to 32 cores. Every proxy runs
8 worker threads on its own 8 cores; the load generator runs on separate
cores (memtier_benchmark 8 threads, 20-second windows; redis-benchmark
8 threads, 12M requests). Arms are interleaved in one run with the order
reversed on the second pass; the ranges below are the two passes. 64-byte
values unless stated, 100k keys, random keys, SET:GET 1:1 unless stated.

Columns: **mithril** (default), **shard** (`backend-sharding yes`),
**cache** (`reply-cache yes`), **shard+cache** (both), mt-proxy (C++),
predixy (C++). Commit 9336a89 lineage.

### Throughput (ops/s)

| cell | mithril | shard | cache | shard+cache | mt-proxy | predixy |
|---|---|---|---|---|---|---|
| P1, 200 conns | 429k | 468-470k | **597-607k** | 487-491k | 470k | 405-407k |
| P1, 400 conns | 485-487k | 554-555k | **614-615k** | 548-564k | 569-576k | 468-475k |
| P1, 400 conns, GET only | 484k | 555-560k | **621-623k** | 603-608k | 577-578k | 469-471k |
| P1, 4 KiB values | 387-393k | 406k | **530-538k** | 445-446k | 415-416k | 354-357k |
| P4, 512 B values | 950-959k | 1.07M | 1.20-1.22M | **1.37M** | 958-961k | 970-977k |
| P16 | 2.72-2.78M | 2.70M | 3.04-3.05M | **3.12-3.15M** | 2.32M | 3.02-3.03M |
| P16, 4 KiB values | 944-957k | 933-939k | **995k-1.01M** | 998k-1.00M | 911-917k | 932-941k |
| redis-benchmark GET, P1, 400 conns | 414k | **494-505k** | 489-494k | 489k | 480-484k | 428k |

### Tail latency (p99 / p999, ms)

| cell | mithril | cache | shard+cache | mt-proxy | predixy |
|---|---|---|---|---|---|
| P1, 200 conns | 0.60 / 0.67 | **0.49 / 0.58-0.61** | 0.73 / 0.90-0.92 | 0.70 / 0.80 | 0.66 / 0.74-0.76 |
| P1, 400 conns | 1.08 / 1.40-1.58 | **0.95 / 1.23-1.32** | 1.30-1.32 / 1.70-1.88 | 1.13-1.14 / 1.40-1.47 | 1.11-1.13 / 1.49-1.72 |
| P1, 4 KiB values | 0.68-0.69 / 0.82-0.86 | **0.61-0.63 / 0.82-0.92** | 0.91 / 1.15-1.16 | 0.80 / 0.93-0.94 | 0.75-0.76 / 0.86-1.02 |
| P4, 512 B values | 1.21-1.23 / 1.30-1.38 | **0.94-0.95 / 1.06-1.07** | 1.02-1.03 / 1.26-1.30 | 1.30 / 1.51-1.54 | 1.07 / 1.14-1.30 |
| P16 | 1.54-1.58 / 1.62-1.75 | 1.47-1.48 / **1.60-1.66** | 1.75-1.78 / 2.08-2.37 | 2.21 / 2.51-2.53 | **1.45** / 1.68-1.75 |
| P16, 4 KiB values | 4.38-4.45 / **5.15-6.91** | 4.29-4.35 / 6.30-7.07 | 4.58-4.74 / 8.32-8.64 | 5.34-5.54 / 6.21-7.26 | **4.19** / 5.38-5.79 |

Reading it: with the reply cache on, mithril holds the throughput lead in
every cell (the redis-benchmark ceiling goes to its sharding mode, with
cache and shard+cache inside 2%) and the best p99 in six of eight; predixy
keeps a 2-3% p99 edge at pipeline 16 while losing on p999. The cache does
not need a read-only workload: these cells are 1:1 SET/GET, and the
server-tracked invalidation keeps the cache coherent — the same session
always reads its own writes, other sessions see a write within the
invalidation push latency.

Without the cache, mithril's default mode is the tail-latency choice in the
pipelined cells and mt-proxy's single-connection-per-node design wins the
unpipelined saturation cells — which `backend-sharding` recovers.

### backend-sharding auto (same rig, commit e6e1592 lineage)

`auto` decides per worker and per session: a worker that is busy with
thin local batches moves every session it hosts to the shared pipes, a
lightly loaded worker keeps its own connections, and an unpipelined
session prefers the shared pipe on its own (see architecture.md). Two
rounds each, redis-benchmark unless stated:

| cell | default | shard | auto |
|---|---|---|---|
| P16, 1000 conns GET | 5.58M / p50 2.86 | 8.95M / 0.94 | **8.85-8.91M / 0.93** |
| P16, 1000 conns SET | 5.54M / 2.89 | 9.02M / 0.93 | **9.13-9.20M / 0.91** |
| P16, 2000 conns GET | 6.70M / 4.52 | 9.67M / 1.86 | **10.4M / 1.8** |
| P64, 1000 conns GET | 10.66M / 5.80 | 12.62M / 2.89 | 10.8-14.4M / 1.9-3.0 |
| P1, 2000 conns GET | 2.20M / 0.89 | 2.63M / 0.71 | **2.66M / 0.70** |
| P16, 256 conns GET | 4.78M / 0.84 | 5.49M / 0.33 | **5.56-5.66M / 0.31** |
| memtier P16, 200 conns (sliding) | **4.36M / 0.76** | 3.59M / 0.92 | 3.74M / 0.86 |
| memtier P1, 400 conns | 445k | 443k | 443k |

`auto` lands on the better of the two paths in every cell but one: a
sliding-pipeline client at moderate concurrency on a partly idle proxy
runs 14% slower than default, because the shared pipes cost a hop those
sessions cannot hide. If that is your only workload, set `no`.

## Bare-metal reference (2026-08, 4-worker proxies, 6-node cluster)

Method: memtier_benchmark on a 16-core bare-metal Linux host, cpuset-pinned
(redis on 6 cores, the proxy under test on 4, memtier on 6), interleaved arms
with the order swapped per round, 6 samples per arm, 64-byte values, 1:1
SET/GET over 100k keys. Backing cluster: redis 6.2, 3 masters + 3 replicas.

| target | pipeline=1 ops/s | pipeline=16 ops/s |
|---|---|---|
| direct to cluster (no proxy) | 763k | 4.09M |
| **mithril** | **944k** | **4.62M** |
| mt-proxy (C++) | 961k | 3.28M |
| predixy | 865k | 4.35M |

At pipeline 16 mithril is the fastest of everything measured — including the
direct arm, because request batching reduces per-op syscall load on the
single-threaded redis processes. At pipeline 1 mithril trails mt-proxy by
~1.7%: mt-proxy handles a request inline in a single epoll loop, while
mithril pays reader-to-writer task handoffs that pipelining amortizes.

Numbers from Docker-for-Mac invert the ranking and must not be used for
comparisons.

# Benchmarks

Method: memtier_benchmark on a 16-core bare-metal Linux host, cpuset-pinned
(redis on 6 cores, the proxy under test on 4, memtier on 6), interleaved arms
with the order swapped per round, 6 samples per arm, 64-byte values, 1:1
SET/GET over 100k keys. Backing cluster: redis 6.2, 3 masters + 3 replicas.
Every proxy runs 4 worker threads.

| target | pipeline=1 ops/s | pipeline=16 ops/s |
|---|---|---|
| direct to cluster (no proxy) | 766k | 4.08M |
| **mithril** | **946k** | **4.64M** |
| mt-proxy (C++) | 963k | 3.28M |
| predixy | 866k | 4.35M |

At pipeline 16 mithril is the fastest of everything measured — including the
direct arm, because request batching reduces per-op syscall load on the
single-threaded redis processes. At pipeline 1 mithril trails mt-proxy by
~1.4%: mt-proxy handles a request inline in a single epoll loop, while
mithril pays reader-to-writer task handoffs that pipelining amortizes.

Numbers from Docker-for-Mac invert the ranking and must not be used for
comparisons.

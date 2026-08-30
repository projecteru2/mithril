# mithril

A fast Redis Cluster proxy in Rust: clients speak RESP2 or RESP3 to a single
endpoint; mithril routes to the cluster, absorbs topology changes (MOVED/ASK,
live slot migrations), and fans out multi-key commands — with a
thread-per-core runtime and zero-copy frame forwarding.

**Documentation: [projecteru2.github.io/mithril](https://projecteru2.github.io/mithril/)** (source in [`docs/`](docs/)).

[![test](https://github.com/projecteru2/mithril/actions/workflows/test.yml/badge.svg)](https://github.com/projecteru2/mithril/actions/workflows/test.yml)
[![lint](https://github.com/projecteru2/mithril/actions/workflows/lint.yml/badge.svg)](https://github.com/projecteru2/mithril/actions/workflows/lint.yml)

## Highlights

- **Thread-per-core** — each worker runs a single-threaded tokio runtime with
  its own backend pools; by default nothing crosses workers on the request
  path, and a central acceptor places each connection on the least-loaded
  worker (configurable) so no worker becomes the latency floor. The optional
  `backend-sharding` mode trades that isolation for one process-wide pipe per
  node, deepening backend batches for unpipelined workloads — and `auto`
  makes that call per session, so unpipelined and pipelining clients each
  get the path that is faster for them
- **Zero-copy pipeline** — requests and replies travel as `bytes::Bytes`
  slices of the socket buffers; the RESP layer finds frame boundaries without
  materializing values, and replies re-order per client by sequence number so
  pipelining is preserved end to end
- **Full cluster absorption** — slot routing with per-slot multi-key fan-out,
  transparent MOVED/ASK retry, multi-key commands that ride out a migrating
  slot (a `TRYAGAIN` part is re-issued key by key), all verified against
  live slot migrations, and single-virtual-node cluster emulation so
  cluster-aware clients work unchanged against one endpoint
- **Reply cache** — optional worker-local GET/MGET cache kept coherent by the
  cluster itself: every backend connection redirects RESP3 key tracking to
  a per-worker tracker and opts each cached read in, so the servers
  invalidate exactly the keys the proxy holds; writes through the proxy
  invalidate synchronously (read-your-writes), and coverage loss flushes
- **RESP2 + RESP3** — per-client `HELLO` negotiation, push-type pubsub frames,
  null conversion; backends stay RESP2
- **The hard paths done right** — MULTI/EXEC as native transactions,
  blocking commands and pubsub on dedicated backend connections with fully
  ordered subscription confirmations, cluster-wide SCAN with synthetic
  cursors, replica read splitting
- **Fast** — on a 32-node cluster with 8-worker proxies, mithril with the
  reply cache leads every cell of an 8-cell memtier/redis-benchmark matrix
  against mt-proxy and predixy (pipeline 1 through 16, 64 B to 4 KiB values,
  mixed 1:1 SET/GET), with the best p99 in six of them; see
  [benchmarks](https://projecteru2.github.io/mithril/benchmarks.html)

## Quick start

```shell
make build

./target/release/mithril mithril.conf.sample --bootstrap 127.0.0.1:7001

# or in docker: announce-addr must be the address clients can reach,
# or cluster-aware clients would be redirected to 0.0.0.0
docker run --rm -p 7979:7979 ghcr.io/projecteru2/mithril \
  /etc/mithril/mithril.conf.sample --bootstrap host:port \
  --announce-addr <externally-routable-ip>:7979
```

Point any Redis client — cluster-aware or not — at port 7979.

## Configuration

`mithril <conf-file> [--<key> <value>]...` — `key value` lines, `#` comments.
See [`mithril.conf.sample`](mithril.conf.sample) and the
[configuration reference](https://projecteru2.github.io/mithril/configuration.html).

## Develop

```shell
make test lint fmt-check   # the CI gate
```

The integration suite lives in [`it/`](it/): 48 dockerized tests driving a
real 3-master/3-replica cluster through the proxy with redis-py — including
a live slot migration under multi-key commands — run against redis 6.2
through 8.2 and valkey 9.1 in every mode combination (`backend-sharding`,
`reply-cache`).

## License

[AGPL-3.0](LICENSE)

# mithril

Mithril is a Redis Cluster proxy. Clients — cluster-aware or not — connect to
one endpoint speaking RESP2 or RESP3; mithril routes every command to the
right node of the backing cluster, absorbs topology changes as they happen,
fans multi-key commands out per slot, and merges the replies. To a client it
looks like a single Redis node that happens to own all 16384 slots.

```
redis clients (RESP2 / RESP3, cluster-aware or not)
        │
        ▼
  ┌──────────────────────────────────────────────┐
  │ mithril                                      │
  │   acceptor ──► worker threads (one per core) │  least-loaded placement
  │                 │                            │
  │                 ├─ session: parse ► dispatch │  zero-copy frames
  │                 │    │ ordered reply stream  │  sequence numbers
  │                 │    ├─► slot router ────────┼─► shared conns per node
  │                 │    ├─► multi-key fan-out   │  per-slot split + merge
  │                 │    └─► pubsub / blocking ──┼─► dedicated conns
  │                 │                            │
  │   topology refresher ◄── CLUSTER NODES ──────┼─► redis / valkey cluster
  └──────────────────────────────────────────────┘
```

## Guides

- [Installation](installation.md) — docker image, building from source, the
  binary's command line
- [Configuration](configuration.md) — every key mithril reads, with types
  and defaults
- [Architecture](architecture.md) — threading model, zero-copy pipeline,
  reply ordering, topology handling
- [Command handling](commands.md) — how each command class is routed,
  what the proxy answers itself, and what is not implemented
- [Behavior contract](behavior.md) — the guarantees clients can rely on
- [Operations](operations.md) — INFO fields, shutdown, deployment notes
- [Benchmarks](benchmarks.md) — method and numbers against predixy,
  mt-proxy and a direct cluster connection
- [Compatibility](compatibility.md) — backend versions, verified clients,
  known limitations

## Repository

Source and issue tracker: [github.com/projecteru2/mithril](https://github.com/projecteru2/mithril).
Part of the [Eru](https://github.com/projecteru2) stack.

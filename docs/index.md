# mithril

A fast Redis Cluster proxy in Rust: clients speak RESP2 or RESP3 to a single
endpoint; mithril routes to the cluster, absorbs topology changes (MOVED/ASK,
live slot migrations), and fans out multi-key commands — with a thread-per-core
runtime and zero-copy frame forwarding.

- [Architecture](architecture.md)
- [Behavior contract](behavior.md)
- [Configuration](configuration.md)
- [Benchmarks](benchmarks.md)
- [Compatibility](compatibility.md)

## Quick start

```shell
make build

./target/release/mithril mithril.conf.sample --bootstrap 127.0.0.1:7001

# or in docker: announce-addr must be the address clients can reach
docker run --rm -p 7979:7979 ghcr.io/projecteru2/mithril \
  /etc/mithril/mithril.conf.sample --bootstrap host:port \
  --announce-addr <externally-routable-ip>:7979
```

Point any Redis client — cluster-aware or not — at port 7979.

## License

AGPL-3.0.

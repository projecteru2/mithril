# Installation

## Container image

Multi-arch (amd64, arm64) images are published on every release tag and on
master:

```shell
docker pull ghcr.io/projecteru2/mithril        # or projecteru2/mithril on Docker Hub

docker run --rm -p 7979:7979 ghcr.io/projecteru2/mithril \
  /etc/mithril/mithril.conf.sample \
  --bootstrap <node>:<port> \
  --announce-addr <externally-routable-ip>:7979
```

`announce-addr` must be an address clients can reach: it is what the cluster
emulation advertises, and the sample config binds the wildcard address.

## Building from source

Requires the Rust toolchain pinned in `rust-toolchain.toml` (rustup picks it
up automatically):

```shell
make build            # release binary at target/release/mithril
make test lint        # the CI gate: cargo test + clippy -D warnings
```

The `Makefile` injects the git tag and revision into `--version`.

## Running

```shell
mithril <conf-file> [--<key> <value>]...
```

Every config key can be overridden on the command line; see
[configuration](configuration.md). The process runs in the foreground and
logs to stdout, so it drops into a container, a systemd unit
(`Type=simple`), or a supervisor unchanged. `SIGTERM`/`SIGINT` trigger a
graceful drain (stop accepting, serve open sessions up to five seconds,
exit 0).

At startup mithril must reach at least one `bootstrap` node to fetch the
initial topology; it retries for thirty seconds before giving up. After
that, any node of the cluster can serve refreshes.

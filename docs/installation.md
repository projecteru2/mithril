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

## Release binaries

Every GitHub release carries static Linux binaries (x86_64 and arm64, musl)
and a macOS arm64 binary, each as a tarball with the sample config, plus a
`checksums.txt`:

```shell
V=0.1.6
curl -LO https://github.com/projecteru2/mithril/releases/download/v$V/mithril_${V}_Linux_x86_64.tar.gz
curl -LO https://github.com/projecteru2/mithril/releases/download/v$V/checksums.txt
grep "mithril_${V}_Linux_x86_64" checksums.txt | sha256sum -c
tar xzf mithril_${V}_Linux_x86_64.tar.gz && ./mithril --version
```

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

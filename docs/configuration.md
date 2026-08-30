# Configuration

`mithril <conf-file> [--<key> <value>]...` — the file holds `key value`
lines with `#` comments; every key can also be given on the command line as
`--key value`, which wins over the file. Unknown keys are startup errors.
See [`mithril.conf.sample`](https://github.com/projecteru2/mithril/blob/master/mithril.conf.sample).

| key | type | default | meaning |
|---|---|---|---|
| `bind` | address | `0.0.0.0` | listen address (IPv4 or IPv6 literal); one socket, owned by the acceptor thread |
| `port` | u16 | `7979` | listen port |
| `announce-addr` | addr:port | `bind:port` | address the cluster emulation advertises to clients; must be externally routable when `bind` is a wildcard |
| `bootstrap` | list | required | comma-separated seed nodes of the backing cluster |
| `worker-threads` | int | CPU count | worker threads, one runtime each |
| `maxclients` | int | `10000` | client connection cap, enforced at accept |
| `backend-conns` | 1..512 | `1` | shared pipelined connections per node per worker |
| `backend-sharding` | `yes`/`no` | `no` | one process-wide connection per node, owned by the worker its address hashes to; deepens backend pipelines for unpipelined workloads |
| `reply-cache` | `yes`/`no` | `no` | worker-local GET reply cache; the servers track the keys the proxy caches (redirected opt-in RESP3 tracking) and invalidate them on change, so hits skip the backend round trip and a session reads its own writes |
| `reply-cache-max-bytes` | bytes | `64mb` | per-worker cache budget (two generations under one budget) |
| `reply-cache-max-age-secs` | 1..3600 | `10` | staleness backstop for missed invalidations; entries older than this never serve |
| `requirepass` | string | empty | client password (default user); empty disables AUTH |
| `backend-auth-user` | string | empty | username sent to backends (`AUTH user pass`) |
| `backend-auth-pass` | string | empty | password sent to backends |
| `slave-mode` | enum | `off` | replica read splitting: `off`, `master_readwrite`, `master_writeonly` |
| `placement` | enum | `least-loaded` | new-connection placement: `least-loaded` (avoid busy workers; decays to rotation under even load) or `round-robin` |
| `tcp-keepalive` | seconds | `300` | keepalive on backend connections |
| `query-buffer-limit` | bytes | `1gb` | per-client input cap; accepts `kb`/`mb`/`gb` suffixes; also bounds queued MULTI bytes |
| `topology-refresh-secs` | 1..3600 | `15` | periodic CLUSTER NODES refresh; redirects trigger one immediately (debounced 100ms) |
| `loglevel` | enum | `notice` | `debug`, `verbose`, `notice`, `warning`; the only key changeable at runtime via `CONFIG SET` |

## Replica read splitting

With `slave-mode master_writeonly`, read-only commands go to a replica of
the owning master (uniformly among replicas); with `master_readwrite` the
master participates in the draw too. Writes, EVAL, transactions and blocking
commands always run on the master. Replica reads trade read-your-write
consistency for throughput, as with any replica routing.

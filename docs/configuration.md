# Configuration

`mithril <conf-file> [--<key> <value>]...` — `key value` lines, `#` comments.

| key | default | notes |
|---|---|---|
| bind / port | 0.0.0.0 / 7979 | one listener, owned by the acceptor thread |
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


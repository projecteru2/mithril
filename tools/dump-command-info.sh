#!/bin/sh
# Refreshes tools/command-info.json from a running Redis and Valkey (redis-cli/valkey-cli with --json).
set -e
cd "$(dirname "$0")"
dump() { "$1" -h "$2" -p "$3" --json command info $("$1" -h "$2" -p "$3" command list | tr '\n' ' '); }
python3 - "$(dump redis-cli "${REDIS_HOST:-127.0.0.1}" "${REDIS_PORT:-6379}")" "$(dump valkey-cli "${VALKEY_HOST:-127.0.0.1}" "${VALKEY_PORT:-6380}")" <<'PY'
import json, sys
out = {}
for tag, raw in (("redis", sys.argv[1]), ("valkey", sys.argv[2])):
    rows = []
    for x in json.loads(raw):
        if not x:
            continue
        specs = []
        for k in x[8]:
            b, f = k["begin_search"], k["find_keys"]
            specs.append({"begin": b["type"], "at": b["spec"].get("index", b["spec"].get("keyword")), "find": f["type"], "spec": f["spec"]})
        rows.append({"name": x[0], "arity": x[1], "flags": x[2], "first": x[3], "last": x[4], "step": x[5], "cats": x[6], "keys": specs})
    out[tag] = rows
json.dump(out, open("command-info.json", "w"), separators=(",", ":"), sort_keys=True)
PY

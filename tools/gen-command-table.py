#!/usr/bin/env python3
"""Generates src/command/table.rs from tools/command-info.json (COMMAND INFO of Redis and Valkey).

Routing kinds and mithril-specific flags live in the override tables below; everything else
(arity, key positions, Redis flags, ACL categories) is taken from the servers themselves.
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
INFO_PATH = os.path.join(HERE, "command-info.json")
OUT = os.path.join(HERE, "..", "src", "command", "table.rs")

with open(INFO_PATH, encoding="utf-8") as source:
    INFO = json.load(source)
for rows in INFO.values():
    for row in rows:
        row["name"] = row["name"].lower()

STD_FLAGS = ["write", "readonly", "denyoom", "module", "admin", "pubsub", "noscript", "blocking",
             "loading", "stale", "skip_monitor", "skip_slowlog", "asking", "fast", "no_auth",
             "may_replicate", "sentinel", "only_sentinel", "no_mandatory_keys", "protected",
             "no_async_loading", "no_multi", "movablekeys", "allow_busy", "touches_arbitrary_keys",
             "script_runner"]
STD_CATS = ["keyspace", "read", "write", "set", "sortedset", "list", "hash", "string", "array", "bitmap",
            "hyperloglog", "geo", "stream", "pubsub", "admin", "fast", "slow", "blocking",
            "dangerous", "connection", "transaction", "scripting"]

KIND = {
    **{n: "MultiSum" for n in ["del", "exists", "touch", "unlink", "pfcount"]},
    "mget": "Mget", "mset": "Mset",
    **{n: "Blocking" for n in ["blpop", "brpop", "brpoplpush", "bzpopmax", "bzpopmin", "blmove", "blmpop", "bzmpop"]},
    "watch": "Watch", "unwatch": "Local", "ssubscribe": "Subscribe", "sunsubscribe": "Subscribe",
    "xread": "Xread", "xreadgroup": "Xread",
    **{n: "Eval" for n in ["eval", "eval_ro", "evalsha", "evalsha_ro", "fcall", "fcall_ro"]},
    **{n: "Subscribe" for n in ["subscribe", "psubscribe", "unsubscribe", "punsubscribe"]},
    **{n: "AnyMaster" for n in ["publish", "pubsub", "randomkey"]},
    "scan": "Scan", "dbsize": "Dbsize", "flushall": "Flushall", "exec": "Exec", "select": "Select",
    "script": "Script", "function": "Script",
    **{n: "Local" for n in ["acl", "auth", "client", "cluster", "command", "config", "discard", "echo",
                            "hello", "info", "multi", "ping", "quit", "reset", "time"]},
}
MFLAGS = {
    "get": ["C"], "mget": ["C"], "eval": ["W"], "evalsha": ["W"], "fcall": ["W"], "sort": ["S"], "georadius": ["S"], "georadiusbymember": ["S"], "pfcount": ["U"],
    **{n: ["P"] for n in ["ping", "subscribe", "psubscribe", "unsubscribe", "punsubscribe", "ssubscribe", "sunsubscribe"]},
    **{n: ["T"] for n in ["multi", "exec", "discard", "watch", "unwatch"]},
    "quit": ["T", "P"], "reset": ["T", "P"],
}
# SORT reports its STORE spec as unknown; its options begin after the key
SCAN_FROM = {"sort": 2}
# key positions of commands whose only key spec is keyword-based: (first, last, step, numkeys)
KEYS = {"json.debug": (2, 2, 1, 0)}

SKIP = {
    "asking", "bgrewriteaof", "bgsave", "clusterscan", "commandlog", "debug", "failover", "flushdb",
    "keys", "lastsave", "latency", "lolwut", "memory", "migrate", "module", "monitor",
    "move", "pfdebug", "pfselftest", "psync", "readonly", "readwrite", "replconf", "replicaof",
    "restore-asking", "role", "save", "shutdown", "slaveof", "slowlog",
    "swapdb", "sync", "wait", "waitaof",
    "ts.mget", "ts.mrange", "ts.mrevrange", "ts.queryindex",
}
SKIP_PREFIX = ("ft.", "_ft.", "search.", "timeseries.")
# commands plus subcommands must fit the ACL bitmap (src/command/mod.rs MAX_COMMANDS)
MAX_ENTRIES = 640


def keyspec(name, specs):
    if name in KEYS:
        return KEYS[name]
    ranges, knum = [], None
    for s in specs:
        if s["begin"] != "index":
            continue
        at, sp = s["at"], s["spec"]
        if s["find"] == "range":
            assert not sp.get("limit"), name
            lk = sp["lastkey"]
            ranges.append((at, at + lk if lk >= 0 else lk, sp["keystep"]))
        elif s["find"] == "keynum":
            assert sp["firstkey"] == 1, name
            knum = (at + sp["keynumidx"], sp["keystep"])
    first = last = step = numkeys = 0
    if ranges:
        ranges.sort()
        first, last, step = ranges[0]
        for a, l, s in ranges[1:]:
            assert s == step and last >= 0 and a == last + 1, (name, ranges)
            last = l
    if knum:
        numkeys, kstep = knum
        assert not ranges or step == kstep, name
        step = kstep
    return first, last, step, numkeys


def scan_from(name, specs):
    return SCAN_FROM.get(name) or max((s["from"] for s in specs if s["begin"] == "keyword"), default=0)


def entries():
    rows = {}
    for tag in ("redis", "valkey"):
        by = {r["name"]: r for r in INFO[tag]}
        for name, r in by.items():
            if "|" in name or name in rows:
                continue
            subs = [by[k] for k in by if k.startswith(name + "|")]
            keyed = [s for s in subs if s["keys"]]
            flags = list(r["flags"])
            if r["arity"] == -2 and not r["keys"] and subs:
                specs = {json.dumps(s["keys"], sort_keys=True) for s in keyed}
                if len(specs) != 1 or len(keyed) != len([s for s in subs if s["name"].split("|")[1] != "help"]):
                    if name not in KIND:
                        continue
                    keys = []
                else:
                    keys = keyed[0]["keys"]
                    if any("write" in s["flags"] for s in keyed):
                        flags.append("write")
                    elif all("readonly" in s["flags"] for s in keyed):
                        flags.append("readonly")
            else:
                keys = r["keys"]
            rows[name] = (r, flags, keys)
    return rows


def prefix64(name):
    used = min(len(name), 8)
    b = name.encode()[:8].ljust(8, b"\0")
    return int.from_bytes(b, "big") | ((0x2020_2020_2020_2020 << (8 * (8 - used))) & 0xFFFF_FFFF_FFFF_FFFF)


def main():
    cats_order = list(STD_CATS)
    table = []
    for name, (r, flags, keys) in entries().items():
        if name in SKIP or name.startswith(SKIP_PREFIX) or not all(c.islower() or c.isdigit() or c in "._" for c in name):
            continue
        first, last, step, numkeys = keyspec(name, keys)
        kind = KIND.get(name)
        if kind is None:
            if first == 0 and numkeys == 0:
                continue
            kind = "Blocking" if "blocking" in flags else "Single"
        m = []
        if "write" in flags:
            m.append("W")
        if "readonly" in flags:
            m.append("R")
        if "no_auth" in flags:
            m.append("N")
        m += MFLAGS.get(name, [])
        info = " | ".join(f"I_{f.upper()}" for f in STD_FLAGS if f in flags) or "0"
        cats = []
        for c in r["cats"]:
            c = c.lstrip("@")
            if c not in cats_order:
                cats_order.append(c)
            cats.append(c)
        cats = " | ".join(f"A_{c.upper()}" for c in cats_order if c in cats) or "0"
        key = (prefix64(name), len(name), name.encode()[8:])
        table.append((key, f'"{name}", {r["arity"]}, {" | ".join(m) or "0"}, {first}, {last}, {step}, {numkeys}, {scan_from(name, keys)}, Kind::{kind}, {info}, {cats}),'))
    table.sort()
    subs_of = {}
    for tag in ("redis", "valkey"):
        for r in INFO[tag]:
            if "|" in r["name"]:
                parent, sub = r["name"].split("|", 1)
                subs_of.setdefault(parent, {}).setdefault(sub, r)
    next_id = len(table)
    sub_blocks, rows, names, sub_names = [], [], [], []
    for i, (key, row) in enumerate(table):
        name = row.split('"')[1]
        names.append(name)
        subs = subs_of.get(name, {})
        if subs:
            items = []
            for sub in sorted(subs):
                sub_names.append(f"{name}|{sub}")
                cats = []
                for c in subs[sub]["cats"]:
                    c = c.lstrip("@")
                    if c not in cats_order:
                        cats_order.append(c)
                    cats.append(c)
                cats = " | ".join(f"A_{c.upper()}" for c in cats_order if c in cats) or "0"
                items.append(f's({next_id}, "{sub}", {cats})')
                next_id += 1
            static = "SUBS_" + name.upper().replace(".", "_")
            sub_blocks.append(f"static {static}: [Sub; {len(items)}] = [" + ", ".join(items) + "];")
            rows.append((key, f"    c({i}, {row[:-2]}, &{static}),"))
        else:
            rows.append((key, f"    c({i}, {row[:-2]}, &[]),"))
    table = rows
    assert next_id <= MAX_ENTRIES, next_id
    lines = ["//! Generated by tools/gen-command-table.py from tools/command-info.json; edit the overrides there.", "",
             "use super::{C, Kind, N, P, R, S, Spec, Sub, T, U, W, c, s};", ""]
    body = "\n".join(row for _, row in table) + "\n".join(sub_blocks)
    for i, f in enumerate(STD_FLAGS):
        if f"I_{f.upper()}" in body:
            lines.append(f"pub(super) const I_{f.upper()}: u32 = 1 << {i};")
    lines.append("")
    for i, c in enumerate(cats_order):
        if f"A_{c.upper()}" in body:
            lines.append(f"const A_{c.upper()}: u32 = 1 << {i};")
    lines += ["", "pub(super) static INFO_NAMES: &[&str] = &[" + ", ".join(f'"{f}"' for f in STD_FLAGS) + "];", "",
              "pub(super) static CAT_NAMES: &[&str] = &[" + ", ".join(f'"@{c}"' for c in cats_order) + "];", "",
              f"pub(super) const ENTRIES: usize = {next_id};", "",
              "/// Command and `container|sub` names by dense id.",
              "pub(super) static NAMES: &[&str] = &[" + ", ".join(f'"{n}"' for n in names + sub_names) + "];", "",
              "#[rustfmt::skip]"] + sub_blocks + ["",
              "#[rustfmt::skip]", "pub(super) static TABLE: &[Spec] = &["] + [row for _, row in table] + ["];", ""]
    with open(OUT, "w", encoding="utf-8") as target:
        target.write("\n".join(lines))
    print(f"{len(table)} commands, {next_id - len(table)} subcommands, {len(cats_order)} categories")


if __name__ == "__main__":
    main()

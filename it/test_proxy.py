"""Integration tests for the mithril Redis Cluster proxy.

Everything runs against the live containers on the `mithnet` docker network;
see conftest.py for connection fixtures and defaults.
"""

import threading
import time

import pytest
import redis
from redis.cluster import RedisCluster
from redis.crc import key_slot


def _cross_slot_pair(prefix):
    """Find two keys under `prefix` that hash to different cluster slots."""
    base = f"{prefix}:cs"
    first = f"{base}:0"
    first_slot = key_slot(first.encode())
    for i in range(1, 1000):
        candidate = f"{base}:{i}"
        if key_slot(candidate.encode()) != first_slot:
            return first, candidate
    raise RuntimeError("could not find a cross-slot key pair")


def _resp_encode(args):
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode() if isinstance(a, str) else a
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


class _RespReader:
    """Minimal RESP2 reply reader for raw-socket protocol tests."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise ConnectionError("socket closed by peer")
        self.buf += chunk

    def _readline(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self.buf) < n:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def read_reply(self):
        line = self._readline()
        prefix, rest = line[:1], line[1:]
        if prefix == b"+":
            return rest.decode()
        if prefix == b"-":
            raise redis.exceptions.ResponseError(rest.decode())
        if prefix == b":":
            return int(rest)
        if prefix == b"$":
            length = int(rest)
            if length == -1:
                return None
            data = self._read_exact(length + 2)[:length]
            return data.decode()
        if prefix == b"*":
            count = int(rest)
            if count == -1:
                return None
            return [self.read_reply() for _ in range(count)]
        raise ValueError(f"unknown reply prefix: {line!r}")


# --- single-key data types: round-trip + cross-check against direct cluster ---


def test_string_roundtrip(r, cluster_direct, key_prefix):
    key = f"{key_prefix}:str"
    assert r.set(key, "hello") is True
    assert r.get(key) == "hello"
    assert cluster_direct.get(key) == "hello"


def test_counter_roundtrip(r, cluster_direct, key_prefix):
    key = f"{key_prefix}:counter"
    assert r.incr(key) == 1
    assert r.incrby(key, 41) == 42
    assert r.get(key) == "42"
    assert cluster_direct.get(key) == "42"


def test_hash_roundtrip(r, cluster_direct, key_prefix):
    key = f"{key_prefix}:hash"
    r.hset(key, mapping={"f1": "v1", "f2": "v2"})
    assert r.hget(key, "f1") == "v1"
    assert r.hgetall(key) == {"f1": "v1", "f2": "v2"}
    assert cluster_direct.hgetall(key) == {"f1": "v1", "f2": "v2"}


def test_set_roundtrip(r, cluster_direct, key_prefix):
    key = f"{key_prefix}:set"
    r.sadd(key, "a", "b", "c")
    assert r.smembers(key) == {"a", "b", "c"}
    assert cluster_direct.smembers(key) == {"a", "b", "c"}


def test_zset_roundtrip(r, cluster_direct, key_prefix):
    key = f"{key_prefix}:zset"
    r.zadd(key, {"a": 1, "b": 2, "c": 3})
    expected = [("a", 1.0), ("b", 2.0), ("c", 3.0)]
    assert r.zrange(key, 0, -1, withscores=True) == expected
    assert cluster_direct.zrange(key, 0, -1, withscores=True) == expected


# --- multi-key / cross-slot fan-out ---


def test_mget_cross_slot_order(r, key_prefix):
    present_keys = [f"{key_prefix}:mget:{i}" for i in range(6)]
    missing_keys = [f"{key_prefix}:mget:missing:{i}" for i in range(2)]
    for i, k in enumerate(present_keys):
        r.set(k, f"val{i}")

    ordered = present_keys[:3] + [missing_keys[0]] + present_keys[3:] + [missing_keys[1]]
    result = r.mget(ordered)

    expected = [f"val{i}" for i in range(3)] + [None] + [f"val{i}" for i in range(3, 6)] + [None]
    assert result == expected


def test_mset_cross_slot(r, cluster_direct, key_prefix):
    mapping = {f"{key_prefix}:mset:{i}": f"v{i}" for i in range(8)}
    assert r.mset(mapping) is True
    for k, v in mapping.items():
        assert r.get(k) == v
        assert cluster_direct.get(k) == v


def test_del_unlink_exists_touch_cross_slot(r, key_prefix):
    keys = [f"{key_prefix}:mut:{i}" for i in range(8)]
    for k in keys:
        r.set(k, "x")

    assert r.exists(*keys) == 8
    assert r.touch(*keys) == 8

    to_delete, to_unlink = keys[:4], keys[4:]
    assert r.delete(*to_delete) == 4
    assert r.exists(*to_delete) == 0
    assert r.unlink(*to_unlink) == 4
    assert r.exists(*to_unlink) == 0


# --- transactions ---


def test_multi_exec_same_slot(r, key_prefix):
    k1 = f"{{{key_prefix}}}:a"
    k2 = f"{{{key_prefix}}}:b"
    pipe = r.pipeline(transaction=True)
    pipe.set(k1, "1")
    pipe.incr(k1)
    pipe.set(k2, "hello")
    pipe.get(k2)
    assert pipe.execute() == [True, 2, True, "hello"]


def test_multi_exec_cross_slot_rejected(r, key_prefix):
    # The proxy rejects the second QUEUED command with CROSSSLOT; redis-py's
    # transaction pipeline then aborts EXEC and re-raises that first queuing
    # error rather than wrapping it as ExecAbortError.
    k1, k2 = _cross_slot_pair(key_prefix)
    pipe = r.pipeline(transaction=True)
    pipe.set(k1, "1")
    pipe.set(k2, "2")
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
        pipe.execute()


def test_watch_rejected(r, key_prefix):
    pipe = r.pipeline(transaction=True)
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)unknown command"):
        pipe.watch(f"{key_prefix}:watched")


# --- blocking commands ---


def test_blpop_with_background_push(r, new_conn, key_prefix):
    key = f"{key_prefix}:blpop"
    pusher_conn = new_conn()

    def pusher():
        time.sleep(0.3)
        pusher_conn.lpush(key, "pushed-value")

    t = threading.Thread(target=pusher)
    t.start()
    try:
        result = r.blpop([key], timeout=5)
    finally:
        t.join()
    assert result == (key, "pushed-value")


def test_blpop_timeout_on_empty_key(r, key_prefix):
    key = f"{key_prefix}:blpop_empty"
    start = time.monotonic()
    result = r.blpop([key], timeout=0.5)
    elapsed = time.monotonic() - start
    assert result is None
    assert elapsed >= 0.4


# --- pub/sub ---


def test_pubsub_subscribe_publish(new_conn, key_prefix):
    channel = f"{key_prefix}:chan"
    sub = new_conn().pubsub()
    sub.subscribe(channel)
    assert sub.get_message(timeout=2)["type"] == "subscribe"

    new_conn().publish(channel, "hello-world")

    msg = sub.get_message(timeout=2)
    while msg is not None and msg["type"] != "message":
        msg = sub.get_message(timeout=2)
    assert msg is not None
    assert msg["data"] == "hello-world"
    sub.close()


def test_psubscribe_pattern(new_conn, key_prefix):
    pattern = f"{key_prefix}:pchan:*"
    channel = f"{key_prefix}:pchan:1"
    sub = new_conn().pubsub()
    sub.psubscribe(pattern)
    assert sub.get_message(timeout=2)["type"] == "psubscribe"

    new_conn().publish(channel, "pattern-hello")

    msg = sub.get_message(timeout=2)
    while msg is not None and msg["type"] != "pmessage":
        msg = sub.get_message(timeout=2)
    assert msg is not None
    assert msg["data"] == "pattern-hello"
    assert msg["pattern"] == pattern
    sub.close()


# --- RESP3 ---


def test_resp3_get_set(r3, key_prefix):
    key = f"{key_prefix}:resp3"
    assert r3.set(key, "v3") is True
    assert r3.get(key) == "v3"


def test_resp3_missing_key_is_none(r3, key_prefix):
    assert r3.get(f"{key_prefix}:doesnotexist") is None


def test_resp3_pubsub(new_conn, key_prefix):
    channel = f"{key_prefix}:resp3chan"
    sub = new_conn(protocol=3).pubsub()
    sub.subscribe(channel)
    assert sub.get_message(timeout=2)["type"] == "subscribe"

    new_conn(protocol=3).publish(channel, "resp3-hello")

    msg = sub.get_message(timeout=2)
    while msg is not None and msg["type"] != "message":
        msg = sub.get_message(timeout=2)
    assert msg is not None
    assert msg["data"] == "resp3-hello"
    sub.close()


# --- SCAN / DBSIZE ---


def test_scan_full_iteration(r, key_prefix):
    n = 50
    keys = {f"{key_prefix}:scan:{i}" for i in range(n)}
    for k in keys:
        r.set(k, "1")

    found = set(r.scan_iter(match=f"{key_prefix}:scan:*", count=25))
    assert found == keys


def test_dbsize_matches_direct_cluster_sum(r, cluster_direct):
    # two clients sample at different instants; retry until replication and
    # the proxy topology snapshot converge
    deadline = time.time() + 5
    while True:
        proxy_size = r.dbsize()
        total_direct = cluster_direct.dbsize(target_nodes=RedisCluster.PRIMARIES)
        if proxy_size == total_direct or time.time() > deadline:
            break
        time.sleep(0.2)
    per_node = {
        f"{n.host}:{n.port}": cluster_direct.get_redis_connection(n).dbsize()
        for n in cluster_direct.get_nodes()
    }
    assert proxy_size == total_direct, (proxy_size, total_direct, per_node)


# --- cluster emulation ---


def test_cluster_slots_emulation(r, proxy_addr):
    slots = r.execute_command("CLUSTER", "SLOTS")
    assert len(slots) == 1
    start, end, primary = slots[0][0], slots[0][1], slots[0][2]
    assert (start, end) == (0, 16383)
    assert (primary[0], primary[1]) == proxy_addr


def test_rediscluster_client_via_proxy(proxy_addr, key_prefix):
    client = RedisCluster(host=proxy_addr[0], port=proxy_addr[1], decode_responses=True)
    try:
        key = f"{key_prefix}:rc"
        assert client.set(key, "via-proxy-cluster-client") is True
        assert client.get(key) == "via-proxy-cluster-client"
    finally:
        client.close()


# --- scripting ---


def test_eval_with_key(r, key_prefix):
    key = f"{key_prefix}:evalkey"
    r.set(key, "eval-value")
    assert r.eval("return redis.call('get', KEYS[1])", 1, key) == "eval-value"


def test_eval_no_keys(r):
    assert r.eval("return 1+1", 0) == 2


# --- admin ---


def test_admin_ping(r):
    assert r.ping() is True


def test_admin_echo(r):
    assert r.echo("hello-echo") == "hello-echo"


def test_admin_time(r):
    seconds, micros = r.time()
    assert seconds > 1_700_000_000
    assert 0 <= micros < 1_000_000


def test_admin_info_contains_mithril_version(r):
    assert "mithril_version" in r.info()


def test_admin_config_get_maxclients(r):
    result = r.config_get("maxclients")
    assert "maxclients" in result
    assert int(result["maxclients"]) > 0


def test_admin_command_count(r):
    assert r.command_count() > 100


def test_admin_client_id_setname_getname(new_conn):
    c = new_conn()
    assert isinstance(c.client_id(), int)
    assert c.client_id() > 0
    assert c.client_setname("it-test-client") is True
    assert c.client_getname() == "it-test-client"


def test_admin_select(new_conn):
    c = new_conn()
    # redis-py's SELECT callback normalizes the +OK reply to a bool.
    assert c.execute_command("SELECT", 0) is True
    with pytest.raises(redis.exceptions.ResponseError):
        c.execute_command("SELECT", 1)


# --- errors ---


def test_unknown_command(r):
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)unknown command"):
        r.execute_command("TOTALLYFAKECMD123")


def test_error_wrong_arity(r):
    with pytest.raises(redis.exceptions.ResponseError):
        r.execute_command("GET")


def test_error_type_mismatch(r, key_prefix):
    key = f"{key_prefix}:typemismatch"
    r.hset(key, "field", "value")
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)wrongtype"):
        r.incr(key)


# --- raw protocol edge cases ---


def test_inline_command_raw_socket(raw_socket):
    s = raw_socket()
    s.sendall(b"PING\r\n")
    assert s.recv(64) == b"+PONG\r\n"


def test_pipelined_burst_raw_socket(raw_socket, key_prefix):
    s = raw_socket(timeout=10)
    reader = _RespReader(s)
    n = 500
    buf = bytearray()
    for i in range(n):
        key, val = f"{key_prefix}:burst:{i}", f"v{i}"
        buf += _resp_encode(["SET", key, val])
        buf += _resp_encode(["GET", key])
    s.sendall(bytes(buf))

    for i in range(n):
        assert reader.read_reply() == "OK"
        assert reader.read_reply() == f"v{i}"


@pytest.fixture
def cache_proxy(r):
    """The proxy under test, once its reply cache is enabled and armed."""
    info = r.info()
    if str(info.get("reply_cache", "no")) != "yes":
        pytest.skip("reply-cache disabled")
    workers = int(info["worker_threads"])
    deadline = time.time() + 10
    while time.time() < deadline:
        if int(r.info().get("cache_armed_workers", 0)) == workers:
            return r
        time.sleep(0.2)
    pytest.fail("reply cache never armed")


def test_cache_hits_and_read_your_writes(cache_proxy, key_prefix):
    r = cache_proxy
    key = f"{key_prefix}:c1"
    assert r.set(key, "v1")
    assert r.get(key) == "v1"
    before = int(r.info()["cache_hits"])
    assert r.get(key) == "v1"
    assert int(r.info()["cache_hits"]) > before
    assert r.set(key, "v2")
    assert r.get(key) == "v2"


def test_cache_converges_after_external_write(cache_proxy, cluster_direct, key_prefix):
    r = cache_proxy
    key = f"{key_prefix}:c2"
    assert r.set(key, "v1")
    assert r.get(key) == "v1"
    assert r.get(key) == "v1"
    cluster_direct.set(key, "v2")
    deadline = time.time() + 3
    while time.time() < deadline:
        if r.get(key) == "v2":
            break
        time.sleep(0.05)
    assert r.get(key) == "v2"


def test_cache_nil_entries_invalidate(cache_proxy, cluster_direct, key_prefix):
    r = cache_proxy
    key = f"{key_prefix}:c3"
    assert r.get(key) is None
    assert r.get(key) is None
    assert r.set(key, "v1")
    assert r.get(key) == "v1"
    cluster_direct.delete(key)
    deadline = time.time() + 3
    while time.time() < deadline:
        if r.get(key) is None:
            break
        time.sleep(0.05)
    assert r.get(key) is None


def test_pipelined_write_fanout_orders_before_following_get(r, key_prefix):
    key = f"{key_prefix}:ord"
    assert r.set(key, "v1")
    for _ in range(50):
        pipe = r.pipeline(transaction=False)
        pipe.set(key, "v1")
        pipe.delete(key)
        pipe.get(key)
        assert pipe.execute() == [True, 1, None]


def test_cache_store_option_targets_read_their_writes(cache_proxy, key_prefix):
    r = cache_proxy
    src, dst = f"{key_prefix}:{{s}}:src", f"{key_prefix}:{{s}}:dst"
    r.rpush(src, "b", "a")
    assert r.set(dst, "stale")
    assert r.get(dst) == "stale"
    assert r.get(dst) == "stale"
    r.execute_command("SORT", src, "ALPHA", "STORE", dst)
    assert r.type(dst) == "list"
    pipe = r.pipeline(transaction=False)
    pipe.set(dst, "v2")
    pipe.get(dst)
    assert pipe.execute() == [True, "v2"]


def test_cache_untouched_writes_send_no_invalidations(cache_proxy, cluster_direct, key_prefix):
    r = cache_proxy
    seen = f"{key_prefix}:seen"
    assert r.set(seen, "v1")
    assert r.get(seen) == "v1"
    assert r.get(seen) == "v1"
    before = int(r.info()["cache_invalidations"])
    for i in range(200):
        cluster_direct.set(f"{key_prefix}:never:{i}", "x")
    cluster_direct.set(seen, "v2")
    deadline = time.time() + 3
    while time.time() < deadline:
        if r.get(seen) == "v2":
            break
        time.sleep(0.05)
    assert r.get(seen) == "v2"
    info = r.info()
    delta = int(info["cache_invalidations"]) - before
    assert 1 <= delta <= int(info["worker_threads"]), delta


def test_subscribe_then_quit_pipelined_confirms_first(raw_socket, key_prefix):
    s = raw_socket()
    s.sendall(_resp_encode(["SUBSCRIBE", f"{key_prefix}:ch"]) + _resp_encode(["QUIT"]))
    reader = _RespReader(s)
    assert reader.read_reply() == ["subscribe", f"{key_prefix}:ch", 1]
    assert reader.read_reply() == "OK"


def test_object_encoding_routes_by_key(r, key_prefix):
    key = f"{key_prefix}:obj"
    assert r.set(key, "12345")
    assert r.object("encoding", key) in ("int", "embstr")
    assert r.execute_command("OBJECT", "REFCOUNT", key) >= 1


def test_client_list_shows_this_connection(new_conn):
    c = new_conn()
    c.client_setname("it-list")
    cid = c.client_id()
    rows = [row for row in c.client_list() if int(row["id"]) == cid]
    assert len(rows) == 1
    assert rows[0]["name"] == "it-list"
    assert rows[0]["addr"]


def test_pubsub_context_and_multi_errors_match_redis_wording(raw_socket, new_conn):
    s = raw_socket()
    s.sendall(_resp_encode(["SUBSCRIBE", "it:ctx"]) + _resp_encode(["GET", "x"]))
    reader = _RespReader(s)
    assert reader.read_reply()[0] == "subscribe"
    with pytest.raises(redis.exceptions.ResponseError, match=r"Can't execute 'get'"):
        reader.read_reply()
    c = new_conn()
    with pytest.raises(redis.exceptions.ResponseError, match=r"in MULTI / EXEC, only support"):
        pipe = c.pipeline(transaction=True)
        pipe.execute_command("CLIENT", "LIST")
        pipe.execute()

# --- slot migration ---


def test_multikey_survives_a_migrating_slot(r, cluster_direct, key_prefix):
    a, b, other = f"{{{key_prefix}}}a", f"{{{key_prefix}}}b", f"{key_prefix}:other"
    slot = key_slot(a.encode())
    assert key_slot(other.encode()) != slot
    src_node = cluster_direct.get_node_from_key(a)
    dst_node = next(
        n for n in cluster_direct.get_primaries() if (n.host, n.port) != (src_node.host, src_node.port)
    )
    src = redis.Redis(host=src_node.host, port=src_node.port, decode_responses=True)
    dst = redis.Redis(host=dst_node.host, port=dst_node.port, decode_responses=True)
    src_id = src.execute_command("CLUSTER MYID")
    dst_id = dst.execute_command("CLUSTER MYID")
    assert r.mset({a: "1", b: "2", other: "3"})
    src.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", dst_id)
    dst.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", src_id)
    try:
        assert src.execute_command("MIGRATE", dst_node.host, dst_node.port, "", 0, 5000, "KEYS", a) == "OK"
        with pytest.raises(redis.exceptions.ResponseError, match="TRYAGAIN|ASK"):
            src.mget(a, b)
        assert r.mget(a, b) == ["1", "2"]
        assert r.mget(a, b, other) == ["1", "2", "3"]
        assert r.exists(a, b) == 2
        assert r.exists(a, b, other) == 3
        assert r.mset({a: "11", b: "22"})
        assert r.mset({a: "111", b: "222", other: "333"})
        assert r.mget(a, b, other) == ["111", "222", "333"]
        pipe = r.pipeline(transaction=False)
        pipe.mset({a: "x", b: "y"})
        pipe.set(b, "z")
        _, second = pipe.execute(raise_on_error=False)
        assert second is True
        assert r.get(b) == "z"
        assert r.mset({a: "p", b: "q"})
        assert r.mget(a, b) == ["p", "q"]
        assert r.delete(a, b, other) == 3
        assert r.mget(a, b) == [None, None]
    finally:
        dst.execute_command("ASKING")
        dst.execute_command("MIGRATE", src_node.host, src_node.port, "", 0, 5000, "KEYS", a)
        src.execute_command("CLUSTER SETSLOT", slot, "STABLE")
        dst.execute_command("CLUSTER SETSLOT", slot, "STABLE")
        r.delete(a, b, other)
        src.close()
        dst.close()


def test_cache_mget_hits_and_read_your_writes(cache_proxy, key_prefix):
    r = cache_proxy
    a, b, c = f"{{{key_prefix}}}a", f"{{{key_prefix}}}b", f"{key_prefix}:c"
    assert key_slot(c.encode()) != key_slot(a.encode())
    assert r.mset({a: "1", b: "2", c: "3"})
    assert r.mget(a, b) == ["1", "2"]
    before = int(r.info()["cache_hits"])
    assert r.mget(a, b) == ["1", "2"]
    assert int(r.info()["cache_hits"]) > before
    assert r.mget(a, b, c) == ["1", "2", "3"]
    before = int(r.info()["cache_hits"])
    assert r.mget(a, b, c) == ["1", "2", "3"]
    assert int(r.info()["cache_hits"]) > before
    assert r.set(a, "11")
    assert r.mget(a, b, c) == ["11", "2", "3"]
    assert r.mset({b: "22", c: "33"})
    assert r.mget(a, b, c) == ["11", "22", "33"]
    assert r.delete(c) == 1
    assert r.mget(a, b, c) == ["11", "22", None]
    assert r.mget(c, a) == [None, "11"]


def test_cache_mget_converges_after_external_write(cache_proxy, cluster_direct, key_prefix):
    r = cache_proxy
    a, b = f"{{{key_prefix}}}a", f"{{{key_prefix}}}b"
    assert r.mset({a: "1", b: "2"})
    assert r.mget(a, b) == ["1", "2"]
    assert r.mget(a, b) == ["1", "2"]
    cluster_direct.set(a, "x")
    deadline = time.time() + 2
    while r.mget(a, b) != ["x", "2"] and time.time() < deadline:
        time.sleep(0.05)
    assert r.mget(a, b) == ["x", "2"]

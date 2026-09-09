"""Integration tests for the mithril Redis Cluster proxy.

Everything runs against the live containers on the `mithnet` docker network;
see conftest.py for connection fixtures and defaults.
"""

import hashlib
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


def _cluster_databases(cluster_direct):
    node = cluster_direct.get_primaries()[0]
    with redis.Redis(host=node.host, port=node.port, decode_responses=True) as direct:
        return int(direct.config_get("cluster-databases").get("cluster-databases", 1))


def test_admin_select(new_conn, cluster_direct):
    c = new_conn()
    # redis-py's SELECT callback normalizes the +OK reply to a bool.
    assert c.execute_command("SELECT", 0) is True
    databases = _cluster_databases(cluster_direct)
    if databases > 1:
        assert c.execute_command("SELECT", databases - 1) is True
        with pytest.raises(redis.exceptions.ResponseError, match="out of range"):
            c.execute_command("SELECT", databases)
    else:
        with pytest.raises(redis.exceptions.ResponseError):
            c.execute_command("SELECT", 1)
    with pytest.raises(redis.exceptions.ResponseError, match="invalid DB index"):
        c.execute_command("SELECT", "x")


def test_select_ends_watch_pipelined(new_conn, cluster_direct, raw_socket, key_prefix):
    if _cluster_databases(cluster_direct) < 2:
        pytest.skip("cluster-databases is 1")
    k = f"{key_prefix}:wselp"
    setup = new_conn()
    assert setup.execute_command("SELECT", 1) is True
    assert setup.set(k, "one")
    assert setup.execute_command("SELECT", 0) is True
    assert setup.set(k, "zero")
    s = raw_socket()
    reader = _RespReader(s)
    batch = (
        _resp_encode(["SELECT", "1"])
        + _resp_encode(["WATCH", k])
        + _resp_encode(["SELECT", "0"])
        + _resp_encode(["GET", k])
    )
    s.sendall(batch)
    assert reader.read_reply() == "OK"
    assert reader.read_reply() == "OK"
    assert reader.read_reply() == "OK"
    assert reader.read_reply() == "zero"
    assert setup.execute_command("SELECT", 1) is True
    assert setup.delete(k) == 1
    assert setup.execute_command("SELECT", 0) is True
    assert setup.delete(k) == 1


def test_exec_after_select_runs_in_the_new_database(cluster_direct, raw_socket, key_prefix):
    if _cluster_databases(cluster_direct) < 2:
        pytest.skip("cluster-databases is 1")
    k = f"{key_prefix}:xsel"
    s = raw_socket()
    reader = _RespReader(s)
    batch = (
        _resp_encode(["SELECT", "1"])
        + _resp_encode(["WATCH", k])
        + _resp_encode(["SELECT", "0"])
        + _resp_encode(["MULTI"])
        + _resp_encode(["SET", k, "v"])
        + _resp_encode(["EXEC"])
    )
    s.sendall(batch)
    assert reader.read_reply() == "OK"
    assert reader.read_reply() == "OK"
    assert reader.read_reply() == "OK"
    assert reader.read_reply() == "OK"
    assert reader.read_reply() == "QUEUED"
    assert reader.read_reply() == ["OK"]
    verify = raw_socket()
    vr = _RespReader(verify)
    verify.sendall(_resp_encode(["GET", k]))
    assert vr.read_reply() == "v"
    verify.sendall(_resp_encode(["SELECT", "1"]) + _resp_encode(["GET", k]))
    assert vr.read_reply() == "OK"
    assert vr.read_reply() is None
    verify.sendall(_resp_encode(["DEL", k]))
    assert vr.read_reply() == 0
    verify.sendall(_resp_encode(["SELECT", "0"]) + _resp_encode(["DEL", k]))
    assert vr.read_reply() == "OK"
    assert vr.read_reply() == 1


def test_select_ends_watch(new_conn, cluster_direct, key_prefix):
    if _cluster_databases(cluster_direct) < 2:
        pytest.skip("cluster-databases is 1")
    k = f"{key_prefix}:wsel"
    c = new_conn()
    assert c.execute_command("SELECT", 1) is True
    assert c.set(k, "one")
    assert c.execute_command("SELECT", 0) is True
    assert c.set(k, "zero")
    assert c.execute_command("SELECT", 1) is True
    assert c.execute_command("WATCH", k) is True
    assert c.execute_command("SELECT", 0) is True
    assert c.get(k) == "zero"
    other = new_conn()
    assert other.set(k, "zero2")
    with c.pipeline(transaction=True) as pipe:
        pipe.multi()
        pipe.get(k)
        assert pipe.execute() == ["zero2"]
    assert c.execute_command("SELECT", 1) is True
    assert c.get(k) == "one"
    assert c.delete(k) == 1
    assert c.execute_command("SELECT", 0) is True
    assert c.delete(k) == 1


def test_select_isolates_databases(r, new_conn, cluster_direct, key_prefix):
    if _cluster_databases(cluster_direct) < 2:
        pytest.skip("cluster-databases is 1")
    k, other = _cross_slot_pair(key_prefix)
    n, lk = f"{key_prefix}:n", f"{key_prefix}:l"
    c = new_conn()
    assert c.execute_command("SELECT", 1) is True
    assert c.mset({k: "one", other: "two"})
    assert c.mget(k, other) == ["one", "two"]
    assert r.mget(k, other) == [None, None]
    assert r.set(k, "zero")
    assert c.get(k) == "one"
    assert c.incr(n) == 1
    assert c.incr(n) == 2
    assert r.get(n) is None
    assert c.eval("return redis.call('GET', KEYS[1])", 1, k) == "one"
    with c.pipeline() as pipe:
        pipe.multi()
        pipe.set(k, "uno")
        pipe.get(k)
        assert pipe.execute() == [True, "uno"]
    pusher = new_conn()
    assert pusher.execute_command("SELECT", 1) is True
    t = threading.Timer(0.2, lambda: pusher.lpush(lk, "x"))
    t.start()
    try:
        assert c.blpop([lk], timeout=5) == (lk, "x")
    finally:
        t.join()
    assert r.llen(lk) == 0
    with c.pipeline() as pipe:
        pipe.watch(k)
        pipe.multi()
        pipe.set(k, "dos")
        assert pipe.execute() == [True]
    assert c.get(k) == "dos"
    assert r.get(k) == "zero"
    assert c.execute_command("SELECT", 0) is True
    assert c.get(k) == "zero"
    assert c.execute_command("SELECT", 1) is True
    assert c.execute_command("RESET") == "RESET"
    assert c.get(k) == "zero"
    assert c.execute_command("SELECT", 1) is True
    assert c.delete(k, other, n) == 3
    assert r.delete(k) == 1


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
    src_node, dst_node, src, dst = _shard_pair(cluster_direct, a)
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


def _shard_pair(cluster_direct, key):
    """The master owning `key` and another master, as nodes and as direct clients."""
    src_node = cluster_direct.get_node_from_key(key)
    dst_node = next(
        n for n in cluster_direct.get_primaries() if (n.host, n.port) != (src_node.host, src_node.port)
    )
    src = redis.Redis(host=src_node.host, port=src_node.port, decode_responses=True)
    dst = redis.Redis(host=dst_node.host, port=dst_node.port, decode_responses=True)
    return src_node, dst_node, src, dst


def _until(cond, seconds=30):
    deadline = time.time() + seconds
    while not cond():
        if time.time() > deadline:
            return False
        time.sleep(0.05)
    return True


def _migrate_slot(source, target, slot, probe):
    """Atomic slot migration of `slot` to `target`, waited until `source` redirects."""
    if "valkey_version" in target.info("server"):
        target_id = target.execute_command("CLUSTER MYID")
        assert source.execute_command("CLUSTER MIGRATESLOTS", "SLOTSRANGE", slot, slot, "NODE", target_id) == "OK"
    else:
        target.execute_command("CLUSTER MIGRATION", "IMPORT", slot, slot)
    for _ in range(600):
        if _moved(source, probe):
            return
        time.sleep(0.05)
    pytest.fail("slot migration did not complete")


def _moved(node, key):
    try:
        node.get(key)
    except redis.exceptions.ResponseError as e:
        return str(e).startswith("MOVED")
    return False


def test_atomic_slot_migration_under_traffic(r, cluster_direct, new_conn, key_prefix):
    _needs(cluster_direct, (8, 4))
    counter = f"{{{key_prefix}}}:n"
    keys = [f"{{{key_prefix}}}:{i}" for i in range(20000)]
    slot = key_slot(counter.encode())
    _, _, src, dst = _shard_pair(cluster_direct, counter)
    bulk = new_conn()
    pipe = bulk.pipeline(transaction=False)
    for k in keys:
        pipe.set(k, "x" * 1000)
    pipe.execute()
    stop, errors, last = threading.Event(), [], [0]

    def churn():
        c = new_conn()
        while not stop.is_set() and len(errors) < 20:
            try:
                n = c.incr(counter)
                if n != last[0] + 1 or c.get(keys[7]) != "x" * 1000:
                    errors.append(f"incr {last[0]} -> {n}")
                last[0] = n
            except redis.exceptions.RedisError as e:
                errors.append(repr(e))

    worker = threading.Thread(target=churn)
    worker.start()
    try:
        for source, target in [(src, dst), (dst, src)] * 2:
            _migrate_slot(source, target, slot, counter)
            assert target.execute_command("CLUSTER COUNTKEYSINSLOT", slot) >= len(keys) + 1
        stop.set()
        worker.join(30)
        final = r.get(counter)
    finally:
        stop.set()
        worker.join(30)
        if _moved(src, counter):
            _migrate_slot(dst, src, slot, counter)
        bulk.delete(counter, *keys)
        src.close()
        dst.close()
    assert errors == []
    assert last[0] > 0 and int(final) == last[0]


def test_mset_stays_atomic_under_slot_migration(r, cluster_direct, new_conn, key_prefix):
    _needs(cluster_direct, (8, 4))
    a, b, probe = f"{{{key_prefix}}}:ma", f"{{{key_prefix}}}:mb", f"{{{key_prefix}}}:mp"
    slot = key_slot(probe.encode())
    _, _, src, dst = _shard_pair(cluster_direct, probe)
    assert r.set(probe, "p")
    assert r.mset({a: "0", b: "0"})
    stop, errors, torn, rounds = threading.Event(), [], [], [0]

    def write():
        c = new_conn()
        n = 0
        while not stop.is_set() and len(errors) < 20:
            n += 1
            try:
                c.mset({a: str(n), b: str(n)})
                rounds[0] = n
            except redis.exceptions.RedisError as e:
                errors.append(repr(e))

    def watch():
        c = new_conn()
        while not stop.is_set() and len(errors) < 20:
            try:
                va, vb = c.mget(a, b)
                if va != vb:
                    torn.append((va, vb))
            except redis.exceptions.RedisError as e:
                errors.append(repr(e))

    threads = [threading.Thread(target=write), threading.Thread(target=watch)]
    for t in threads:
        t.start()
    try:
        for source, target in [(src, dst), (dst, src)] * 2:
            _migrate_slot(source, target, slot, probe)
    finally:
        stop.set()
        for t in threads:
            t.join(30)
        if _moved(src, probe):
            _migrate_slot(dst, src, slot, probe)
        r.delete(a, b, probe)
        src.close()
        dst.close()
    assert errors == []
    assert torn == []
    assert rounds[0] > 0


def test_keyless_write_rides_out_a_failover(r, cluster_direct, new_conn, key_prefix):
    _needs(cluster_direct, (7, 0))
    k = f"{key_prefix}:fo"
    nodes = _slot_nodes(cluster_direct, key_slot(k.encode()))
    if len(nodes) < 2:
        pytest.skip("the slot's master has no replica")
    master = redis.Redis(host=nodes[0].host, port=nodes[0].port, decode_responses=True)
    replica = redis.Redis(host=nodes[1].host, port=nodes[1].port, decode_responses=True)
    name = key_prefix.replace(":", "_")
    lib = f"#!lua name={name}\nredis.register_function('f_{name}', function() return 1 end)"
    stop, errors, loads = threading.Event(), [], [0]

    def churn():
        c = new_conn()
        while not stop.is_set() and len(errors) < 20:
            try:
                c.execute_command("FUNCTION", "LOAD", "REPLACE", lib)
                loads[0] += 1
            except redis.exceptions.RedisError as e:
                errors.append(repr(e))

    def role_is(node, role):
        return _until(lambda: node.info("replication")["role"] == role)

    def synced(node):
        def caught_up():
            info = node.info("replication")
            return info.get("master_link_status") == "up" and not info.get("master_sync_in_progress")

        return _until(caught_up)

    assert synced(replica), "the replica never caught up with its master"
    worker = threading.Thread(target=churn)
    worker.start()
    try:
        assert replica.execute_command("CLUSTER", "FAILOVER", "TAKEOVER")
        assert role_is(replica, "master"), "failover did not complete"
        assert role_is(master, "slave"), "the old master was not demoted"
        time.sleep(0.5)
        stop.set()
        worker.join(30)
    finally:
        stop.set()
        worker.join(30)
        restored = master.info("replication")["role"] == "master"
        if not restored and synced(master):
            master.execute_command("CLUSTER", "FAILOVER", "TAKEOVER")
            restored = role_is(master, "master") and role_is(replica, "slave")
        master.close()
        replica.close()
    assert errors == []
    assert loads[0] > 0
    assert restored, "the original master was not restored"


def test_evalsha_reloads_after_a_redirect(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (8, 4))
    key = f"{key_prefix}:reload"
    slot = key_slot(key.encode())
    _, _, src, dst = _shard_pair(cluster_direct, key)
    sha = r.script_load("return redis.call('set', KEYS[1], ARGV[1])")
    assert r.evalsha(sha, 1, key, "v1") == "OK"
    try:
        _migrate_slot(src, dst, slot, key)
        dst.script_flush()
        assert r.evalsha(sha, 1, key, "v2") == "OK"
        assert r.get(key) == "v2"
    finally:
        if _moved(src, key):
            _migrate_slot(dst, src, slot, key)
        r.delete(key)
        src.close()
        dst.close()


def _slot_nodes(cluster_direct, slot):
    deadline = time.time() + 10
    while True:
        nodes = cluster_direct.nodes_manager.slots_cache[slot]
        if len(nodes) > 1 or time.time() > deadline:
            return nodes
        time.sleep(0.5)
        cluster_direct.nodes_manager.initialize()


def test_cache_mget_hits_and_read_your_writes(cache_proxy, key_prefix):
    r = cache_proxy
    a, b, c = f"{{{key_prefix}}}a", f"{{{key_prefix}}}b", f"{key_prefix}:c"
    assert key_slot(c.encode()) != key_slot(a.encode())
    assert r.mset({a: "1", b: "2", c: "3"})
    assert _cached_mget(r, [a, b]) == ["1", "2"]
    assert _cached_mget(r, [a, b, c]) == ["1", "2", "3"]
    assert r.set(a, "11")
    assert r.mget(a, b, c) == ["11", "2", "3"]
    assert r.mset({b: "22", c: "33"})
    assert r.mget(a, b, c) == ["11", "22", "33"]
    assert r.delete(c) == 1
    assert r.mget(a, b, c) == ["11", "22", None]
    assert r.mget(c, a) == [None, "11"]


def _cached_mget(r, keys):
    deadline = time.time() + 5
    while True:
        before = int(r.info()["cache_hits"])
        reply = r.mget(*keys)
        if int(r.info()["cache_hits"]) > before:
            return reply
        if time.time() > deadline:
            pytest.fail("MGET never hit the reply cache")
        time.sleep(0.05)


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


def _server(cluster_direct):
    info = cluster_direct.info("server")
    info = next(iter(info.values())) if isinstance(info, dict) and "redis_version" not in info else info
    if "valkey_version" in info:
        return "valkey", tuple(int(x) for x in info["valkey_version"].split(".")[:2])
    return "redis", tuple(int(x) for x in info["redis_version"].split(".")[:2])


def _needs(cluster_direct, redis_min, valkey_min=(9, 0)):
    name, ver = _server(cluster_direct)
    floor = valkey_min if name == "valkey" else redis_min
    if ver < floor:
        pytest.skip(f"{name} {ver} lacks these commands")


def test_list_and_zset_numkeys_commands(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (7, 0))
    l1, l2, z1, z2, dst = (f"{{{key_prefix}}}:{n}" for n in ("l1", "l2", "z1", "z2", "dst"))
    r.rpush(l1, "a", "b", "c")
    assert r.lmove(l1, l2, "LEFT", "RIGHT") == "a"
    assert r.lmpop(2, l1, l2, direction="RIGHT", count=2) == [l1, ["c", "b"]]
    assert r.lmpop(2, l1, l2, direction="LEFT") == [l2, ["a"]]
    r.zadd(z1, {"x": 1, "y": 2})
    r.zadd(z2, {"y": 5, "z": 9})
    assert r.zmpop(2, [z1, z2], min=True) == [z1, [["x", "1"]]]
    assert set(r.zrandmember(z2, 2)) == {"y", "z"}
    assert r.zrangestore(dst, z2, 0, -1) == 2
    assert r.zunion([z1, z2], aggregate="MAX", withscores=True) == [("y", 5.0), ("z", 9.0)]
    assert r.zinter([z1, z2]) == ["y"]
    assert r.zdiff([z2, z1]) == ["z"]
    assert r.zintercard(2, [z1, z2]) == 1
    assert r.zmscore(z2, ["y", "missing"]) == [5.0, None]
    assert r.zunionstore(dst, {z1: 1, z2: 2}) == 2
    assert cluster_direct.zcard(dst) == 2


def test_hash_and_set_extensions(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (7, 0))
    h, s1, s2 = (f"{{{key_prefix}}}:{n}" for n in ("h", "s1", "s2"))
    r.hset(h, mapping={"f1": "1", "f2": "2", "f3": "3"})
    assert set(r.hrandfield(h, 2)) <= {"f1", "f2", "f3"}
    r.sadd(s1, "a", "b", "c")
    r.sadd(s2, "b", "c", "d")
    assert r.smismember(s1, ["a", "d"]) == [1, 0]
    assert r.sintercard(2, [s1, s2]) == 2
    assert r.sintercard(2, [s1, s2], limit=1) == 1


def test_hash_field_expiration(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (7, 4), (9, 0))
    h = f"{key_prefix}:hx"
    r.hset(h, mapping={"f1": "1", "f2": "2"})
    assert r.execute_command("HEXPIRE", h, 100, "FIELDS", 1, "f1") == [1]
    ttl = r.execute_command("HTTL", h, "FIELDS", 2, "f1", "f2")
    assert 0 < ttl[0] <= 100 and ttl[1] == -1
    assert r.execute_command("HPERSIST", h, "FIELDS", 1, "f1") == [1]
    assert cluster_direct.execute_command("HTTL", h, "FIELDS", 1, "f1") == [-1]


def test_blocking_new_forms(r, new_conn, cluster_direct, key_prefix):
    _needs(cluster_direct, (7, 0))
    src, dst, z = (f"{{{key_prefix}}}:{n}" for n in ("bsrc", "bdst", "bz"))
    assert r.blmpop(0.3, 1, src, direction="LEFT") is None
    pusher = new_conn()

    def push():
        time.sleep(0.3)
        pusher.rpush(src, "v1")
        pusher.zadd(z, {"m": 1})

    t = threading.Thread(target=push)
    t.start()
    try:
        assert r.blmove(src, dst, 5, "LEFT", "RIGHT") == "v1"
        assert r.bzmpop(5, 1, [z], min=True) == [z, [["m", "1"]]]
    finally:
        t.join()
    assert cluster_direct.lrange(dst, 0, -1) == ["v1"]


def test_geo_stream_and_string_extensions(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (7, 0))
    g, gdst, st, a, b, hll1, hll2, hdst = (
        f"{{{key_prefix}}}:{n}" for n in ("g", "gdst", "st", "a", "b", "hll1", "hll2", "hdst")
    )
    r.geoadd(g, (13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania"))
    assert set(r.geosearch(g, longitude=15, latitude=37, radius=200, unit="km")) == {"Palermo", "Catania"}
    assert r.geosearchstore(gdst, g, longitude=15, latitude=37, radius=100, unit="km") == 1
    entry = r.xadd(st, {"k": "v"})
    assert r.xgroup_create(st, "g1", id="0")
    got = r.xreadgroup("g1", "c1", {st: ">"}, count=1)
    assert got[0][1][0][0] == entry
    assert r.xack(st, "g1", entry) == 1
    assert r.xinfo_groups(st)[0]["name"] == "g1"
    assert r.xautoclaim(st, "g1", "c2", 0)[0] == "0-0"
    assert r.xtrim(st, maxlen=0) == 1
    assert r.xlen(st) == 0
    r.set(a, "ohmytext")
    r.set(b, "mynewtext")
    assert r.lcs(a, b) == "mytext"
    assert r.set(a, "v", ex=100)
    assert r.expiretime(a) > time.time()
    r.pfadd(hll1, "x", "y")
    r.pfadd(hll2, "y", "z")
    assert r.pfmerge(hdst, hll1, hll2)
    assert r.pfcount(hdst) == 3
    assert r.dump(a) is not None
    assert r.getex(a, persist=True) == "v"
    assert cluster_direct.ttl(a) == -1


def test_command_info_shape_and_getkeys(r, raw_socket):
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["COMMAND", "INFO", "get", "set", "nosuchcmd"]))
    get, set_, nope = reader.read_reply()
    assert get == ["get", 2, ["readonly", "fast"], 1, 1, 1, ["@read", "@string", "@fast"]]
    assert set_ == ["set", -3, ["write", "denyoom"], 1, 1, 1, ["@write", "@string", "@slow"]]
    assert nope is None
    for cmd, keys in [
        (["mset", "k1", "v1", "k2", "v2"], ["k1", "k2"]),
        (["lmpop", "2", "l1", "l2", "LEFT"], ["l1", "l2"]),
        (["zunionstore", "d", "2", "z1", "z2"], ["d", "z1", "z2"]),
        (["eval", "return 1", "0"], []),
    ]:
        s.sendall(_resp_encode(["COMMAND", "GETKEYS", *cmd]))
        assert reader.read_reply() == keys
    s.sendall(_resp_encode(["COMMAND", "GETKEYS", "nosuchcmd", "k"]))
    with pytest.raises(redis.exceptions.ResponseError):
        reader.read_reply()
    assert r.command_count() == len(r.execute_command("COMMAND"))


def test_evalsha_routes_and_reports_noscript(r, cluster_direct, key_prefix):
    key = f"{key_prefix}:sha"
    sha = cluster_direct.script_load("return redis.call('set', KEYS[1], ARGV[1])")
    assert r.evalsha(sha, 1, key, "v") == "OK"
    assert r.get(key) == "v"
    assert r.eval("return redis.call('get', KEYS[1])", 1, key) == "v"
    with pytest.raises(redis.exceptions.NoScriptError):
        r.evalsha("0" * 40, 1, key)


def test_multi_checks_numkeys_and_store_slots(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (7, 0))
    k = f"{{{key_prefix}}}:lm"
    pipe = r.pipeline(transaction=True)
    pipe.rpush(k, "a", "b")
    pipe.execute_command("LMPOP", 1, k, "LEFT")
    pushed, popped = pipe.execute()
    assert pushed == 2 and popped[0] == k
    near, far = _cross_slot_pair(key_prefix)
    pipe = r.pipeline(transaction=True)
    pipe.set(near, "1")
    pipe.execute_command("LMPOP", 1, far, "LEFT")
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
        pipe.execute()
    pipe = r.pipeline(transaction=True)
    pipe.rpush(near, "b", "a")
    pipe.sort(near, store=far)
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
        pipe.execute()
    pipe = r.pipeline(transaction=True)
    pipe.rpush(near, "b", "a")
    pipe.execute_command("SORT", near, "BY", "STORE", "ALPHA")
    assert len(pipe.execute()[1]) == 2


def test_xreadgroup_consumer_named_streams(r, key_prefix):
    stream = f"{key_prefix}:xs"
    assert r.execute_command("XGROUP", "CREATE", stream, "g", "$", "MKSTREAM") == "OK"
    read = r.execute_command("XREADGROUP", "GROUP", "g", "STREAMS", "COUNT", "1", "STREAMS", stream, ">")
    assert read in (None, [])


def test_command_getkeys_keyword_specs(raw_socket):
    s = raw_socket()
    reader = _RespReader(s)
    for cmd, keys in [
        (["xread", "COUNT", "1", "STREAMS", "s1", "s2", "0", "0"], ["s1", "s2"]),
        (["sort", "src", "ALPHA", "STORE", "dst"], ["src", "dst"]),
        (["georadius", "g", "0", "0", "1", "km", "STOREDIST", "d"], ["g", "d"]),
        (["lmpop", "1", "l", "LEFT"], ["l"]),
        (["xreadgroup", "GROUP", "g", "STREAMS", "STREAMS", "s1", ">"], ["s1"]),
        (["sort", "{STORE}", "BY", "STORE", "ALPHA"], ["{STORE}"]),
        (["georadiusbymember", "g", "STORE", "1", "km"], ["g"]),
    ]:
        s.sendall(_resp_encode(["COMMAND", "GETKEYS", *cmd]))
        assert reader.read_reply() == keys
    s.sendall(_resp_encode(["COMMAND", "INFO", "lmpop", "xread", "eval"]))
    for entry in reader.read_reply():
        assert entry[3:6] == [0, 0, 0], entry


def test_container_help_is_answered_by_the_engine(raw_socket):
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["OBJECT", "HELP"]))
    assert any("OBJECT" in line for line in reader.read_reply())
    s.sendall(_resp_encode(["XINFO", "HELP"]))
    assert any("XINFO" in line for line in reader.read_reply())
    s.sendall(_resp_encode(["OBJECT", "ENCODING"]))
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)wrong number of arguments"):
        reader.read_reply()


def test_script_write_invalidates_cached_string(cache_proxy, key_prefix):
    r = cache_proxy
    k = f"{key_prefix}:ev"
    assert r.set(k, "old")
    assert r.get(k) == "old"
    assert r.get(k) == "old"
    pipe = r.pipeline(transaction=False)
    pipe.eval("return redis.call('set', KEYS[1], ARGV[1])", 1, k, "new")
    pipe.get(k)
    assert pipe.execute() == ["OK", "new"]


def test_numkeys_write_invalidates_cached_string(cache_proxy, cluster_direct, key_prefix):
    r = cache_proxy
    _needs(cluster_direct, (6, 2))
    k, z = (f"{{{key_prefix}}}:{n}" for n in ("k", "z"))
    assert r.set(k, "plain")
    assert r.get(k) == "plain"
    assert r.get(k) == "plain"
    r.zadd(z, {"m": 1})
    assert r.zunionstore(k, [z]) == 1
    with pytest.raises(redis.ResponseError):
        r.get(k)


def test_script_management_through_proxy(r, cluster_direct, key_prefix):
    key = f"{key_prefix}:script"
    sha = r.script_load("return redis.call('set', KEYS[1], ARGV[1])")
    assert r.script_exists(sha) == [True]
    assert r.evalsha(sha, 1, key, "v1") == "OK"
    cluster_direct.script_flush()
    assert r.evalsha(sha, 1, key, "v2") == "OK"
    assert r.get(key) == "v2"
    cluster_direct.script_flush()
    pipe = r.pipeline(transaction=False)
    pipe.evalsha(sha, 1, key, "v3")
    pipe.set(key, "v4")
    first, second = pipe.execute(raise_on_error=False)
    assert isinstance(first, redis.exceptions.NoScriptError) and second is True
    assert r.get(key) == "v4"
    assert r.evalsha(sha, 1, key, "v5") == "OK"
    pipe = r.pipeline(transaction=False)
    pipe.script_flush()
    pipe.evalsha(sha, 1, key, "v6")
    flushed, rerun = pipe.execute(raise_on_error=False)
    assert flushed is True and isinstance(rerun, redis.exceptions.NoScriptError)
    assert r.get(key) == "v5"
    assert r.script_exists(sha) == [False]
    with pytest.raises(redis.exceptions.NoScriptError):
        r.evalsha(sha, 1, key, "v7")
    assert any("SCRIPT" in line for line in r.execute_command("SCRIPT", "HELP"))
    sha2 = r.script_load("return 2")
    with pytest.raises(redis.exceptions.ResponseError):
        r.execute_command("SCRIPT", "FLUSH", "BAD")
    cluster_direct.script_flush()
    assert r.evalsha(sha2, 0) == 2
    with pytest.raises(redis.exceptions.ResponseError):
        r.execute_command("SCRIPT", "KILL")


def test_info_counts_commands_and_client_list_names_the_last(r, new_conn, key_prefix):
    k = f"{key_prefix}:cs"
    c = new_conn()
    assert c.get(k) is None
    before = r.info("commandstats")["cmdstat_get"]["calls"]
    assert c.get(k) is None
    assert r.info("commandstats")["cmdstat_get"]["calls"] == before + 1
    assert r.info("cluster")["cluster_enabled"] == 1
    assert r.info("stats")["total_error_replies"] >= 0
    rid, cid = r.client_id(), c.client_id()
    assert c.get(k) is None
    by_id = {int(row["id"]): row for row in r.client_list()}
    assert by_id[rid]["cmd"] == "client|list"
    assert by_id[cid]["cmd"] == "get"
    assert r.info("commandstats")["cmdstat_client|list"]["calls"] >= 1


def test_slowlog_keeps_commands_over_the_threshold(r, new_conn, key_prefix):
    k = f"{key_prefix}:slow"
    assert r.config_set("slowlog-log-slower-than", 0)
    assert r.config_set("slowlog-max-len", 8)
    try:
        assert r.slowlog_reset()
        c = new_conn()
        assert c.client_setname("slowprobe")
        assert c.set(k, "v")
        assert c.get(k) == "v"
        entries = r.slowlog_get()
        commands = [e["command"] for e in entries]
        assert f"GET {k}".encode() in commands and f"SET {k} v".encode() in commands
        mine = next(e for e in entries if e["command"] == f"GET {k}".encode())
        assert mine["client_name"] == b"slowprobe"
        assert b":" in mine["client_address"]
        assert mine["duration"] >= 0
        assert [e["id"] for e in entries] == sorted((e["id"] for e in entries), reverse=True)
        assert r.slowlog_len() == len(entries)
        assert r.config_set("slowlog-max-len", 2)
        assert c.get(k) == "v"
        assert r.slowlog_len() == 2
        assert len(r.slowlog_get(1)) == 1
        with pytest.raises(redis.exceptions.ResponseError):
            r.execute_command("SLOWLOG", "GET", "-2")
        assert r.config_set("slowlog-log-slower-than", -1)
        assert r.slowlog_reset()
        assert c.get(k) == "v"
        assert r.slowlog_len() == 0
        assert r.config_get("slowlog-log-slower-than") == {"slowlog-log-slower-than": "-1"}
    finally:
        r.config_set("slowlog-log-slower-than", 10000)
        r.config_set("slowlog-max-len", 128)
        r.delete(k)


def test_register_script_round_trips(r, key_prefix):
    key = f"{key_prefix}:reg"
    script = r.register_script("return redis.call('incr', KEYS[1])")
    assert script(keys=[key]) == 1
    assert script(keys=[key]) == 2


def test_functions_through_proxy(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (7, 0))
    lib = f"lib{key_prefix[2:10]}"
    code = (
        f"#!lua name={lib}\n"
        f"redis.register_function('{lib}_set', function(keys, args) "
        "return redis.call('set', keys[1], args[1]) end)"
    )
    assert r.function_load(code, replace=True) == lib
    key = f"{key_prefix}:fn"
    assert r.fcall(f"{lib}_set", 1, key, "v") == "OK"
    assert r.get(key) == "v"
    assert lib in str(r.execute_command("FUNCTION", "LIST", "LIBRARYNAME", lib))
    assert any("FUNCTION" in line for line in r.execute_command("FUNCTION", "HELP"))
    assert r.function_delete(lib)
    with pytest.raises(redis.ResponseError):
        r.fcall(f"{lib}_set", 1, key, "v")


def test_acl_users_are_managed_at_runtime(r):
    name = "it_acl_user"
    r.execute_command("ACL", "DELUSER", name)
    rules = ["on", ">pw", "~it:*", "resetchannels", "&chan*", "-@all", "+@string", "+acl|whoami"]
    assert r.execute_command("ACL", "SETUSER", name, *rules) == "OK"
    assert name in r.execute_command("ACL", "USERS")
    line = next(l for l in r.execute_command("ACL", "LIST") if l.startswith(f"user {name} "))
    digest = hashlib.sha256(b"pw").hexdigest()
    assert line == f"user {name} on #{digest} ~it:* &chan* -@all +@string +acl|whoami"
    got = r.execute_command("ACL", "GETUSER", name)
    assert got[:4] == ["flags", ["on"], "passwords", [digest]]
    assert got[4:] == ["commands", "-@all +@string +acl|whoami", "keys", ["it:*"], "channels", ["chan*"]]
    assert r.execute_command("ACL", "GETUSER", "it_no_such_user") is None
    assert len(r.execute_command("ACL", "CAT")) == 22
    assert "get" in r.execute_command("ACL", "CAT", "string")
    with pytest.raises(redis.exceptions.ResponseError, match="Unknown category"):
        r.execute_command("ACL", "CAT", "nosuch")
    with pytest.raises(redis.exceptions.ResponseError, match="Unknown command or category"):
        r.execute_command("ACL", "SETUSER", name, "+nosuchcommand")
    with pytest.raises(redis.exceptions.ResponseError, match="after the \\* pattern"):
        r.execute_command("ACL", "SETUSER", name, "~*", "~more")
    assert len(r.execute_command("ACL", "GENPASS")) == 64
    assert len(r.execute_command("ACL", "GENPASS", "32")) == 8
    assert r.execute_command("ACL", "DELUSER", name, "it_no_such_user") == 1
    with pytest.raises(redis.exceptions.ResponseError, match="default"):
        r.execute_command("ACL", "DELUSER", "default")


def test_acl_restricted_user_is_enforced(r, new_conn, key_prefix):
    name = "it_acl_limited"
    rules = ["reset", "on", ">pw", f"~{key_prefix}:*", "resetchannels", "&news:*", "-@all", "+get", "+set",
             "+publish", "+multi", "+exec", "+acl|whoami"]
    assert r.execute_command("ACL", "SETUSER", name, *rules) == "OK"
    r.execute_command("ACL", "LOG", "RESET")
    c = new_conn()
    assert c.execute_command("AUTH", name, "pw")
    assert c.execute_command("ACL", "WHOAMI") == name
    k = f"{key_prefix}:acl"
    assert c.set(k, "v")
    assert c.get(k) == "v"
    with pytest.raises(redis.exceptions.NoPermissionError, match="run the 'del' command"):
        c.delete(k)
    with pytest.raises(redis.exceptions.NoPermissionError, match="access one of the keys"):
        c.get("other:key")
    with pytest.raises(redis.exceptions.NoPermissionError, match="access one of the channels"):
        c.publish("sports", "x")
    assert c.publish("news:1", "x") == 0
    with pytest.raises(redis.exceptions.NoPermissionError, match="run the 'acl' command"):
        c.execute_command("ACL", "LOG")
    pipe = c.pipeline(transaction=True)
    pipe.set(k, "changed")
    pipe.delete(k)
    with pytest.raises(redis.exceptions.NoPermissionError):
        pipe.execute()
    assert c.get(k) == "v"
    log = r.execute_command("ACL", "LOG")
    assert {e[3] for e in log} == {"command", "key", "channel"}
    assert {e[7] for e in log} >= {"del", "other:key", "sports", "acl"}
    assert {e[9] for e in log} == {name}
    assert "multi" in {e[5] for e in log}
    parsed = r.acl_log()
    assert parsed[0]["client-info"]["db"] == 0 and parsed[0]["client-info"]["user"] == name
    assert c.execute_command("AUTH", name, "pw")
    assert len(r.execute_command("ACL", "LOG", "2")) == 2
    assert r.execute_command("ACL", "LOG", "RESET") == "OK"
    assert r.execute_command("ACL", "LOG") == []
    assert r.execute_command("ACL", "DELUSER", name) == 1


def test_acl_changes_reach_live_sessions(r, new_conn, key_prefix):
    name = "it_acl_live"
    assert r.execute_command("ACL", "SETUSER", name, "reset", "on", ">pw", "~*", "&*", "+@all") == "OK"
    c = new_conn()
    assert c.execute_command("AUTH", name, "pw")
    k = f"{key_prefix}:live"
    assert c.set(k, "1")
    assert r.execute_command("ACL", "SETUSER", name, "-set") == "OK"
    with pytest.raises(redis.exceptions.NoPermissionError, match="run the 'set' command"):
        c.set(k, "2")
    assert c.get(k) == "1"
    assert r.execute_command("ACL", "SETUSER", name, "off") == "OK"
    with pytest.raises(redis.exceptions.AuthenticationError):
        new_conn().execute_command("AUTH", name, "pw")
    assert c.get(k) == "1"
    assert r.execute_command("ACL", "DELUSER", name) == 1
    with pytest.raises(redis.exceptions.ConnectionError):
        c.get(k)


def test_hello_auth_and_acl_config(r, new_conn, raw_socket):
    name = "it_acl_hello"
    assert r.execute_command("ACL", "SETUSER", name, "reset", "on", ">pw", "~*", "&*", "+@all") == "OK"
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["HELLO", "2", "AUTH", name, "bad"]))
    with pytest.raises(redis.exceptions.ResponseError, match="WRONGPASS"):
        reader.read_reply()
    s.sendall(_resp_encode(["HELLO", "2", "AUTH", name, "pw"]))
    assert "proto" in reader.read_reply()
    s.sendall(_resp_encode(["ACL", "WHOAMI"]))
    assert reader.read_reply() == name
    assert r.config_get("acl-pubsub-default") == {"acl-pubsub-default": "allchannels"}
    assert r.config_set("acllog-max-len", 2)
    assert r.config_get("acllog-max-len") == {"acllog-max-len": "2"}
    r.execute_command("ACL", "LOG", "RESET")
    for _ in range(3):
        with pytest.raises(redis.exceptions.AuthenticationError):
            new_conn().execute_command("AUTH", name, "nope")
    log = r.execute_command("ACL", "LOG")
    assert len(log) == 2 and {e[3] for e in log} == {"auth"}
    assert r.config_set("acllog-max-len", 128)
    with pytest.raises(redis.exceptions.ResponseError, match="acl-pubsub-default"):
        r.config_set("acl-pubsub-default", "sometimes")
    assert r.execute_command("ACL", "DELUSER", name) == 1


def test_acl_categories_reach_subcommands(r, new_conn):
    name = "it_acl_nodanger"
    rules = ["reset", "on", ">pw", "~*", "&*", "+@all", "-@dangerous"]
    assert r.execute_command("ACL", "SETUSER", name, *rules) == "OK"
    c = new_conn()
    assert c.execute_command("AUTH", name, "pw")
    with pytest.raises(redis.exceptions.NoPermissionError):
        c.execute_command("ACL", "SETUSER", name, "+@all")
    assert c.execute_command("ACL", "WHOAMI") == name
    with pytest.raises(redis.exceptions.NoPermissionError):
        c.execute_command("CONFIG", "GET", "loglevel")
    assert r.execute_command("ACL", "SETUSER", name, "+config", "-config|set") == "OK"
    assert c.config_get("loglevel")
    with pytest.raises(redis.exceptions.NoPermissionError):
        c.config_set("loglevel", "notice")
    assert "acl|setuser" in r.execute_command("ACL", "CAT", "dangerous")
    with pytest.raises(redis.exceptions.ResponseError):
        r.execute_command("ACL", "SETUSER", name, "")
    with pytest.raises(redis.exceptions.ResponseError):
        r.execute_command("ACL", "WHOAMI", "extra")
    assert r.execute_command("ACL", "DELUSER", name) == 1


def test_default_user_off_requires_another_login(r, raw_socket):
    assert r.execute_command("ACL", "SETUSER", "it_acl_alt", "reset", "on", ">pw", "~*", "&*", "+@all") == "OK"
    assert r.execute_command("ACL", "SETUSER", "default", "off") == "OK"
    try:
        s = raw_socket()
        reader = _RespReader(s)
        s.sendall(_resp_encode(["PING"]))
        with pytest.raises(redis.exceptions.ResponseError, match="NOAUTH"):
            reader.read_reply()
        s.sendall(_resp_encode(["AUTH", "it_acl_alt", "pw"]))
        assert reader.read_reply() == "OK"
        s.sendall(_resp_encode(["PING"]))
        assert reader.read_reply() == "PONG"
    finally:
        assert r.execute_command("ACL", "SETUSER", "default", "on") == "OK"
        r.execute_command("ACL", "DELUSER", "it_acl_alt")


def test_redis84_delex_digest_msetex(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (8, 4), (99, 0))
    k = f"{key_prefix}:dx"
    assert r.set(k, "v")
    assert len(r.execute_command("DIGEST", k)) == 16
    assert r.execute_command("DELEX", k, "IFEQ", "nope") == 0
    assert r.execute_command("DELEX", k, "IFEQ", "v") == 1
    assert r.get(k) is None


def test_msetex_shares_one_expiry(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (8, 4), (9, 0))
    a, b = (f"{{{key_prefix}}}:m{i}" for i in (1, 2))
    assert r.execute_command("MSETEX", 2, a, "1", b, "2", "EX", 100) == 1
    assert r.get(a) == "1" and 0 < r.ttl(b) <= 100


def test_redis88_increx_and_arrays(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (8, 8), (99, 0))
    c, arr = f"{key_prefix}:ix", f"{key_prefix}:ar"
    assert r.execute_command("INCREX", c, "EX", 50)[0] == 1
    assert r.execute_command("INCREX", c, "EX", 50)[0] == 2
    assert 0 < r.ttl(c) <= 50
    assert r.execute_command("ARSET", arr, 0, "hello", "world") == 2
    assert r.execute_command("ARGET", arr, 1) == "world"
    assert r.execute_command("ARLEN", arr) == 2


def test_redis810_lmovem_and_set_cardinalities(r, cluster_direct, key_prefix):
    _needs(cluster_direct, (8, 10), (99, 0))
    src, dst = (f"{{{key_prefix}}}:l{i}" for i in (1, 2))
    assert r.rpush(src, "1", "2", "3", "4") == 4
    assert r.execute_command("LMOVEM", src, dst, "LEFT", "RIGHT", "COUNT", 2, "OBO") == ["1", "2"]
    assert r.execute_command("BLMOVEM", src, dst, "LEFT", "RIGHT", 0.1, "COUNT", 5, "OBO") == ["3", "4"]
    assert r.execute_command("BLMOVEM", src, dst, "LEFT", "RIGHT", 0.1) is None
    assert r.lrange(dst, 0, -1) == ["1", "2", "3", "4"]
    s1, s2 = (f"{{{key_prefix}}}:s{i}" for i in (1, 2))
    r.sadd(s1, "a", "b", "c")
    r.sadd(s2, "a")
    assert r.execute_command("SDIFFCARD", 2, s1, s2) == 2
    assert r.execute_command("SUNIONCARD", 2, s1, s2) == 3


def test_watch_aborts_exec_after_a_foreign_write(r, cluster_direct, raw_socket, key_prefix):
    k = f"{key_prefix}:w"
    assert r.set(k, "0")
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["WATCH", k]))
    assert reader.read_reply() == "OK"
    cluster_direct.set(k, "changed")
    for cmd in (["MULTI"], ["SET", k, "mine"], ["EXEC"]):
        s.sendall(_resp_encode(cmd))
    assert [reader.read_reply() for _ in range(3)] == ["OK", "QUEUED", None]
    assert r.get(k) == "changed"
    s.sendall(_resp_encode(["WATCH", k]))
    assert reader.read_reply() == "OK"
    for cmd in (["MULTI"], ["INCR", k], ["EXEC"]):
        s.sendall(_resp_encode(cmd))
    assert reader.read_reply() == "OK" and reader.read_reply() == "QUEUED"
    with pytest.raises(redis.exceptions.ResponseError):
        reader.read_reply()
    assert r.get(k) == "changed"
    s.sendall(_resp_encode(["UNWATCH"]))
    assert reader.read_reply() == "OK"


def test_watch_rules_and_redis_py_flow(r, cluster_direct, raw_socket, key_prefix):
    near, far = _cross_slot_pair(key_prefix)
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["WATCH", near, far]))
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
        reader.read_reply()
    s.sendall(_resp_encode(["WATCH", near]))
    assert reader.read_reply() == "OK"
    for cmd in (["MULTI"], ["SET", far, "1"]):
        s.sendall(_resp_encode(cmd))
    assert reader.read_reply() == "OK"
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
        reader.read_reply()
    s.sendall(_resp_encode(["WATCH", near]))
    with pytest.raises(redis.exceptions.ResponseError, match="inside MULTI"):
        reader.read_reply()
    s.sendall(_resp_encode(["DISCARD"]))
    assert reader.read_reply() == "OK"
    k = f"{key_prefix}:opt"
    assert r.set(k, "1")
    with r.pipeline() as pipe:
        pipe.watch(k)
        current = int(pipe.get(k))
        pipe.multi()
        pipe.set(k, current + 1)
        assert pipe.execute() == [True]
    assert r.get(k) == "2"
    with r.pipeline() as pipe:
        pipe.watch(k)
        cluster_direct.set(k, "9")
        pipe.multi()
        pipe.set(k, "3")
        with pytest.raises(redis.WatchError):
            pipe.execute()
    assert r.get(k) == "9"


def test_watch_orders_pipelined_neighbours(cluster_direct, raw_socket, key_prefix):
    k = f"{key_prefix}:fence"
    cluster_direct.delete(k)
    s = raw_socket()
    reader = _RespReader(s)
    for _ in range(3):
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["SET", k, "1"], ["WATCH", k], ["MULTI"], ["SET", k, "2"], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(5)] == ["OK", "OK", "OK", "QUEUED", ["OK"]]
        assert cluster_direct.get(k) == "2"
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["SET", k, "3"], ["MULTI"], ["INCR", k], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(5)] == ["OK", "OK", "OK", "QUEUED", None]
        assert cluster_direct.get(k) == "3"
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["DEL", k], ["MULTI"], ["SET", k, "4"], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(5)] == ["OK", 1, "OK", "QUEUED", None]
        assert cluster_direct.get(k) is None
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["BLPOP", k, "0.3"], ["WATCH", k], ["MULTI"], ["LPUSH", k, "v"], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(5)] == [None, "OK", "OK", "QUEUED", [1]]
        assert cluster_direct.lrange(k, 0, -1) == ["v"]
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["BLPOP", k, "0.3"], ["MULTI"], ["LPUSH", k, "w"], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(5)] == ["OK", [k, "v"], "OK", "QUEUED", None]
        assert cluster_direct.lrange(k, 0, -1) == []
        other = _cross_slot_pair(key_prefix)[1]
        assert cluster_direct.set(k, "5") and cluster_direct.set(other, "5")
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["DEL", k, other], ["MULTI"], ["SET", k, "6"], ["EXEC"])
            )
        )
        assert reader.read_reply() == "OK"
        with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
            reader.read_reply()
        assert [reader.read_reply() for _ in range(3)] == ["OK", "QUEUED", ["OK"]]
        assert cluster_direct.get(k) == "6" and cluster_direct.get(other) == "5"
        cluster_direct.delete(k, other)
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["SET", k, "7"], ["UNWATCH"], ["GET", k], ["DEL", k, other])
            )
        )
        assert [reader.read_reply() for _ in range(4)] == ["OK", "OK", "OK", "7"]
        with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
            reader.read_reply()
        for _ in range(100):
            s.sendall(_resp_encode(["DEL", k, other]))
            try:
                assert reader.read_reply() == 1
                break
            except redis.exceptions.ResponseError:
                time.sleep(0.01)
        else:
            pytest.fail("the released connection never went quiet")
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["MULTI"], ["SET", k, "8"], ["EXEC"], ["GET", k], ["MULTI"], ["SET", other, "9"], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(8)] == ["OK", "OK", "QUEUED", ["OK"], "8", "OK", "QUEUED", ["OK"]]
        assert cluster_direct.get(other) == "9"
        cluster_direct.delete(k, other)
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["MULTI"], ["SET", k], ["EXEC"], ["MULTI"], ["SET", other, "10"], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(2)] == ["OK", "OK"]
        with pytest.raises(redis.exceptions.ResponseError, match="wrong number"):
            reader.read_reply()
        with pytest.raises(redis.exceptions.ResponseError, match="EXECABORT"):
            reader.read_reply()
        assert [reader.read_reply() for _ in range(3)] == ["OK", "QUEUED", ["OK"]]
        assert cluster_direct.get(other) == "10"
        started = time.time()
        s.sendall(
            b"".join(_resp_encode(c) for c in (["WATCH", k], ["UNWATCH"], ["BLPOP", k, "0.5"]))
        )
        assert [reader.read_reply() for _ in range(2)] == ["OK", "OK"]
        assert time.time() - started < 0.4
        s.sendall(_resp_encode(["LPUSH", k, "late"]))
        assert reader.read_reply() is None
        assert reader.read_reply() == 1
        assert time.time() - started >= 0.5
        assert cluster_direct.lrange(k, 0, -1) == ["late"]
        cluster_direct.delete(k, other)
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["MULTI"], ["SET", k, "1"], ["EXEC"], ["MULTI"], ["INCR", k], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(7)] == ["OK", "OK", "QUEUED", ["OK"], "OK", "QUEUED", [2]]
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["DEL", k], ["WATCH", k], ["UNWATCH"], ["BLPOP", k, "0.3"], ["WATCH", other], ["LPUSH", k, "v"], ["UNWATCH"])
            )
        )
        assert [reader.read_reply() for _ in range(7)] == [1, "OK", "OK", None, "OK", 1, "OK"]
        assert cluster_direct.lrange(k, 0, -1) == ["v"]
        cluster_direct.delete(k, other)
        s.sendall(
            b"".join(
                _resp_encode(c)
                for c in (["WATCH", k], ["UNWATCH"], ["BLPOP", k, "0.3"], ["WATCH", other], ["UNWATCH"], ["MULTI"], ["LPUSH", k, "w"], ["EXEC"])
            )
        )
        assert [reader.read_reply() for _ in range(8)] == ["OK", "OK", None, "OK", "OK", "OK", "QUEUED", [1]]
        assert cluster_direct.lrange(k, 0, -1) == ["w"]
        cluster_direct.delete(k, other)


def test_watch_sees_flushall(r, raw_socket, key_prefix):
    k = f"{key_prefix}:flushed"
    assert r.set(k, "1")
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(b"".join(_resp_encode(c) for c in (["WATCH", k], ["FLUSHALL"])))
    assert [reader.read_reply() for _ in range(2)] == ["OK", "OK"]
    s.sendall(b"".join(_resp_encode(c) for c in (["MULTI"], ["SET", k, "2"], ["EXEC"], ["GET", k])))
    assert [reader.read_reply() for _ in range(4)] == ["OK", "QUEUED", None, None]


def test_watch_reads_the_master_uncached(cache_proxy, cluster_direct, key_prefix):
    k = f"{key_prefix}:fresh"
    assert cache_proxy.set(k, "0")
    assert cache_proxy.get(k) == "0"
    assert cache_proxy.get(k) == "0"
    for i in range(1, 4):
        cluster_direct.set(k, str(i))
        with cache_proxy.pipeline() as pipe:
            pipe.watch(k)
            assert pipe.get(k) == str(i)
            pipe.multi()
            pipe.incr(k)
            assert pipe.execute() == [i + 1]
    src, dst = f"{{{key_prefix}}}:src", f"{{{key_prefix}}}:dst"
    assert cache_proxy.mset({src: "new", dst: "old"})
    assert cache_proxy.get(dst) == "old"
    assert cache_proxy.get(dst) == "old"
    with cache_proxy.pipeline() as pipe:
        pipe.watch(src)
        assert pipe.execute_command("RENAME", src, dst) is True
        assert cache_proxy.get(dst) == "new"
        pipe.unwatch()


def test_sharded_pubsub_routes_by_slot(r, cluster_direct, raw_socket, key_prefix):
    _needs(cluster_direct, (7, 0))
    ch = f"{key_prefix}:{{sh}}:news"
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["SSUBSCRIBE", ch]))
    assert reader.read_reply() == ["ssubscribe", ch, 1]
    assert r.execute_command("SPUBLISH", ch, "hi") == 1
    assert reader.read_reply() == ["smessage", ch, "hi"]
    cluster_direct.execute_command("SPUBLISH", ch, "direct")
    assert reader.read_reply() == ["smessage", ch, "direct"]
    s.sendall(_resp_encode(["PING"]))
    assert reader.read_reply() == ["pong", ""]
    near, far = _cross_slot_pair(key_prefix)
    s.sendall(_resp_encode(["SSUBSCRIBE", near, far]))
    with pytest.raises(redis.exceptions.ResponseError, match=r"(?i)crossslot"):
        reader.read_reply()
    s.sendall(_resp_encode(["SUNSUBSCRIBE"]))
    assert reader.read_reply() == ["sunsubscribe", ch, 0]
    s.sendall(_resp_encode(["PING"]))
    assert reader.read_reply() == "PONG"


def test_pubsub_overlapping_commands_keep_the_subscription(r, raw_socket, key_prefix):
    a, b, key = f"{key_prefix}:ov:a", f"{key_prefix}:ov:b", f"{key_prefix}:ov:k"
    bulk = [f"{key_prefix}:ov:c{i}" for i in range(20000)]
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["SUBSCRIBE", a, b]))
    assert reader.read_reply() == ["subscribe", a, 1]
    assert reader.read_reply() == ["subscribe", b, 2]
    for _ in range(2):
        s.sendall(_resp_encode(["SUBSCRIBE", *bulk]))
        for n in range(len(bulk)):
            assert reader.read_reply() == ["subscribe", bulk[n], 3 + n]
        s.sendall(
            _resp_encode(["UNSUBSCRIBE", a])
            + _resp_encode(["UNSUBSCRIBE", *bulk])
            + _resp_encode(["SUBSCRIBE", a])
            + _resp_encode(["UNSUBSCRIBE", b])
        )
        assert reader.read_reply() == ["unsubscribe", a, len(bulk) + 1]
        s.sendall(_resp_encode(["GET", key]))
        for n in range(len(bulk)):
            assert reader.read_reply() == ["unsubscribe", bulk[n], len(bulk) - n]
        assert reader.read_reply() == ["subscribe", a, 2]
        assert reader.read_reply() == ["unsubscribe", b, 1]
        with pytest.raises(redis.exceptions.ResponseError, match=r"only \(P\|S\)SUBSCRIBE"):
            reader.read_reply()
        s.sendall(_resp_encode(["SUBSCRIBE", b]))
        assert reader.read_reply() == ["subscribe", b, 2]
    r.publish(a, "still")
    assert reader.read_reply() == ["message", a, "still"]


def test_pubsub_bare_unsubscribe_behind_overlapping_commands(raw_socket, key_prefix):
    a, b = f"{key_prefix}:bu:a", f"{key_prefix}:bu:b"
    bulk = [f"{key_prefix}:bu:c{i}" for i in range(20000)]
    s = raw_socket()
    s.settimeout(30)
    reader = _RespReader(s)
    s.sendall(_resp_encode(["SUBSCRIBE", b]))
    assert reader.read_reply() == ["subscribe", b, 1]
    s.sendall(_resp_encode(["SUBSCRIBE", a, *bulk]) + _resp_encode(["UNSUBSCRIBE", a]))
    assert reader.read_reply() == ["subscribe", a, 2]
    s.sendall(_resp_encode(["UNSUBSCRIBE"]) + _resp_encode(["PING"]))
    for n in range(len(bulk)):
        assert reader.read_reply() == ["subscribe", bulk[n], 3 + n]
    assert reader.read_reply() == ["unsubscribe", a, len(bulk) + 1]
    gone = set()
    for _ in range(len(bulk) + 1):
        kind, name, _ = reader.read_reply()
        assert kind == "unsubscribe"
        gone.add(name)
    assert gone == {b, *bulk}
    assert reader.read_reply() == "PONG"


def test_pubsub_regular_command_behind_pipelined_unsubscribe(r, raw_socket, key_prefix):
    b, key = f"{key_prefix}:rc:b", f"{key_prefix}:rc:k"
    bulk = [f"{key_prefix}:rc:c{i}" for i in range(20000)]
    r.set(key, "v")
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["SUBSCRIBE", b]))
    assert reader.read_reply() == ["subscribe", b, 1]
    s.sendall(_resp_encode(["SUBSCRIBE", *bulk]) + _resp_encode(["UNSUBSCRIBE"]) + _resp_encode(["GET", key]))
    for n in range(len(bulk)):
        assert reader.read_reply() == ["subscribe", bulk[n], 2 + n]
    gone = set()
    for _ in range(len(bulk) + 1):
        kind, name, _ = reader.read_reply()
        assert kind == "unsubscribe"
        gone.add(name)
    assert gone == {b, *bulk}
    assert reader.read_reply() == "v"
    s.sendall(_resp_encode(["SUBSCRIBE", b]) + _resp_encode(["PING"]))
    assert reader.read_reply() == ["subscribe", b, 1]
    assert reader.read_reply() == ["pong", ""]


def test_pubsub_reset_behind_pipelined_subscribe(raw_socket, key_prefix):
    a = f"{key_prefix}:rs:a"
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["SUBSCRIBE", a]) + _resp_encode(["RESET"]) + _resp_encode(["PING"]))
    assert reader.read_reply() == ["subscribe", a, 1]
    assert reader.read_reply() == "RESET"
    assert reader.read_reply() == "PONG"


def test_sharded_channel_follows_its_slot_away(cluster_direct, raw_socket, key_prefix):
    _needs(cluster_direct, (7, 0))
    ch = f"{key_prefix}:{{mv}}:news"
    slot = key_slot(ch.encode())
    _, _, src, dst = _shard_pair(cluster_direct, ch)
    src_id, dst_id = src.execute_command("CLUSTER MYID"), dst.execute_command("CLUSTER MYID")
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["SSUBSCRIBE", ch]))
    assert reader.read_reply() == ["ssubscribe", ch, 1]
    try:
        src.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", dst_id)
        dst.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", src_id)
        dst.execute_command("CLUSTER SETSLOT", slot, "NODE", dst_id)
        src.execute_command("CLUSTER SETSLOT", slot, "NODE", dst_id)
        assert reader.read_reply() == ["sunsubscribe", ch, 0]
        s.sendall(_resp_encode(["PING"]))
        assert reader.read_reply() == "PONG"
    finally:
        dst.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", src_id)
        src.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", dst_id)
        src.execute_command("CLUSTER SETSLOT", slot, "NODE", src_id)
        dst.execute_command("CLUSTER SETSLOT", slot, "NODE", src_id)
        src.close()
        dst.close()


def test_sharded_channels_stay_on_one_node(cluster_direct, raw_socket, key_prefix):
    _needs(cluster_direct, (7, 0))
    first = f"{key_prefix}:sa"
    owner = cluster_direct.get_node_from_key(first)
    other = next(
        f"{key_prefix}:sb{i}"
        for i in range(1000)
        if (n := cluster_direct.get_node_from_key(f"{key_prefix}:sb{i}")).host != owner.host
        or n.port != owner.port
    )
    s = raw_socket()
    reader = _RespReader(s)
    s.sendall(_resp_encode(["SSUBSCRIBE", first]))
    assert reader.read_reply() == ["ssubscribe", first, 1]
    s.sendall(_resp_encode(["SSUBSCRIBE", other]))
    with pytest.raises(redis.exceptions.ResponseError, match="one node"):
        reader.read_reply()
    s.sendall(_resp_encode(["SUNSUBSCRIBE", first]))
    assert reader.read_reply() == ["sunsubscribe", first, 0]

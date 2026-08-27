import os
import socket
import uuid

import pytest
import redis
from redis.cluster import RedisCluster

PROXY_HOST = os.environ.get("MITHRIL_HOST", "172.28.0.30")
PROXY_PORT = int(os.environ.get("MITHRIL_PORT", "7979"))
CLUSTER_HOST = os.environ.get("MITHRIL_CLUSTER_HOST", "172.28.0.11")
CLUSTER_PORT = int(os.environ.get("MITHRIL_CLUSTER_PORT", "7001"))


@pytest.fixture(scope="session")
def proxy_addr():
    return PROXY_HOST, PROXY_PORT


@pytest.fixture(scope="session")
def r():
    """RESP2 client against the proxy."""
    client = redis.Redis(host=PROXY_HOST, port=PROXY_PORT, decode_responses=True)
    client.ping()
    yield client
    client.close()


@pytest.fixture(scope="session")
def r3():
    """RESP3 client against the proxy."""
    client = redis.Redis(host=PROXY_HOST, port=PROXY_PORT, protocol=3, decode_responses=True)
    client.ping()
    yield client
    client.close()


@pytest.fixture(scope="session")
def cluster_direct():
    """Direct redis-cluster client, bypassing the proxy, for cross-checks."""
    client = RedisCluster(host=CLUSTER_HOST, port=CLUSTER_PORT, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def new_conn():
    """Factory for extra proxy connections (pubsub, blocking, multi-conn tests)."""
    conns = []

    def _make(protocol=2, decode_responses=True):
        c = redis.Redis(
            host=PROXY_HOST, port=PROXY_PORT, protocol=protocol, decode_responses=decode_responses
        )
        conns.append(c)
        return c

    yield _make
    for c in conns:
        c.close()


@pytest.fixture
def key_prefix():
    return f"it{uuid.uuid4().hex[:12]}"


@pytest.fixture
def raw_socket():
    """Factory for raw TCP sockets to the proxy (protocol edge-case tests)."""
    socks = []

    def _make(timeout=5.0):
        s = socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=timeout)
        socks.append(s)
        return s

    yield _make
    for s in socks:
        try:
            s.close()
        except OSError:
            pass

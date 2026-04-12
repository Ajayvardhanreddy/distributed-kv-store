"""
Unit tests for Phase 4 replication.

Tests cover:
  - get_nodes() on the hash ring
  - ClusterRouter fan-out for PUT (all N nodes receive the write)
  - ClusterRouter fan-out for DELETE
  - Replica failure → RuntimeError propagated
  - N=1 disables replication (single-node behaviour)
"""
import os
import tempfile
from unittest.mock import AsyncMock, call

import pytest

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.storage.engine import StorageEngine


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def ring_with(*node_ids: str, num_vnodes: int = 50) -> ConsistentHashRing:
    r = ConsistentHashRing(num_vnodes=num_vnodes)
    for nid in node_ids:
        r.add_node(nid)
    return r


async def make_router(
    tmpdir: str,
    node_id: str,
    peers: list[str],
    monkeypatch,
    replication_factor: int = 2,
) -> tuple[ClusterRouter, StorageEngine]:
    monkeypatch.setenv("NODE_ID", node_id)
    monkeypatch.setenv("PEERS", ",".join(peers))
    monkeypatch.setenv("REPLICATION_FACTOR", str(replication_factor))

    cfg = NodeConfig()
    ring = ring_with(*cfg.node_ids)

    wal = os.path.join(tmpdir, f"{node_id}.wal")
    storage = StorageEngine(wal)
    await storage.initialize()

    router = ClusterRouter(cfg, storage, ring)
    await router.initialize()
    return router, storage


# -----------------------------------------------------------------------
# ConsistentHashRing.get_nodes()
# -----------------------------------------------------------------------

def test_get_nodes_returns_n_distinct_nodes():
    r = ring_with("node-0", "node-1", "node-2")
    nodes = r.get_nodes("any-key", n=2)
    assert len(nodes) == 2
    assert len(set(nodes)) == 2          # all distinct physical nodes


def test_get_nodes_primary_matches_get_node():
    r = ring_with("node-0", "node-1", "node-2")
    for k in [f"key:{i}" for i in range(50)]:
        assert r.get_nodes(k, n=1)[0] == r.get_node(k)


def test_get_nodes_capped_at_total_nodes():
    r = ring_with("node-0", "node-1")
    # Ask for 5, only 2 physical nodes exist
    nodes = r.get_nodes("key", n=5)
    assert len(nodes) == 2


def test_get_nodes_empty_ring():
    r = ConsistentHashRing()
    assert r.get_nodes("key", n=2) == []


def test_get_nodes_single_node_ring():
    r = ring_with("node-0")
    nodes = r.get_nodes("key", n=2)  # n=2 but only 1 node
    assert nodes == ["node-0"]


def test_get_nodes_is_deterministic():
    r = ring_with("node-0", "node-1", "node-2")
    key = "user:42"
    assert r.get_nodes(key, n=2) == r.get_nodes(key, n=2)


def test_get_nodes_covers_all_physical_nodes():
    """Over many keys, every physical node should appear as primary."""
    r = ring_with("node-0", "node-1", "node-2")
    primaries = {r.get_nodes(f"k:{i}", n=1)[0] for i in range(300)}
    assert primaries == {"node-0", "node-1", "node-2"}


# -----------------------------------------------------------------------
# ClusterRouter: replication_factor wired correctly
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replication_factor_from_env(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        router, storage = await make_router(
            tmp, "node-0", ["http://localhost:8000"], monkeypatch,
            replication_factor=1,
        )
        assert router.replication_factor == 1
        await router.close(); await storage.close()


@pytest.mark.asyncio
async def test_replicas_of_returns_n_nodes(monkeypatch):
    peers = ["http://localhost:8000", "http://localhost:8001",
             "http://localhost:8002"]
    with tempfile.TemporaryDirectory() as tmp:
        router, storage = await make_router(
            tmp, "node-0", peers, monkeypatch, replication_factor=2
        )
        rset = router.replicas_of("hello")
        assert len(rset) == 2
        assert len(set(rset)) == 2
        await router.close(); await storage.close()


# -----------------------------------------------------------------------
# ClusterRouter: fan-out PUT
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_fans_out_to_all_replicas(monkeypatch):
    """
    With rf=2, put() must write to leader first, then replicate to remaining
    nodes.  We mock _forward_put (leader path) and _forward_put_versioned
    (replica path) and verify they get called for remote nodes.
    """
    peers = ["http://localhost:8000", "http://localhost:8001",
             "http://localhost:8002"]
    with tempfile.TemporaryDirectory() as tmp:
        router, storage = await make_router(
            tmp, "node-0", peers, monkeypatch, replication_factor=2
        )
        router._forward_put = AsyncMock(return_value=1)
        router._forward_put_versioned = AsyncMock(return_value=None)

        await router.put("test-key", "test-value")

        replication_set = router.replicas_of("test-key")
        remote_nodes = [n for n in replication_set
                        if not router.config.is_local(n)]
        # Either _forward_put (leader) or _forward_put_versioned (replica)
        # was called for each remote node
        total_remote_calls = (
            router._forward_put.call_count
            + router._forward_put_versioned.call_count
        )
        assert total_remote_calls == len(remote_nodes)

        await router.close(); await storage.close()


@pytest.mark.asyncio
async def test_put_with_rf1_no_replication(monkeypatch):
    """When rf=1, put() behaves exactly like Phase 3 — primary only."""
    with tempfile.TemporaryDirectory() as tmp:
        router, storage = await make_router(
            tmp, "node-0", ["http://localhost:8000"], monkeypatch,
            replication_factor=1,
        )
        router._forward_put = AsyncMock(return_value=1)

        await router.put("key", "value")

        # Single node → all local → _forward_put never called
        router._forward_put.assert_not_called()
        value, version = await storage.get("key")
        assert value == "value"

        await router.close(); await storage.close()


# -----------------------------------------------------------------------
# ClusterRouter: fan-out DELETE
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_fans_out_to_replicas(monkeypatch):
    """delete() must remove the key from all nodes in the replication set."""
    peers = ["http://localhost:8000", "http://localhost:8001",
             "http://localhost:8002"]
    with tempfile.TemporaryDirectory() as tmp:
        router, storage = await make_router(
            tmp, "node-0", peers, monkeypatch, replication_factor=2
        )

        # Seed the key directly into local storage so the existence check passes
        test_key = "delete-me"
        # First find a key that belongs to node-0 (our local node)
        local_key = None
        for i in range(200):
            k = f"delete:{i}"
            if router.owner_of(k) == "node-0":
                local_key = k
                break
        assert local_key, "Could not find a key owned by node-0"

        await storage.put(local_key, "v")

        router._forward_put = AsyncMock(return_value=None)
        router._forward_delete_replica = AsyncMock(return_value=None)

        result = await router.delete(local_key)
        assert result is True
        value, _ = await storage.get(local_key)
        assert value is None   # removed locally

        # Replica delete was forwarded for remaining nodes in replication set
        replication_set = router.replicas_of(local_key)
        remote_replicas = [n for n in replication_set
                           if not router.config.is_local(n)]
        assert router._forward_delete_replica.call_count == len(remote_replicas)

        await router.close(); await storage.close()


@pytest.mark.asyncio
async def test_delete_nonexistent_returns_false(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        router, storage = await make_router(
            tmp, "node-0", ["http://localhost:8000"], monkeypatch,
            replication_factor=1,
        )
        assert await router.delete("no-such-key") is False
        await router.close(); await storage.close()


# -----------------------------------------------------------------------
# ClusterRouter: replica failure → 503
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replica_failure_raises_runtime_error(monkeypatch):
    """If a replica is unreachable during PUT, RuntimeError must propagate."""
    peers = ["http://localhost:8000", "http://localhost:8001",
             "http://localhost:8002"]
    with tempfile.TemporaryDirectory() as tmp:
        router, storage = await make_router(
            tmp, "node-0", peers, monkeypatch, replication_factor=2
        )
        # Make any remote forward fail
        router._forward_put = AsyncMock(
            side_effect=RuntimeError("Peer node-1 unreachable")
        )
        router._forward_put_versioned = AsyncMock(
            side_effect=RuntimeError("Peer node-1 unreachable")
        )

        # Find a key that has at least one remote node in its replication set
        remote_key = next(
            k for i in range(500)
            if (k := f"probe:{i}")
            and any(not router.config.is_local(n) for n in router.replicas_of(k))
        )

        with pytest.raises(RuntimeError, match="unreachable"):
            await router.put(remote_key, "value")

        await router.close(); await storage.close()

"""
Unit tests for ClusterRouter.

Tests local routing and HTTP forwarding logic.
Forwarding is tested by mocking the httpx.AsyncClient so no real
peer processes are needed.
"""
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.storage.engine import StorageEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_ring(node_ids: list[str], num_vnodes: int = 10) -> ConsistentHashRing:
    ring = ConsistentHashRing(num_vnodes=num_vnodes)
    for nid in node_ids:
        ring.add_node(nid)
    return ring


async def make_router(
    tmpdir: str, node_id: str, peers: list[str], monkeypatch,
    replication_factor: int = 1,
) -> ClusterRouter:
    """Build a ClusterRouter with node_id as the local node."""
    peer_str = ",".join(peers)
    monkeypatch.setenv("NODE_ID", node_id)
    monkeypatch.setenv("PEERS", peer_str)
    monkeypatch.setenv("REPLICATION_FACTOR", str(replication_factor))

    cfg = NodeConfig()
    ring = make_ring(cfg.node_ids)

    wal = os.path.join(tmpdir, f"{node_id}.wal")
    storage = StorageEngine(wal)
    await storage.initialize()

    router = ClusterRouter(cfg, storage, ring)
    await router.initialize()
    return router, storage


# ---------------------------------------------------------------------------
# Tests: single-node (all keys are local)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_local_put_get(monkeypatch):
    """All operations are local when there is only one node."""
    with tempfile.TemporaryDirectory() as tmpdir:
        router, storage = await make_router(
            tmpdir, "node-0", ["http://localhost:8000"], monkeypatch
        )
        await router.put("hello", "world")
        value, version = await router.get("hello")
        assert value == "world"
        await router.close()
        await storage.close()


@pytest.mark.asyncio
async def test_local_delete(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        router, storage = await make_router(
            tmpdir, "node-0", ["http://localhost:8000"], monkeypatch
        )
        await router.put("key", "val")
        assert await router.delete("key") is True
        value, version = await router.get("key")
        assert value is None
        await router.close()
        await storage.close()


@pytest.mark.asyncio
async def test_local_delete_nonexistent(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        router, storage = await make_router(
            tmpdir, "node-0", ["http://localhost:8000"], monkeypatch
        )
        assert await router.delete("no-such-key") is False
        await router.close()
        await storage.close()


@pytest.mark.asyncio
async def test_local_size(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        router, storage = await make_router(
            tmpdir, "node-0", ["http://localhost:8000"], monkeypatch
        )
        assert await router.local_size() == 0
        await router.put("k1", "v1")
        assert await router.local_size() == 1
        await router.close()
        await storage.close()


# ---------------------------------------------------------------------------
# Tests: two-node ring — verify forwarding is triggered
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_routing_is_deterministic(monkeypatch):
    """
    With 2 nodes, the router must CONSISTENTLY decide the same owner
    for a given key across multiple calls.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        peers = ["http://localhost:8000", "http://localhost:8001"]
        router, storage = await make_router(tmpdir, "node-0", peers, monkeypatch)

        key = "user:42"
        owner1 = router.owner_of(key)
        owner2 = router.owner_of(key)
        assert owner1 == owner2   # must be deterministic

        await router.close()
        await storage.close()


@pytest.mark.asyncio
async def test_forward_get_called_for_remote_key(monkeypatch):
    """
    When the owning node is not this node, ClusterRouter must call
    _forward_get (i.e. make an HTTP request to the peer).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        peers = ["http://localhost:8000", "http://localhost:8001"]
        router, storage = await make_router(tmpdir, "node-0", peers, monkeypatch)

        # Find a key owned by node-1 (not node-0)
        remote_key = None
        for i in range(500):
            k = f"probe:{i}"
            if router.owner_of(k) != "node-0":
                remote_key = k
                break

        assert remote_key is not None, "Could not find a remote key in 500 tries"

        # Mock _forward_get so no real HTTP call is made
        router._forward_get = AsyncMock(return_value=("mocked-value", 1))

        value, version = await router.get(remote_key)
        assert value == "mocked-value"
        router._forward_get.assert_awaited_once_with(
            router.owner_of(remote_key), remote_key
        )

        await router.close()
        await storage.close()


@pytest.mark.asyncio
async def test_forward_put_called_for_remote_key(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        peers = ["http://localhost:8000", "http://localhost:8001"]
        router, storage = await make_router(tmpdir, "node-0", peers, monkeypatch)

        remote_key = None
        for i in range(500):
            k = f"probe:{i}"
            if router.owner_of(k) != "node-0":
                remote_key = k
                break

        assert remote_key is not None

        router._forward_put = AsyncMock(return_value=1)
        await router.put(remote_key, "some-value")
        router._forward_put.assert_awaited_once_with(
            router.owner_of(remote_key), remote_key, "some-value"
        )

        await router.close()
        await storage.close()


@pytest.mark.asyncio
async def test_unreachable_peer_raises_runtime_error(monkeypatch):
    """
    When the HTTP forward call fails (peer down) and there is NO local
    replica to fall back to, ClusterRouter must raise RuntimeError so
    main.py can convert it to HTTP 503.

    rf=1 means the replication set is exactly [primary], so when that
    forward fails there is nothing else to try.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        peers = ["http://localhost:8000", "http://localhost:8001"]
        # rf=1: no replica fallback, the remote primary is the only candidate
        router, storage = await make_router(
            tmpdir, "node-0", peers, monkeypatch, replication_factor=1
        )

        remote_key = None
        for i in range(500):
            k = f"probe:{i}"
            if router.owner_of(k) != "node-0":
                remote_key = k
                break

        assert remote_key is not None

        router._forward_get = AsyncMock(
            side_effect=RuntimeError("Peer node-1 unreachable")
        )

        with pytest.raises(RuntimeError, match="unreachable"):
            await router.get(remote_key)

        await router.close()
        await storage.close()

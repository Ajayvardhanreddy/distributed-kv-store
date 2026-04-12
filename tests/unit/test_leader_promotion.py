"""
Unit tests for Phase 6-A: Leader Promotion for Writes

Covers:
  - write_leader_of() returns primary when healthy
  - write_leader_of() promotes first healthy replica when primary is down
  - write_leader_of() returns None when all nodes are down
  - put() routes to healthy replica when primary marked down
  - put() raises when all nodes down
  - delete() uses promoted leader for existence check
  - Unit tests for sync_from_peers() logic via storage.snapshot()
"""
import os
import tempfile
from unittest.mock import AsyncMock

import pytest

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.health_checker import HealthChecker
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.storage.engine import StorageEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(node_id: str, peers: list[str], monkeypatch, rf: int = 2) -> NodeConfig:
    monkeypatch.setenv("NODE_ID", node_id)
    monkeypatch.setenv("PEERS", ",".join(peers))
    monkeypatch.setenv("REPLICATION_FACTOR", str(rf))
    return NodeConfig()


async def make_router(
    tmpdir: str,
    cfg: NodeConfig,
    hc: HealthChecker,
) -> tuple[ClusterRouter, StorageEngine]:
    ring = ConsistentHashRing(num_vnodes=50)
    for nid in cfg.node_ids:
        ring.add_node(nid)

    storage = StorageEngine(os.path.join(tmpdir, f"{cfg.node_id}.wal"))
    await storage.initialize()

    router = ClusterRouter(cfg, storage, ring, health=hc)
    await router.initialize()
    return router, storage


# ---------------------------------------------------------------------------
# _write_leader / write_leader_of
# ---------------------------------------------------------------------------

def test_write_leader_is_primary_when_all_healthy(monkeypatch):
    """When all nodes are healthy, write-leader == ring primary."""
    peers = ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"]
    cfg = make_config("node-0", peers, monkeypatch, rf=2)
    hc = HealthChecker(cfg)   # all healthy by default

    ring = ConsistentHashRing(num_vnodes=50)
    for nid in cfg.node_ids:
        ring.add_node(nid)

    # Borrow _write_leader directly without a full router
    from app.cluster.router import ClusterRouter
    router = ClusterRouter.__new__(ClusterRouter)
    router.config = cfg
    router.ring = ring
    router.replication_factor = 2
    router.health = hc

    for i in range(100):
        key = f"test:{i}"
        primary = ring.get_node(key)
        leader = router.write_leader_of(key)
        assert leader == primary, f"Expected {primary} but got {leader} for key {key}"


def test_write_leader_promotes_replica_when_primary_down(monkeypatch):
    """When primary is down, write-leader = first healthy replica."""
    peers = ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"]
    cfg = make_config("node-0", peers, monkeypatch, rf=2)
    hc = HealthChecker(cfg)

    ring = ConsistentHashRing(num_vnodes=50)
    for nid in cfg.node_ids:
        ring.add_node(nid)

    from app.cluster.router import ClusterRouter
    router = ClusterRouter.__new__(ClusterRouter)
    router.config = cfg
    router.ring = ring
    router.replication_factor = 2
    router.health = hc

    # Find a key where node-1 is primary, then mark node-1 down
    key = next(k for i in range(500) if (k := f"k:{i}") and ring.get_node(k) == "node-1")
    hc.mark_down("node-1")

    replication_set = ring.get_nodes(key, n=2)
    expected_leader = next(n for n in replication_set if n != "node-1")

    leader = router.write_leader_of(key)
    assert leader == expected_leader, f"Expected {expected_leader}, got {leader}"


def test_write_leader_none_when_all_down(monkeypatch):
    """None returned when all nodes in the replication set are down."""
    peers = ["http://localhost:8000", "http://localhost:8001"]
    cfg = make_config("node-0", peers, monkeypatch, rf=2)
    hc = HealthChecker(cfg)
    hc.mark_down("node-1")
    # node-0 is local and always healthy — create a scenario with rf=1
    # so only node-1 is in the set
    cfg2 = make_config("node-0", peers, monkeypatch, rf=1)
    hc2 = HealthChecker(cfg2)
    hc2.mark_down("node-1")

    ring = ConsistentHashRing(num_vnodes=50)
    for nid in cfg2.node_ids:
        ring.add_node(nid)

    from app.cluster.router import ClusterRouter
    router = ClusterRouter.__new__(ClusterRouter)
    router.config = cfg2
    router.ring = ring
    router.replication_factor = 1
    router.health = hc2

    # Find a key whose only owner is node-1
    key = next(k for i in range(500) if (k := f"k:{i}") and ring.get_node(k) == "node-1")
    assert router.write_leader_of(key) is None


# ---------------------------------------------------------------------------
# ClusterRouter.put() with leader promotion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_routes_to_promoted_leader(monkeypatch):
    """
    When primary is marked down, put() must call _forward_put on the
    first healthy replica instead (leader promotion).
    """
    peers = ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"]
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config("node-0", peers, monkeypatch, rf=2)
        hc = HealthChecker(cfg)
        router, storage = await make_router(tmp, cfg, hc)

        # Find a key where both primary and replica are remote
        key = next(
            k for i in range(500)
            if (k := f"leader:{i}")
            and all(not cfg.is_local(n) for n in router.replicas_of(k))
        )
        primary = router.owner_of(key)
        replication_set = router.replicas_of(key)
        replica = next(n for n in replication_set if n != primary)

        # Mark primary down — replica should be promoted as write target
        hc.mark_down(primary)
        router._forward_put = AsyncMock(return_value=1)

        await router.put(key, "value")

        # _forward_put must have been called with the REPLICA, not the primary
        called_nodes = [call.args[0] for call in router._forward_put.call_args_list]
        assert primary not in called_nodes, "Primary was called despite being marked down"
        assert replica in called_nodes, "Replica was not promoted as write target"

        await router.close(); await storage.close()


@pytest.mark.asyncio
async def test_put_raises_when_all_nodes_down(monkeypatch):
    """put() raises RuntimeError when every node in the replication set is down."""
    peers = ["http://localhost:8000", "http://localhost:8001"]
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config("node-0", peers, monkeypatch, rf=1)
        hc = HealthChecker(cfg)
        router, storage = await make_router(tmp, cfg, hc)

        # Find a remote-only key
        key = next(k for i in range(500) if (k := f"down:{i}") and router.owner_of(k) == "node-1")
        hc.mark_down("node-1")

        with pytest.raises(RuntimeError, match="No healthy node"):
            await router.put(key, "value")

        await router.close(); await storage.close()


# ---------------------------------------------------------------------------
# StorageEngine.snapshot() — used by /internal/sync
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_snapshot_returns_full_store():
    """snapshot() returns all stored key-value pairs."""
    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageEngine(os.path.join(tmp, "snap.wal"))
        await storage.initialize()
        await storage.put("a", "1")
        await storage.put("b", "2")
        snap = await storage.snapshot()
        assert snap == {
            "a": {"value": "1", "version": 1},
            "b": {"value": "2", "version": 1},
        }
        await storage.close()


@pytest.mark.asyncio
async def test_snapshot_is_a_copy():
    """Mutating the snapshot dict does not affect the live store."""
    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageEngine(os.path.join(tmp, "snap2.wal"))
        await storage.initialize()
        await storage.put("key", "original")
        snap = await storage.snapshot()
        snap["key"] = "mutated"   # modify the copy
        value, version = await storage.get("key")
        assert value == "original"   # store unchanged
        await storage.close()


@pytest.mark.asyncio
async def test_snapshot_empty_store():
    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageEngine(os.path.join(tmp, "snap3.wal"))
        await storage.initialize()
        assert await storage.snapshot() == {}
        await storage.close()

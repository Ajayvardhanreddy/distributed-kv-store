"""
Unit tests for Phase 5: HealthChecker and replica read fallback.

Tests cover:
  HealthChecker:
    - optimistic default (all peers healthy)
    - mark_down() makes peer unhealthy
    - background loop restores peer (mocked HTTP)
    - local node always reports healthy

  ClusterRouter.get() with replica fallback:
    - reads from primary when healthy
    - falls back to replica when primary is marked down
    - returns 503 (RuntimeError) when ALL replicas down
    - immediately marks node down on network error during read
"""
import asyncio
import os
import tempfile
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.health_checker import HealthChecker
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.storage.engine import StorageEngine


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def make_config(node_id: str, peers: list[str], monkeypatch, rf: int = 2) -> NodeConfig:
    monkeypatch.setenv("NODE_ID", node_id)
    monkeypatch.setenv("PEERS", ",".join(peers))
    monkeypatch.setenv("REPLICATION_FACTOR", str(rf))
    return NodeConfig()


async def make_router(
    tmpdir: str,
    cfg: NodeConfig,
    hc: Optional[HealthChecker] = None,
) -> tuple[ClusterRouter, StorageEngine]:
    ring = ConsistentHashRing(num_vnodes=50)
    for nid in cfg.node_ids:
        ring.add_node(nid)

    storage = StorageEngine(os.path.join(tmpdir, f"{cfg.node_id}.wal"))
    await storage.initialize()

    router = ClusterRouter(cfg, storage, ring, health=hc)
    await router.initialize()
    return router, storage


# -----------------------------------------------------------------------
# HealthChecker — unit tests
# -----------------------------------------------------------------------

def test_health_checker_default_all_healthy(monkeypatch):
    """All peers start as healthy (optimistic default)."""
    cfg = make_config("node-0", ["http://localhost:8000", "http://localhost:8001"], monkeypatch)
    hc = HealthChecker(cfg)
    assert hc.is_healthy("node-0") is True
    assert hc.is_healthy("node-1") is True


def test_health_checker_local_node_always_healthy(monkeypatch):
    """This node itself is always healthy."""
    cfg = make_config("node-0", ["http://localhost:8000", "http://localhost:8001"], monkeypatch)
    hc = HealthChecker(cfg)
    hc.mark_down("node-0")   # even if someone marks it down
    assert hc.is_healthy("node-0") is True   # local node ignores mark_down


def test_health_checker_mark_down(monkeypatch):
    """mark_down() makes a peer unhealthy."""
    cfg = make_config("node-0", ["http://localhost:8000", "http://localhost:8001"], monkeypatch)
    hc = HealthChecker(cfg)
    assert hc.is_healthy("node-1") is True
    hc.mark_down("node-1")
    assert hc.is_healthy("node-1") is False


def test_health_checker_get_status(monkeypatch):
    """get_status() returns a full copy of the health map."""
    cfg = make_config(
        "node-0",
        ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"],
        monkeypatch,
    )
    hc = HealthChecker(cfg)
    hc.mark_down("node-2")
    status = hc.get_status()
    assert status["node-0"] is True
    assert status["node-1"] is True
    assert status["node-2"] is False


@pytest.mark.asyncio
async def test_health_checker_bg_loop_restores_peer(monkeypatch):
    """
    After mark_down(), a successful ping in the background loop
    should restore the peer to healthy.
    """
    cfg = make_config("node-0", ["http://localhost:8000", "http://localhost:8001"], monkeypatch)
    hc = HealthChecker(cfg, check_interval=0.05)  # very short for test speed

    # Make pings succeed
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()

    with patch("app.cluster.health_checker.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.aclose = AsyncMock()
        mock_cls.return_value = mock_client

        await hc.start()
        hc.mark_down("node-1")
        assert hc.is_healthy("node-1") is False

        # Wait a couple of check intervals
        await asyncio.sleep(0.15)
        assert hc.is_healthy("node-1") is True   # restored by background loop

        await hc.stop()


@pytest.mark.asyncio
async def test_health_checker_bg_loop_detects_down(monkeypatch):
    """Background loop marks a peer as down when ping fails."""
    cfg = make_config("node-0", ["http://localhost:8000", "http://localhost:8001"], monkeypatch)
    hc = HealthChecker(cfg, check_interval=0.05)

    import httpx as _httpx
    with patch("app.cluster.health_checker.httpx.AsyncClient") as mock_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=_httpx.RequestError("timeout"))
        mock_client.aclose = AsyncMock()
        mock_cls.return_value = mock_client

        await hc.start()
        await asyncio.sleep(0.15)
        assert hc.is_healthy("node-1") is False   # loop detected failure

        await hc.stop()


# -----------------------------------------------------------------------
# ClusterRouter.get() — replica fallback
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_reads_primary_when_healthy(monkeypatch):
    """Normal case: primary is healthy, reads from primary."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config("node-0", ["http://localhost:8000"], monkeypatch, rf=1)
        hc = HealthChecker(cfg)
        router, storage = await make_router(tmp, cfg, hc)

        await storage.put("key", "value")
        result = await router.get("key")
        assert result == "value"

        await router.close(); await storage.close()


@pytest.mark.asyncio
async def test_get_falls_back_to_replica_when_primary_down(monkeypatch):
    """
    When the primary is marked down and _forward_get would fail,
    get() should retry the replica and return its value.
    """
    peers = ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"]
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config("node-0", peers, monkeypatch, rf=2)
        hc = HealthChecker(cfg)
        router, storage = await make_router(tmp, cfg, hc)

        # Find a key where node-0 is NOT the primary
        remote_key = next(
            k for i in range(500)
            if (k := f"probe:{i}") and router.owner_of(k) != "node-0"
        )

        primary = router.owner_of(remote_key)
        replica_set = router.replicas_of(remote_key)
        replica = next(n for n in replica_set if n != primary)

        # Mark primary as down
        hc.mark_down(primary)

        # _forward_get to replica returns a value
        router._forward_get = AsyncMock(return_value="fallback-value")

        result = await router.get(remote_key)
        assert result == "fallback-value"
        # Was called with the REPLICA, not the (skipped) primary
        router._forward_get.assert_awaited_once_with(replica, remote_key)

        await router.close(); await storage.close()


@pytest.mark.asyncio
async def test_get_raises_when_all_replicas_down(monkeypatch):
    """When every replica is down, get() raises RuntimeError."""
    peers = ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"]
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config("node-0", peers, monkeypatch, rf=2)
        hc = HealthChecker(cfg)
        router, storage = await make_router(tmp, cfg, hc)

        # Find a key where node-0 is NOT in the replication set (fully remote)
        fully_remote_key = None
        for i in range(500):
            k = f"alldown:{i}"
            rset = router.replicas_of(k)
            if all(not cfg.is_local(n) for n in rset):
                fully_remote_key = k
                break
        assert fully_remote_key, "Could not find a fully-remote key"

        # Mark all nodes in the replication set as down
        for n in router.replicas_of(fully_remote_key):
            hc.mark_down(n)

        with pytest.raises(RuntimeError, match="All replicas unreachable"):
            await router.get(fully_remote_key)

        await router.close(); await storage.close()


@pytest.mark.asyncio
async def test_get_marks_down_immediately_on_network_error(monkeypatch):
    """
    If _forward_get raises RuntimeError (network error), get() should
    call health.mark_down() on that node immediately.

    We use rf=1 so node-1 is the only candidate — no local fallback.
    When it fails the router marks it down then exhausts all candidates
    and raises RuntimeError.
    """
    peers = ["http://localhost:8000", "http://localhost:8001"]
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config("node-0", peers, monkeypatch, rf=1)
        hc = HealthChecker(cfg)
        router, storage = await make_router(tmp, cfg, hc)

        # Find a key whose sole owner is node-1 (rf=1 → single candidate)
        remote_key = next(
            k for i in range(500)
            if (k := f"err:{i}") and router.owner_of(k) == "node-1"
        )

        # Make the remote forward fail
        router._forward_get = AsyncMock(side_effect=RuntimeError("unreachable"))

        with pytest.raises(RuntimeError):
            await router.get(remote_key)

        # node-1 must be marked down after the error
        assert hc.is_healthy("node-1") is False

        await router.close(); await storage.close()


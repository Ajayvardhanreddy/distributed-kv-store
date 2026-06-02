"""
Unit tests for Phase 3 — TTL lazy + active expiration.

Spec contract:
  - PUT accepts optional ttl_seconds; engine stores expires_at (epoch seconds float)
  - GET returns miss for expired keys WITHOUT writing a tombstone (lazy, side-effect-free)
  - exists() / size() treat expired keys as misses
  - Active sweeper samples keys, finds expired ones, writes tombstone via Phase 2 delete()
  - expires_at travels with value in replication fan-out (replicas expire identically)
  - WAL persists expires_at and restores it on replay
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import time
from unittest.mock import AsyncMock, patch

import pytest

from app.storage.engine import StorageEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _engine(tmp: str, **kw) -> StorageEngine:
    e = StorageEngine(os.path.join(tmp, "t.wal"), **kw)
    await e.initialize()
    return e


def _past(seconds: float = 5.0) -> float:
    return time.time() - seconds


def _future(seconds: float = 3600.0) -> float:
    return time.time() + seconds


# ---------------------------------------------------------------------------
# PUT with TTL — engine level
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_with_expires_at_stores_field():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        expires = _future()
        await engine.put("k", "v", expires_at=expires)
        entry = engine.store["k"]
        assert entry["expires_at"] == pytest.approx(expires, abs=1.0)
        await engine.close()


@pytest.mark.asyncio
async def test_put_without_ttl_has_no_expiry():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v")
        entry = engine.store["k"]
        assert entry.get("expires_at") is None
        await engine.close()


# ---------------------------------------------------------------------------
# Lazy expiry — GET, exists, size
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_returns_miss_for_expired_key():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_past())

        value, version = await engine.get("k")
        assert value is None
        assert version == 0
        await engine.close()


@pytest.mark.asyncio
async def test_get_returns_value_for_unexpired_key():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_future())
        value, version = await engine.get("k")
        assert value == "v"
        assert version == 1
        await engine.close()


@pytest.mark.asyncio
async def test_get_returns_value_for_key_with_no_ttl():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v")          # no TTL
        value, version = await engine.get("k")
        assert value == "v"
        await engine.close()


@pytest.mark.asyncio
async def test_exists_returns_false_for_expired():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_past())
        assert await engine.exists("k") is False
        await engine.close()


@pytest.mark.asyncio
async def test_size_excludes_expired_keys():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("live", "yes")
        await engine.put("dead", "no", expires_at=_past())
        assert await engine.size() == 1
        await engine.close()


@pytest.mark.asyncio
async def test_lazy_get_does_not_write_tombstone():
    """GET on an expired key is side-effect-free — no tombstone written."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_past())

        await engine.get("k")   # expired — should NOT tombstone

        entry = engine.store["k"]
        assert entry.get("deleted") is not True, \
            "Lazy GET must not write a tombstone"
        await engine.close()


# ---------------------------------------------------------------------------
# WAL persistence of expires_at
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_expires_at_survives_wal_replay():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "t.wal")
        expires = _future(600)

        engine = StorageEngine(path)
        await engine.initialize()
        await engine.put("k", "v", expires_at=expires)
        await engine.close()

        engine2 = StorageEngine(path)
        await engine2.initialize()
        entry = engine2.store["k"]
        assert entry["expires_at"] == pytest.approx(expires, abs=1.0)
        # Key is not yet expired — still readable
        value, _ = await engine2.get("k")
        assert value == "v"
        await engine2.close()


@pytest.mark.asyncio
async def test_expired_key_still_miss_after_wal_replay():
    """expires_at is replayed correctly — expired key stays expired after restart."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "t.wal")

        engine = StorageEngine(path)
        await engine.initialize()
        await engine.put("k", "v", expires_at=_past(10))
        await engine.close()

        engine2 = StorageEngine(path)
        await engine2.initialize()
        value, _ = await engine2.get("k")
        assert value is None   # expired → miss
        await engine2.close()


# ---------------------------------------------------------------------------
# put_versioned with expires_at (replication fan-out path)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_versioned_carries_expires_at():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        expires = _future(300)
        await engine.put_versioned("k", "v", version=5, expires_at=expires)
        entry = engine.store["k"]
        assert entry["expires_at"] == pytest.approx(expires, abs=1.0)
        assert entry["version"] == 5
        await engine.close()


@pytest.mark.asyncio
async def test_replicas_expire_identically():
    """Replica receives expires_at from leader — does NOT recompute from ttl_seconds."""
    with tempfile.TemporaryDirectory() as tmp:
        leader = await _engine(tmp)
        replica_dir = tempfile.mkdtemp()
        replica = await _engine(replica_dir)

        leader_expires = _future(60)
        await leader.put("k", "v", expires_at=leader_expires)

        # Simulate fan-out: replica gets exactly the same expires_at
        leader_entry = leader.store["k"]
        await replica.put_versioned(
            "k", leader_entry["value"],
            version=leader_entry["version"],
            expires_at=leader_entry["expires_at"],
        )

        replica_entry = replica.store["k"]
        assert replica_entry["expires_at"] == pytest.approx(leader_expires, abs=0.1)

        await leader.close()
        await replica.close()
        import shutil; shutil.rmtree(replica_dir)


# ---------------------------------------------------------------------------
# Active sweeper — get_expired_keys sampling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_expired_keys_returns_expired():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("alive", "yes")
        await engine.put("expired1", "x", expires_at=_past())
        await engine.put("expired2", "y", expires_at=_past())

        expired = await engine.get_expired_keys(sample_size=1000)
        assert set(expired) == {"expired1", "expired2"}
        await engine.close()


@pytest.mark.asyncio
async def test_get_expired_keys_ignores_tombstones():
    """Tombstoned keys are already handled — sweeper must not re-process them."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_past())
        await engine.delete("k")   # already tombstoned

        expired = await engine.get_expired_keys(sample_size=1000)
        assert "k" not in expired
        await engine.close()


@pytest.mark.asyncio
async def test_get_expired_keys_ignores_unexpired():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_future())
        expired = await engine.get_expired_keys(sample_size=1000)
        assert expired == []
        await engine.close()


@pytest.mark.asyncio
async def test_get_expired_keys_respects_sample_size():
    """Never returns more than sample_size keys."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        for i in range(50):
            await engine.put(f"k{i}", "v", expires_at=_past())

        expired = await engine.get_expired_keys(sample_size=10)
        assert len(expired) <= 10
        await engine.close()


# ---------------------------------------------------------------------------
# Sweeper tombstones expired keys (integration with Phase 2 delete contract)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sweeper_delete_produces_tombstone():
    """
    Sweeper calls engine.delete() on an expired key.
    That must produce a tombstone (Phase 2 contract), not a physical removal.
    """
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_past())

        # Sweeper logic: find expired, delete each
        expired = await engine.get_expired_keys(sample_size=1000)
        assert "k" in expired

        result = await engine.delete("k")
        assert result is True

        # Must be a tombstone, not absent
        entry = engine.store.get("k")
        assert entry is not None
        assert entry["deleted"] is True
        await engine.close()


@pytest.mark.asyncio
async def test_sweeper_does_not_touch_unexpired_keys():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _engine(tmp)
        await engine.put("k", "v", expires_at=_future())
        expired = await engine.get_expired_keys(sample_size=1000)
        assert expired == []
        await engine.close()


# ---------------------------------------------------------------------------
# Metrics counters exist and are importable
# ---------------------------------------------------------------------------

def test_sweeper_metrics_exist():
    from app.storage import engine as eng_module
    assert hasattr(eng_module, "_keys_expired_total")
    assert hasattr(eng_module, "_sweeper_runs_total")

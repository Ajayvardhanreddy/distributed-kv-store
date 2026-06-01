"""
Unit tests for Phase 2 — Tombstone DELETE model.

All DELETE paths write tombstones; physical removal only happens via compaction
after tombstone_expires_at. Tests cover:
  - Tombstone structure and WAL persistence
  - GET / exists / size treat tombstones as misses
  - snapshot() includes tombstones for anti-entropy
  - Compaction removes only expired tombstones
  - Resurrection prevention: peer tombstone beats local live value on rejoin
  - PUT after DELETE creates a fresh live entry (reuse key)
"""
import os
import time
import tempfile

import pytest

from app.storage.engine import StorageEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_engine(tmp: str, retention: float = 3600.0) -> StorageEngine:
    path = os.path.join(tmp, "t.wal")
    engine = StorageEngine(path, tombstone_retention_seconds=retention)
    await engine.initialize()
    return engine


# ---------------------------------------------------------------------------
# Tombstone basics
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_produces_tombstone_in_store():
    """After delete() the key is still in the store, marked deleted."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("k", "v")
        deleted = await engine.delete("k")
        assert deleted is True

        entry = engine.store.get("k")
        assert entry is not None
        assert entry["deleted"] is True
        assert "tombstone_expires_at" in entry
        assert entry["version"] == 2   # version incremented on delete

        await engine.close()


@pytest.mark.asyncio
async def test_get_on_tombstone_returns_miss():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("k", "v")
        await engine.delete("k")

        value, version = await engine.get("k")
        assert value is None
        assert version == 0
        await engine.close()


@pytest.mark.asyncio
async def test_exists_on_tombstone_returns_false():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("k", "v")
        await engine.delete("k")
        assert await engine.exists("k") is False
        await engine.close()


@pytest.mark.asyncio
async def test_size_excludes_tombstones():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("a", "1")
        await engine.put("b", "2")
        assert await engine.size() == 2

        await engine.delete("a")
        assert await engine.size() == 1   # tombstone not counted
        await engine.close()


@pytest.mark.asyncio
async def test_delete_nonexistent_returns_false():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        result = await engine.delete("never-existed")
        assert result is False
        await engine.close()


@pytest.mark.asyncio
async def test_delete_already_tombstoned_returns_false():
    """Double-delete is a no-op."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("k", "v")
        assert await engine.delete("k") is True
        assert await engine.delete("k") is False
        await engine.close()


# ---------------------------------------------------------------------------
# snapshot() includes tombstones (anti-entropy requirement)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_snapshot_includes_tombstones():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("live", "yes")
        await engine.put("dead", "no")
        await engine.delete("dead")

        snap = await engine.snapshot()
        assert "live" in snap
        assert snap["live"]["deleted"] is False
        assert "dead" in snap
        assert snap["dead"]["deleted"] is True
        assert "tombstone_expires_at" in snap["dead"]
        await engine.close()


# ---------------------------------------------------------------------------
# WAL persistence of tombstones
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tombstone_survives_wal_replay():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "t.wal")

        engine = StorageEngine(path)
        await engine.initialize()
        await engine.put("k", "v")
        await engine.delete("k")
        await engine.close()

        # New engine replaying WAL
        engine2 = StorageEngine(path)
        await engine2.initialize()
        value, version = await engine2.get("k")
        assert value is None       # tombstone → miss
        assert version == 0
        assert engine2.store["k"]["deleted"] is True
        await engine2.close()


@pytest.mark.asyncio
async def test_tombstone_version_increments_on_delete():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        v1 = await engine.put("k", "a")   # version 1
        v2 = await engine.put("k", "b")   # version 2
        await engine.delete("k")          # tombstone at version 3

        entry = engine.store["k"]
        assert entry["version"] == 3
        await engine.close()


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compaction_removes_expired_tombstone():
    with tempfile.TemporaryDirectory() as tmp:
        # retention = 0 → immediately expired
        engine = await _make_engine(tmp, retention=0.0)
        await engine.put("k", "v")
        await engine.delete("k")

        removed = await engine.compact()
        assert removed == 1
        assert "k" not in engine.store   # physically gone after compaction
        await engine.close()


@pytest.mark.asyncio
async def test_compaction_keeps_unexpired_tombstone():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp, retention=3600.0)
        await engine.put("k", "v")
        await engine.delete("k")

        removed = await engine.compact()
        assert removed == 0
        assert "k" in engine.store       # tombstone still present
        await engine.close()


@pytest.mark.asyncio
async def test_compaction_only_touches_tombstones():
    """Live keys are never removed by compaction."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp, retention=0.0)
        await engine.put("live", "yes")
        await engine.put("dead", "no")
        await engine.delete("dead")

        removed = await engine.compact()
        assert removed == 1
        assert "live" in engine.store
        assert engine.store["live"].get("deleted") is not True
        await engine.close()


# ---------------------------------------------------------------------------
# put_tombstone() — sync-on-rejoin path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_tombstone_applies_peer_tombstone():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        expires = time.time() + 3600
        await engine.put_tombstone("k", version=5, tombstone_expires_at=expires)

        entry = engine.store["k"]
        assert entry["deleted"] is True
        assert entry["version"] == 5
        await engine.close()


@pytest.mark.asyncio
async def test_put_tombstone_survives_wal_replay():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "t.wal")
        engine = StorageEngine(path)
        await engine.initialize()
        expires = time.time() + 3600
        await engine.put_tombstone("k", version=7, tombstone_expires_at=expires)
        await engine.close()

        engine2 = StorageEngine(path)
        await engine2.initialize()
        assert engine2.store["k"]["deleted"] is True
        assert engine2.store["k"]["version"] == 7
        await engine2.close()


# ---------------------------------------------------------------------------
# Resurrection prevention (offline-rejoin scenario)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_peer_tombstone_wins_over_stale_local_value():
    """
    Simulates offline-rejoin resurrection scenario:
      - Node was offline when a DELETE happened
      - Node's WAL has old live value at version 1
      - Peer snapshot has tombstone at version 2
      - After sync, tombstone must win (key stays deleted)
    """
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("k", "old-value")   # local: v=1

        # Peer snapshot has tombstone at higher version
        peer_snapshot = {
            "k": {"deleted": True, "version": 2,
                  "tombstone_expires_at": time.time() + 3600}
        }

        # Simulate sync-on-rejoin merge
        for key, entry in peer_snapshot.items():
            peer_version = entry["version"]
            local_version = await engine.get_version(key)
            if peer_version > local_version:
                if entry.get("deleted"):
                    await engine.put_tombstone(
                        key, peer_version, entry["tombstone_expires_at"]
                    )
                else:
                    await engine.put_versioned(key, entry["value"], peer_version)

        # Key must be a tombstone, not the old live value
        value, ver = await engine.get("k")
        assert value is None, "Resurrected key must not be visible after tombstone sync"
        assert engine.store["k"]["deleted"] is True
        await engine.close()


@pytest.mark.asyncio
async def test_local_tombstone_beats_stale_peer_value():
    """
    If local tombstone has higher version than peer's live value,
    keep the tombstone — do NOT resurrect the key.
    """
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("k", "v")    # v=1
        await engine.delete("k")      # tombstone v=2

        peer_snapshot = {"k": {"value": "old", "version": 1, "deleted": False}}

        for key, entry in peer_snapshot.items():
            peer_version = entry["version"]
            local_version = await engine.get_version(key)
            if peer_version > local_version:
                await engine.put_versioned(key, entry["value"], peer_version)

        assert engine.store["k"]["deleted"] is True
        await engine.close()


# ---------------------------------------------------------------------------
# PUT after DELETE reuses key cleanly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_after_delete_creates_fresh_entry():
    with tempfile.TemporaryDirectory() as tmp:
        engine = await _make_engine(tmp)
        await engine.put("k", "original")   # v=1
        await engine.delete("k")            # tombstone v=2
        new_ver = await engine.put("k", "reborn")  # live again v=3

        assert new_ver == 3
        value, version = await engine.get("k")
        assert value == "reborn"
        assert version == 3
        assert engine.store["k"].get("deleted") is not True
        await engine.close()


@pytest.mark.asyncio
async def test_put_after_delete_survives_wal_replay():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "t.wal")
        engine = StorageEngine(path)
        await engine.initialize()
        await engine.put("k", "first")
        await engine.delete("k")
        await engine.put("k", "second")
        await engine.close()

        engine2 = StorageEngine(path)
        await engine2.initialize()
        value, version = await engine2.get("k")
        assert value == "second"
        assert version == 3
        await engine2.close()

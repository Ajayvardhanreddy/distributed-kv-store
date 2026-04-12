"""
Unit tests for Phase 8-A: Per-key monotonic version counter.

Tests cover:
  - Version starts at 1 for new keys
  - Version increments on each put
  - Version preserved across WAL replay
  - put_versioned() accepts explicit version (replica path)
  - snapshot() includes versions
  - Sync-on-rejoin: higher version wins
  - Relaxed WAL durability: writes buffered and flushed
"""
import os
import tempfile

import pytest

from app.storage.engine import StorageEngine
from app.storage.wal import WriteAheadLog


# -----------------------------------------------------------------------
# Version counter behaviour
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_version_starts_at_one():
    """First put for a new key yields version 1."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        version = await engine.put("k", "v")
        assert version == 1
        await engine.close()


@pytest.mark.asyncio
async def test_version_increments():
    """Each successive put bumps the version."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        v1 = await engine.put("k", "a")
        v2 = await engine.put("k", "b")
        v3 = await engine.put("k", "c")
        assert (v1, v2, v3) == (1, 2, 3)
        await engine.close()


@pytest.mark.asyncio
async def test_get_returns_version():
    """get() returns (value, version) tuple."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        await engine.put("k", "hello")
        await engine.put("k", "world")
        value, version = await engine.get("k")
        assert value == "world"
        assert version == 2
        await engine.close()


@pytest.mark.asyncio
async def test_get_nonexistent_returns_zero_version():
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        value, version = await engine.get("nope")
        assert value is None
        assert version == 0
        await engine.close()


# -----------------------------------------------------------------------
# WAL replay preserves version
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_version_survives_wal_replay():
    """After crash+restart, the WAL replay restores correct versions."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "v.wal")
        # Session 1: write keys
        engine = StorageEngine(path)
        await engine.initialize()
        await engine.put("a", "1")  # v=1
        await engine.put("a", "2")  # v=2
        await engine.put("b", "x")  # v=1
        await engine.close()

        # Session 2: new engine replaying the same WAL
        engine2 = StorageEngine(path)
        await engine2.initialize()
        val_a, ver_a = await engine2.get("a")
        val_b, ver_b = await engine2.get("b")
        assert val_a == "2" and ver_a == 2
        assert val_b == "x" and ver_b == 1
        await engine2.close()


# -----------------------------------------------------------------------
# put_versioned (replica path)
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_versioned_accepts_explicit_version():
    """put_versioned() stores the exact version provided (no auto-increment)."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        await engine.put_versioned("k", "val", version=42)
        value, version = await engine.get("k")
        assert value == "val"
        assert version == 42
        await engine.close()


@pytest.mark.asyncio
async def test_put_versioned_does_not_auto_increment():
    """After put_versioned(v=5), a normal put() should produce v=6."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        await engine.put_versioned("k", "a", version=5)
        next_version = await engine.put("k", "b")
        assert next_version == 6
        await engine.close()


# -----------------------------------------------------------------------
# Snapshot with versions
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_snapshot_includes_versions():
    """snapshot() returns {key: {value, version}}."""
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        await engine.put("x", "1")
        await engine.put("x", "2")
        await engine.put("y", "a")
        snap = await engine.snapshot()
        assert snap == {
            "x": {"value": "2", "version": 2},
            "y": {"value": "a", "version": 1},
        }
        await engine.close()


# -----------------------------------------------------------------------
# Version-aware merge (simulates sync-on-rejoin logic)
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_higher_version_wins_in_sync():
    """
    Simulates sync-on-rejoin: peer has version 3, we have version 1.
    After merge, version 3 should win.
    """
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        # Our local state
        await engine.put("k", "local")  # v=1

        # Peer snapshot (higher version wins)
        peer_snapshot = {"k": {"value": "peer", "version": 3}}
        for key, entry in peer_snapshot.items():
            _, local_ver = await engine.get(key)
            if entry["version"] > local_ver:
                await engine.put_versioned(key, entry["value"], entry["version"])

        val, ver = await engine.get("k")
        assert val == "peer"
        assert ver == 3
        await engine.close()


@pytest.mark.asyncio
async def test_lower_version_loses_in_sync():
    """
    Simulates sync-on-rejoin: peer has version 1, we have version 5.
    Our version should be kept.
    """
    with tempfile.TemporaryDirectory() as tmp:
        engine = StorageEngine(os.path.join(tmp, "v.wal"))
        await engine.initialize()
        await engine.put_versioned("k", "ours", version=5)

        peer_snapshot = {"k": {"value": "theirs", "version": 1}}
        for key, entry in peer_snapshot.items():
            _, local_ver = await engine.get(key)
            if entry["version"] > local_ver:
                await engine.put_versioned(key, entry["value"], entry["version"])

        val, ver = await engine.get("k")
        assert val == "ours"
        assert ver == 5
        await engine.close()


# -----------------------------------------------------------------------
# Relaxed-mode WAL
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_relaxed_wal_buffers_and_flushes():
    """In relaxed mode, writes buffer; after close() they persist on disk."""
    import asyncio
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "relaxed.wal")
        engine = StorageEngine(path, durability="relaxed")
        await engine.initialize()
        await engine.wal.start_flush_loop()

        for i in range(10):
            await engine.put(f"k{i}", f"v{i}")

        # Close triggers final flush
        await engine.close()

        # New strict engine replays the flushed WAL
        engine2 = StorageEngine(path)
        await engine2.initialize()
        for i in range(10):
            val, ver = await engine2.get(f"k{i}")
            assert val == f"v{i}"
        await engine2.close()

"""
Integration tests for data persistence.

Tests that data survives across storage engine restarts.
"""
import pytest
import tempfile
import os
from app.storage.engine import StorageEngine


@pytest.mark.asyncio
async def test_persistence_basic():
    """Test that data persists across engine restarts"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # First session: write data
        engine1 = StorageEngine(wal_path)
        await engine1.initialize()
        await engine1.put("persistent", "value")
        await engine1.close()
        
        # Second session: data should persist
        engine2 = StorageEngine(wal_path)
        await engine2.initialize()
        value = await engine2.get("persistent")
        assert value == "value"
        await engine2.close()


@pytest.mark.asyncio
async def test_persistence_multiple_operations():
    """Test persistence with multiple operations"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # First session: multiple operations
        engine1 = StorageEngine(wal_path)
        await engine1.initialize()
        await engine1.put("key1", "value1")
        await engine1.put("key2", "value2")
        await engine1.put("key3", "value3")
        await engine1.delete("key2")
        await engine1.close()
        
        # Second session: verify state
        engine2 = StorageEngine(wal_path)
        await engine2.initialize()
        
        assert await engine2.get("key1") == "value1"
        assert await engine2.get("key2") is None  # Was deleted
        assert await engine2.get("key3") == "value3"
        assert await engine2.size() == 2
        
        await engine2.close()


@pytest.mark.asyncio
async def test_persistence_updates():
    """Test that updates persist correctly"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # First session: write and update
        engine1 = StorageEngine(wal_path)
        await engine1.initialize()
        await engine1.put("key", "value1")
        await engine1.put("key", "value2")
        await engine1.put("key", "value3")
        await engine1.close()
        
        # Second session: should have final value
        engine2 = StorageEngine(wal_path)
        await engine2.initialize()
        assert await engine2.get("key") == "value3"
        await engine2.close()


@pytest.mark.asyncio
async def test_persistence_accumulation():
    """Test that WAL accumulates across multiple sessions"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # Session 1: add key1
        engine1 = StorageEngine(wal_path)
        await engine1.initialize()
        await engine1.put("key1", "value1")
        await engine1.close()
        
        # Session 2: add key2
        engine2 = StorageEngine(wal_path)
        await engine2.initialize()
        await engine2.put("key2", "value2")
        await engine2.close()
        
        # Session 3: both should exist
        engine3 = StorageEngine(wal_path)
        await engine3.initialize()
        assert await engine3.get("key1") == "value1"
        assert await engine3.get("key2") == "value2"
        assert await engine3.size() == 2
        await engine3.close()


@pytest.mark.asyncio
async def test_persistence_empty_after_deletes():
    """Test that deleting all keys results in empty store after restart"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # Session 1: add and delete
        engine1 = StorageEngine(wal_path)
        await engine1.initialize()
        await engine1.put("key1", "value1")
        await engine1.put("key2", "value2")
        await engine1.delete("key1")
        await engine1.delete("key2")
        await engine1.close()
        
        # Session 2: should be empty
        engine2 = StorageEngine(wal_path)
        await engine2.initialize()
        assert await engine2.size() == 0
        await engine2.close()

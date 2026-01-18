"""
Unit tests for StorageEngine.

Tests the core storage operations without WAL persistence
(WAL is tested separately).
"""
import pytest
import tempfile
import os
from app.storage.engine import StorageEngine


@pytest.mark.asyncio
async def test_storage_basic_put_get():
    """Test basic put and get operations"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        # Put a value
        await engine.put("key1", "value1")
        
        # Get it back
        value = await engine.get("key1")
        assert value == "value1"
        
        await engine.close()


@pytest.mark.asyncio
async def test_storage_get_nonexistent():
    """Test getting a nonexistent key returns None"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        value = await engine.get("does-not-exist")
        assert value is None
        
        await engine.close()


@pytest.mark.asyncio
async def test_storage_delete():
    """Test delete operation"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        # Put then delete
        await engine.put("key1", "value1")
        deleted = await engine.delete("key1")
        assert deleted is True
        
        # Verify it's gone
        value = await engine.get("key1")
        assert value is None
        
        await engine.close()


@pytest.mark.asyncio
async def test_storage_delete_nonexistent():
    """Test deleting nonexistent key returns False"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        deleted = await engine.delete("does-not-exist")
        assert deleted is False
        
        await engine.close()


@pytest.mark.asyncio
async def test_storage_exists():
    """Test exists operation"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        # Should not exist initially
        assert await engine.exists("key1") is False
        
        # Should exist after put
        await engine.put("key1", "value1")
        assert await engine.exists("key1") is True
        
        # Should not exist after delete
        await engine.delete("key1")
        assert await engine.exists("key1") is False
        
        await engine.close()


@pytest.mark.asyncio
async def test_storage_size():
    """Test size operation"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        # Initially empty
        assert await engine.size() == 0
        
        # Add keys
        await engine.put("key1", "value1")
        assert await engine.size() == 1
        
        await engine.put("key2", "value2")
        assert await engine.size() == 2
        
        # Delete one
        await engine.delete("key1")
        assert await engine.size() == 1
        
        await engine.close()


@pytest.mark.asyncio
async def test_storage_update():
    """Test updating an existing key"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        # Put initial value
        await engine.put("key1", "value1")
        assert await engine.get("key1") == "value1"
        
        # Update value
        await engine.put("key1", "value2")
        assert await engine.get("key1") == "value2"
        
        # Size should still be 1
        assert await engine.size() == 1
        
        await engine.close()


@pytest.mark.asyncio
async def test_storage_concurrent_access():
    """Test concurrent access with asyncio tasks"""
    import asyncio
    
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        engine = StorageEngine(wal_path)
        await engine.initialize()
        
        # Concurrent puts
        async def put_task(key: str, value: str):
            await engine.put(key, value)
        
        tasks = [put_task(f"key{i}", f"value{i}") for i in range(10)]
        await asyncio.gather(*tasks)
        
        # Verify all keys exist
        assert await engine.size() == 10
        for i in range(10):
            value = await engine.get(f"key{i}")
            assert value == f"value{i}"
        
        await engine.close()

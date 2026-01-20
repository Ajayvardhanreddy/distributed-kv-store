"""
Unit tests for ShardManager.

Tests the shard management and routing logic.
"""
import pytest
import tempfile
import os
from app.cluster.shard_manager import ShardManager


@pytest.mark.asyncio
async def test_shard_manager_initialization():
    """Test initializing shard manager"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shard_ids = ["shard-0", "shard-1", "shard-2"]
        manager = ShardManager(shard_ids, tmpdir)
        await manager.initialize()
        
        assert len(manager.shards) == 3
        assert await manager.size() == 0
        
        await manager.close()


@pytest.mark.asyncio
async def test_shard_manager_put_get():
    """Test basic put and get operations"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shard_ids = ["shard-0", "shard-1"]
        manager = ShardManager(shard_ids, tmpdir)
        await manager.initialize()
        
        # Put some keys
        await manager.put("key1", "value1")
        await manager.put("key2", "value2")
        
        # Get them back
        assert await manager.get("key1") == "value1"
        assert await manager.get("key2") == "value2"
        
        await manager.close()


@pytest.mark.asyncio
async def test_consistent_routing():
    """Test that same key always goes to same shard"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shard_ids = ["shard-0", "shard-1", "shard-2"]
        manager = ShardManager(shard_ids, tmpdir)
        await manager.initialize()
        
        # Put a key
        test_key = "user:123"
        await manager.put(test_key, "alice")
        
        # Determine which shard got it
        shard_for_key = manager.hash_ring.get_node(test_key)
        
        # Multiple operations on same key should all hit same shard
        await manager.put(test_key, "bob")
        assert manager.hash_ring.get_node(test_key) == shard_for_key
        
        value = await manager.get(test_key)
        assert value == "bob"
        assert manager.hash_ring.get_node(test_key) == shard_for_key
        
        await manager.close()


@pytest.mark.asyncio
async def test_keys_distributed_across_shards():
    """Test that keys are distributed across multiple shards"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shard_ids = ["shard-0", "shard-1", "shard-2"]
        manager = ShardManager(shard_ids, tmpdir)
        await manager.initialize()
        
        # Add 300 keys
        for i in range(300):
            await manager.put(f"key:{i}", f"value:{i}")
        
        # Check distribution
        stats = await manager.get_stats()
        assert stats["total_keys"] == 300
        
        # Each shard should have some keys (not all in one shard)
        for shard_id, count in stats["shards"].items():
            assert count > 0, f"{shard_id} has no keys"
            # With good distribution, each should have 80-120 keys (±20%)
            assert 60 < count < 140, f"{shard_id} has {count} keys (expected ~100)"
        
        await manager.close()


@pytest.mark.asyncio
async def test_delete_operation():
    """Test delete operation across shards"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shard_ids = ["shard-0", "shard-1"]
        manager = ShardManager(shard_ids, tmpdir)
        await manager.initialize()
        
        # Put and delete
        await manager.put("key1", "value1")
        assert await manager.delete("key1") is True
        assert await manager.get("key1") is None
        
        # Delete nonexistent
        assert await manager.delete("does-not-exist") is False
        
        await manager.close()


@pytest.mark.asyncio
async def test_get_stats():
    """Test stats endpoint"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shard_ids = ["shard-0", "shard-1", "shard-2"]
        manager = ShardManager(shard_ids, tmpdir)
        await manager.initialize()
        
        # Add some data
        for i in range(30):
            await manager.put(f"key:{i}", f"value:{i}")
        
        stats = await manager.get_stats()
        
        assert stats["total_keys"] == 30
        assert stats["num_shards"] == 3
        assert len(stats["shards"]) == 3
        assert "vnodes_per_shard" in stats
        
        await manager.close()


@pytest.mark.asyncio
async def test_wal_files_per_shard():
    """Test that each shard gets its own WAL file"""
    with tempfile.TemporaryDirectory() as tmpdir:
        shard_ids = ["shard-0", "shard-1", "shard-2"]
        manager = ShardManager(shard_ids, tmpdir)
        await manager.initialize()
        
        # Add data
        for i in range(30):
            await manager.put(f"key:{i}", f"value:{i}")
        
        await manager.close()
        
        # Check WAL files exist
        wal_files = os.listdir(tmpdir)
        assert "shard-0.wal" in wal_files
        assert "shard-1.wal" in wal_files
        assert "shard-2.wal" in wal_files

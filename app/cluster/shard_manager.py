"""
Shard Manager for distributing data across multiple storage engines.

The ShardManager coordinates multiple StorageEngine instances, using
consistent hashing to route each key to the appropriate shard.

From the API's perspective, ShardManager looks like a single StorageEngine,
but internally it manages N shards with separate WAL files.
"""
import os
from typing import Optional
import logging
from app.storage.engine import StorageEngine
from app.cluster.consistent_hash import ConsistentHashRing

logger = logging.getLogger(__name__)


class ShardManager:
    """
    Manages multiple storage shards with consistent hashing.
    
    Each shard is a separate StorageEngine with its own WAL file.
    Keys are distributed across shards using a consistent hash ring.
    
    Usage:
        manager = ShardManager(["shard-0", "shard-1", "shard-2"], "data")
        await manager.initialize()
        
        await manager.put("user:123", "alice")  # Routed to shard
        value = await manager.get("user:123")   # Same shard
        
        await manager.close()
    """
    
    def __init__(self, shard_ids: list[str], data_dir: str = "data", num_vnodes: int = 150):
        """
        Initialize shard manager.
        
        Args:
            shard_ids: List of shard identifiers (e.g., ["shard-0", "shard-1"])
            data_dir: Directory for WAL files
            num_vnodes: Virtual nodes per shard for hash ring
        """
        self.shard_ids = shard_ids
        self.data_dir = data_dir
        self.shards: dict[str, StorageEngine] = {}
        self.hash_ring = ConsistentHashRing(num_vnodes=num_vnodes)
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        logger.info(f"Initializing ShardManager with {len(shard_ids)} shards")
    
    async def initialize(self) -> None:
        """
        Initialize all shards and build the hash ring.
        
        Creates a StorageEngine for each shard and adds nodes to the hash ring.
        """
        # Create storage engine for each shard
        for shard_id in self.shard_ids:
            wal_path = os.path.join(self.data_dir, f"{shard_id}.wal")
            engine = StorageEngine(wal_path)
            await engine.initialize()
            self.shards[shard_id] = engine
            
            # Add to hash ring
            self.hash_ring.add_node(shard_id)
        
        # Log distribution
        distribution = self.hash_ring.get_distribution()
        logger.info(f"ShardManager initialized: {len(self.shards)} shards")
        logger.info(f"Virtual node distribution: {distribution}")
        
        # Count total keys
        total_keys = 0
        for shard in self.shards.values():
            total_keys += await shard.size()
        logger.info(f"Total keys across all shards: {total_keys}")
    
    def _get_shard(self, key: str) -> StorageEngine:
        """
        Get the storage engine responsible for a key.
        
        Uses consistent hashing to determine which shard owns the key.
        
        Args:
            key: The key to look up
            
        Returns:
            StorageEngine for the shard owning this key
            
        Raises:
            RuntimeError: If hash ring is empty (shouldn't happen after init)
        """
        shard_id = self.hash_ring.get_node(key)
        
        if shard_id is None:
            raise RuntimeError("Hash ring is empty - no shards available")
        
        return self.shards[shard_id]
    
    async def get(self, key: str) -> Optional[str]:
        """
        Get value for a key.
        
        Routes to the appropriate shard using consistent hashing.
        
        Args:
            key: Key to look up
            
        Returns:
            Value if found, None otherwise
        """
        shard = self._get_shard(key)
        return await shard.get(key)
    
    async def put(self, key: str, value: str) -> None:
        """
        Store a key-value pair.
        
        Routes to the appropriate shard using consistent hashing.
        
        Args:
            key: Key to store
            value: Value to store
        """
        shard = self._get_shard(key)
        await shard.put(key, value)
    
    async def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.
        
        Routes to the appropriate shard using consistent hashing.
        
        Args:
            key: Key to delete
            
        Returns:
            True if key existed and was deleted, False otherwise
        """
        shard = self._get_shard(key)
        return await shard.delete(key)
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists.
        
        Routes to the appropriate shard using consistent hashing.
        
        Args:
            key: Key to check
            
        Returns:
            True if key exists, False otherwise
        """
        shard = self._get_shard(key)
        return await shard.exists(key)
    
    async def size(self) -> int:
        """
        Get total number of keys across all shards.
        
        Returns:
            Total key count
        """
        total = 0
        for shard in self.shards.values():
            total += await shard.size()
        return total
    
    async def get_stats(self) -> dict:
        """
        Get statistics about shard distribution.
        
        Returns:
            Dictionary with:
            - total_keys: Total keys across all shards
            - num_shards: Number of shards
            - shards: Dict mapping shard_id → key count
            - vnodes: Virtual node distribution
        """
        shard_stats = {}
        total_keys = 0
        
        for shard_id, shard in self.shards.items():
            count = await shard.size()
            shard_stats[shard_id] = count
            total_keys += count
        
        return {
            "total_keys": total_keys,
            "num_shards": len(self.shards),
            "shards": shard_stats,
            "vnodes_per_shard": self.hash_ring.get_distribution()
        }
    
    async def close(self) -> None:
        """
        Close all shards gracefully.
        
        Ensures all WAL files are flushed properly.
        """
        for shard_id, shard in self.shards.items():
            await shard.close()
            logger.debug(f"Closed shard {shard_id}")
        
        logger.info("All shards closed")

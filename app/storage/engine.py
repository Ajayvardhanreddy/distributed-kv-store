"""
Storage Engine for in-memory key-value storage with WAL support.

The StorageEngine provides a clean async interface for CRUD operations
while ensuring durability through the Write-Ahead Log.

All mutations are logged before being applied to memory, ensuring
crash recovery is possible by replaying the WAL.
"""
from typing import Optional
import asyncio
import logging
from .wal import WriteAheadLog

logger = logging.getLogger(__name__)


class StorageEngine:
    """
    Thread-safe in-memory key-value storage with WAL-backed durability.
    
    Usage:
        engine = StorageEngine("data/node.wal")
        await engine.put("key", "value")
        value = await engine.get("key")
        await engine.delete("key")
        await engine.close()
    
    All operations are async and thread-safe through asyncio.Lock.
    State is recovered from WAL automatically on initialization.
    """
    
    def __init__(self, wal_path: str):
        """
        Initialize storage engine with WAL at specified path.
        
        Args:
            wal_path: Path to WAL file (e.g., "data/node.wal")
            
        The WAL is replayed synchronously during initialization to
        restore previous state before accepting new operations.
        """
        self.wal = WriteAheadLog(wal_path)
        self.lock = asyncio.Lock()
        self.store: dict[str, str] = {}
        self._closed = False
        
        logger.info(f"Initializing storage engine with WAL: {wal_path}")
    
    async def initialize(self) -> None:
        """
        Replay WAL to restore state.
        
        This should be called once after construction, typically in
        FastAPI's startup event.
        """
        logger.info("Replaying WAL to restore state...")
        self.store = await self.wal.replay()
        logger.info(f"Storage engine initialized with {len(self.store)} keys")
    
    async def get(self, key: str) -> Optional[str]:
        """
        Retrieve value for a key.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if key exists, None otherwise
        """
        async with self.lock:
            return self.store.get(key)
    
    async def put(self, key: str, value: str) -> None:
        """
        Store a key-value pair.
        
        The operation is logged to WAL before updating memory,
        ensuring durability even if the process crashes immediately after.
        
        Args:
            key: The key to store
            value: The value to store
        """
        async with self.lock:
            # Write to WAL first (durability)
            await self.wal.append("PUT", key, value)
            
            # Then update in-memory store
            self.store[key] = value
            
        logger.debug(f"PUT {key} (size: {len(value)} bytes)")
    
    async def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key existed and was deleted, False otherwise
        """
        async with self.lock:
            if key not in self.store:
                return False
            
            # Write to WAL first
            await self.wal.append("DELETE", key)
            
            # Then delete from memory
            del self.store[key]
            
        logger.debug(f"DELETE {key}")
        return True
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists.
        
        Args:
            key: The key to check
            
        Returns:
            True if key exists, False otherwise
        """
        async with self.lock:
            return key in self.store
    
    async def size(self) -> int:
        """
        Get the number of keys stored.
        
        Returns:
            Number of keys in the store
        """
        async with self.lock:
            return len(self.store)
    
    async def close(self) -> None:
        """
        Close the storage engine and WAL.
        
        Should be called during application shutdown to ensure
        all data is flushed properly.
        """
        if self._closed:
            return
        
        async with self.lock:
            await self.wal.close()
            self._closed = True
            
        logger.info("Storage engine closed")

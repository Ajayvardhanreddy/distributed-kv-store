"""
Storage Engine for in-memory key-value storage with WAL support.

The StorageEngine provides a clean async interface for CRUD operations
while ensuring durability through the Write-Ahead Log.

All mutations are logged before being applied to memory, ensuring
crash recovery is possible by replaying the WAL.

Each key carries a monotonic version counter that increments on every
write.  This allows:
  - Conflict detection during split-brain (compare versions)
  - Staleness detection on reads (client can see the version)
  - Smarter sync-on-rejoin (accept higher-version only)
"""
from typing import Optional
import asyncio
import logging
from .wal import WriteAheadLog

logger = logging.getLogger(__name__)


class StorageEngine:
    """
    Thread-safe in-memory key-value storage with WAL-backed durability
    and per-key monotonic versioning.

    Version semantics:
        - New key → version 1
        - Each subsequent put → version + 1
        - Delete removes the key and its version
        - put_versioned() accepts an explicit version (used by replica fan-out)

    Usage:
        engine = StorageEngine("data/node.wal")
        await engine.initialize()
        await engine.put("key", "value")       # version = 1
        val, ver = await engine.get("key")     # ("value", 1)
        await engine.put("key", "value2")      # version = 2
    """

    def __init__(self, wal_path: str, durability: str = "strict"):
        self.wal = WriteAheadLog(wal_path, durability=durability)
        self.lock = asyncio.Lock()
        # Internal store: key → {"value": str, "version": int}
        self.store: dict[str, dict] = {}
        self._closed = False
        logger.info(f"Initializing storage engine with WAL: {wal_path}")

    async def initialize(self) -> None:
        """
        Replay WAL to restore state (including versions).
        """
        logger.info("Replaying WAL to restore state...")
        self.store = await self.wal.replay()
        logger.info(f"Storage engine initialized with {len(self.store)} keys")

    async def get(self, key: str) -> tuple[Optional[str], int]:
        """
        Retrieve value and version for a key.

        Returns:
            (value, version) if key exists, (None, 0) otherwise
        """
        async with self.lock:
            entry = self.store.get(key)
            if entry is None:
                return None, 0
            return entry["value"], entry["version"]

    async def put(self, key: str, value: str) -> int:
        """
        Store a key-value pair, auto-incrementing the version.

        Returns:
            The new version number
        """
        async with self.lock:
            # Increment version (or start at 1 for new keys)
            current = self.store.get(key)
            new_version = (current["version"] + 1) if current else 1

            await self.wal.append("PUT", key, value, version=new_version)
            self.store[key] = {"value": value, "version": new_version}

        logger.debug(f"PUT {key} ver={new_version}")
        return new_version

    async def put_versioned(self, key: str, value: str, version: int) -> None:
        """
        Store a key-value pair with an explicit version.

        Used by replica fan-out: the write-leader decides the version,
        replicas accept it as-is.  Also used by sync-on-rejoin to
        accept a peer's version if it is higher than ours.
        """
        async with self.lock:
            await self.wal.append("PUT", key, value, version=version)
            self.store[key] = {"value": value, "version": version}
        logger.debug(f"PUT_VERSIONED {key} ver={version}")

    async def delete(self, key: str) -> bool:
        """
        Delete a key-value pair.

        Returns:
            True if key existed and was deleted, False otherwise
        """
        async with self.lock:
            if key not in self.store:
                return False
            await self.wal.append("DELETE", key, version=0)
            del self.store[key]

        logger.debug(f"DELETE {key}")
        return True

    async def exists(self, key: str) -> bool:
        async with self.lock:
            return key in self.store

    async def size(self) -> int:
        async with self.lock:
            return len(self.store)

    async def snapshot(self) -> dict[str, dict]:
        """
        Return a safe copy of all key-value pairs with versions.

        Used by /internal/sync so a rejoining node can pull current
        state and compare versions.

        Returns:
            {key: {"value": str, "version": int}, ...}
        """
        async with self.lock:
            return {k: dict(v) for k, v in self.store.items()}

    async def close(self) -> None:
        if self._closed:
            return
        async with self.lock:
            await self.wal.close()
            self._closed = True
        logger.info("Storage engine closed")


"""
Storage Engine — Phase 2: Tombstone DELETE model.

Store entry shapes
------------------
Live key   : {"value": str,  "version": int, "deleted": False}
Tombstone  : {"deleted": True, "version": int, "tombstone_expires_at": float}

DELETE contract
---------------
Every delete() writes a tombstone to WAL and memory.
Physical removal only happens via compact() after tombstone_expires_at.
GET / exists / size treat tombstones as misses — callers cannot distinguish
a tombstoned key from a key that never existed.

snapshot() includes tombstones so replicas and sync-on-rejoin can propagate
deletions and prevent resurrection.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from .wal import WriteAheadLog

logger = logging.getLogger(__name__)

# Default tombstone retention when not overridden (24 h)
_DEFAULT_RETENTION_SECONDS = 86_400.0


class StorageEngine:
    """
    Thread-safe in-memory key-value store with WAL-backed durability,
    per-key monotonic versioning, and tombstone-based deletes.
    """

    def __init__(
        self,
        wal_path: str,
        durability: str = "strict",
        tombstone_retention_seconds: float = _DEFAULT_RETENTION_SECONDS,
    ):
        self.wal = WriteAheadLog(wal_path, durability=durability)
        self.lock = asyncio.Lock()
        self.store: dict[str, dict] = {}
        self._tombstone_retention = tombstone_retention_seconds
        self._closed = False
        logger.info(f"StorageEngine init: wal={wal_path} durability={durability}")

    async def initialize(self) -> None:
        """Replay WAL to restore state (live keys + tombstones)."""
        logger.info("Replaying WAL…")
        self.store = await self.wal.replay()
        live = sum(1 for e in self.store.values() if not e.get("deleted"))
        tomb = len(self.store) - live
        logger.info(f"StorageEngine ready: {live} live keys, {tomb} tombstones")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    async def get(self, key: str) -> tuple[Optional[str], int]:
        """Return (value, version). Tombstones and missing keys both → (None, 0)."""
        async with self.lock:
            entry = self.store.get(key)
            if entry is None or entry.get("deleted"):
                return None, 0
            return entry["value"], entry["version"]

    async def get_version(self, key: str) -> int:
        """Return the current version for a key, including tombstones. 0 if absent."""
        async with self.lock:
            entry = self.store.get(key)
            return entry["version"] if entry else 0

    async def exists(self, key: str) -> bool:
        """True only for live (non-tombstoned) keys."""
        async with self.lock:
            entry = self.store.get(key)
            return entry is not None and not entry.get("deleted")

    async def size(self) -> int:
        """Count of live keys only (tombstones excluded)."""
        async with self.lock:
            return sum(1 for e in self.store.values() if not e.get("deleted"))

    async def snapshot(self) -> dict[str, dict]:
        """
        Full copy of the store including tombstones.

        Used by /internal/sync so rejoining peers receive deletes and cannot
        resurrect keys that were removed while they were offline.
        """
        async with self.lock:
            return {k: dict(v) for k, v in self.store.items()}

    # ------------------------------------------------------------------
    # Writes — live entries
    # ------------------------------------------------------------------

    async def put(self, key: str, value: str) -> int:
        """Store a key-value pair, auto-incrementing the version. Returns new version."""
        async with self.lock:
            current = self.store.get(key)
            # Version increments from whatever was there (live or tombstone)
            new_version = (current["version"] + 1) if current else 1
            await self.wal.append("PUT", key, value, version=new_version)
            self.store[key] = {"value": value, "version": new_version, "deleted": False}
        logger.debug(f"PUT {key} ver={new_version}")
        return new_version

    async def put_versioned(self, key: str, value: str, version: int) -> None:
        """Store with an explicit version (replica fan-out / sync-on-rejoin)."""
        async with self.lock:
            await self.wal.append("PUT", key, value, version=version)
            self.store[key] = {"value": value, "version": version, "deleted": False}
        logger.debug(f"PUT_VERSIONED {key} ver={version}")

    # ------------------------------------------------------------------
    # Writes — tombstones
    # ------------------------------------------------------------------

    async def delete(self, key: str) -> bool:
        """
        Write a tombstone for key. Returns True if the key was live, False otherwise.

        The tombstone carries version = current_version + 1 so it always beats
        the previous live entry during sync-on-rejoin comparisons.
        Physical removal happens only via compact().
        """
        async with self.lock:
            entry = self.store.get(key)
            if entry is None or entry.get("deleted"):
                return False   # key absent or already a tombstone

            new_version = entry["version"] + 1
            expires_at = time.time() + self._tombstone_retention

            await self.wal.append(
                "DELETE", key, version=new_version,
                tombstone_expires_at=expires_at,
            )
            self.store[key] = {
                "deleted": True,
                "version": new_version,
                "tombstone_expires_at": expires_at,
            }

        logger.debug(f"DELETE {key} tombstone ver={new_version}")
        return True

    async def put_tombstone(
        self, key: str, version: int, tombstone_expires_at: float
    ) -> None:
        """
        Apply a tombstone received from a peer (sync-on-rejoin / replica fan-out).

        Writes to WAL so the tombstone survives a subsequent crash.
        The caller is responsible for the version-comparison guard.
        """
        async with self.lock:
            await self.wal.append(
                "DELETE", key, version=version,
                tombstone_expires_at=tombstone_expires_at,
            )
            self.store[key] = {
                "deleted": True,
                "version": version,
                "tombstone_expires_at": tombstone_expires_at,
            }
        logger.debug(f"PUT_TOMBSTONE {key} ver={version}")

    # ------------------------------------------------------------------
    # Compaction — the ONLY place physical deletion is allowed
    # ------------------------------------------------------------------

    async def compact(self) -> int:
        """
        Physically remove tombstones whose tombstone_expires_at has passed.

        Returns the number of tombstones removed.
        This is the single allowed site of `del self.store[key]`.
        """
        now = time.time()
        async with self.lock:
            expired = [
                k for k, v in self.store.items()
                if v.get("deleted") and v.get("tombstone_expires_at", float("inf")) <= now
            ]
            for key in expired:
                del self.store[key]

        if expired:
            logger.info(f"Compaction: physically removed {len(expired)} expired tombstones")
        return len(expired)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        if self._closed:
            return
        async with self.lock:
            await self.wal.close()
            self._closed = True
        logger.info("StorageEngine closed")

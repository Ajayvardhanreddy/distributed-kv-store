"""
Storage Engine — Phase 3: TTL lazy + active expiration.

Store entry shapes
------------------
Live key (no TTL) : {"value": str, "version": int, "deleted": False, "expires_at": None}
Live key (TTL)    : {"value": str, "version": int, "deleted": False, "expires_at": float}
Tombstone         : {"deleted": True, "version": int, "tombstone_expires_at": float}

Expiration contract
-------------------
  Lazy : GET / exists / size treat entries with expires_at <= now as misses.
         The read path is side-effect-free — no tombstone is written on read.
  Active: get_expired_keys() samples the store and returns keys ready to be
         tombstoned.  The caller (sweeper loop in main.py) calls delete() on
         each, which writes a Phase-2 tombstone and fans out to replicas.

expires_at is an absolute epoch-seconds float. It is set by the write leader
and travels unchanged through the replication fan-out so all replicas expire
at the same wall-clock moment.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Optional

from .wal import WriteAheadLog

logger = logging.getLogger(__name__)

_DEFAULT_RETENTION_SECONDS = 86_400.0

# ---------------------------------------------------------------------------
# Prometheus counters
# ---------------------------------------------------------------------------
try:
    from prometheus_client import REGISTRY, Counter
    try:
        _keys_expired_total = Counter(
            "keys_expired_total",
            "Keys expired and tombstoned by the active sweeper",
        )
    except ValueError:
        _keys_expired_total = REGISTRY._names_to_collectors["keys_expired_total"]
    try:
        _sweeper_runs_total = Counter(
            "sweeper_runs_total",
            "Number of active sweeper iterations",
        )
    except ValueError:
        _sweeper_runs_total = REGISTRY._names_to_collectors["sweeper_runs_total"]
except ImportError:  # pragma: no cover
    class _Stub:
        def inc(self, n=1): ...
    _keys_expired_total = _Stub()
    _sweeper_runs_total = _Stub()


class StorageEngine:
    """
    Thread-safe in-memory KV store with WAL durability, tombstone DELETEs, and TTL.
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
        """Replay WAL to restore state (live keys + tombstones + TTLs)."""
        logger.info("Replaying WAL…")
        self.store = await self.wal.replay()
        live = sum(1 for e in self.store.values() if not e.get("deleted"))
        tomb = len(self.store) - live
        logger.info(f"StorageEngine ready: {live} live keys, {tomb} tombstones")

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    async def get(self, key: str) -> tuple[Optional[str], int]:
        """
        Return (value, version).

        Returns (None, 0) for:
          - missing keys
          - tombstoned keys
          - keys whose expires_at has passed (lazy expiry — no side effects)
        """
        async with self.lock:
            entry = self.store.get(key)
            if entry is None or entry.get("deleted"):
                return None, 0
            expires_at = entry.get("expires_at")
            if expires_at is not None and expires_at <= time.time():
                return None, 0   # expired — caller cannot distinguish from absent
            return entry["value"], entry["version"]

    async def get_version(self, key: str) -> int:
        """Version for key including tombstones. 0 if absent."""
        async with self.lock:
            entry = self.store.get(key)
            return entry["version"] if entry else 0

    async def exists(self, key: str) -> bool:
        """True only for live, non-expired keys."""
        async with self.lock:
            entry = self.store.get(key)
            if entry is None or entry.get("deleted"):
                return False
            expires_at = entry.get("expires_at")
            if expires_at is not None and expires_at <= time.time():
                return False
            return True

    async def size(self) -> int:
        """Count of live, non-expired keys only."""
        async with self.lock:
            now = time.time()
            return sum(
                1 for e in self.store.values()
                if not e.get("deleted") and (
                    e.get("expires_at") is None or e["expires_at"] > now
                )
            )

    async def snapshot(self) -> dict[str, dict]:
        """
        Full copy of the store including tombstones and expires_at.

        Used by /internal/sync — expires_at travels to peers so replicas
        expire at the same wall-clock moment as the leader.
        """
        async with self.lock:
            return {k: dict(v) for k, v in self.store.items()}

    # ------------------------------------------------------------------
    # Writes — live entries
    # ------------------------------------------------------------------

    async def put(self, key: str, value: str, expires_at: Optional[float] = None) -> int:
        """
        Store a key-value pair. Returns new version.

        expires_at: absolute epoch-seconds float set by the write leader.
                    None means the key never expires.
        """
        async with self.lock:
            current = self.store.get(key)
            new_version = (current["version"] + 1) if current else 1
            await self.wal.append(
                "PUT", key, value, version=new_version, expires_at=expires_at
            )
            self.store[key] = {
                "value": value,
                "version": new_version,
                "deleted": False,
                "expires_at": expires_at,
            }
        logger.debug(f"PUT {key} ver={new_version} expires_at={expires_at}")
        return new_version

    async def put_versioned(
        self, key: str, value: str, version: int,
        expires_at: Optional[float] = None,
    ) -> None:
        """Store with explicit version and optional expiry (replica / sync path)."""
        async with self.lock:
            await self.wal.append(
                "PUT", key, value, version=version, expires_at=expires_at
            )
            self.store[key] = {
                "value": value,
                "version": version,
                "deleted": False,
                "expires_at": expires_at,
            }
        logger.debug(f"PUT_VERSIONED {key} ver={version} expires_at={expires_at}")

    # ------------------------------------------------------------------
    # Writes — tombstones
    # ------------------------------------------------------------------

    async def delete(self, key: str) -> bool:
        """
        Write a tombstone for key. Returns True if the key was live, False otherwise.

        Works on expired-but-not-yet-tombstoned keys too — this is exactly what
        the active sweeper calls to finalise expiry.
        """
        async with self.lock:
            entry = self.store.get(key)
            if entry is None or entry.get("deleted"):
                return False

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
        """Apply a peer tombstone (sync-on-rejoin / replica fan-out)."""
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
    # Active sweeper support
    # ------------------------------------------------------------------

    async def get_expired_keys(self, sample_size: int = 100) -> list[str]:
        """
        Sample up to sample_size live keys and return those whose expires_at
        has passed. Never does a full scan — O(sample_size).

        The caller (sweeper loop) is responsible for calling delete() on each
        returned key and fanning out the tombstone to replicas.
        """
        now = time.time()
        async with self.lock:
            # Only consider live (non-tombstoned) keys that have a TTL
            candidates = [
                k for k, v in self.store.items()
                if not v.get("deleted") and v.get("expires_at") is not None
            ]
            if not candidates:
                return []
            sample = random.sample(candidates, min(sample_size, len(candidates)))
            return [k for k in sample if self.store[k]["expires_at"] <= now]

    # ------------------------------------------------------------------
    # Compaction — only place physical deletion is allowed
    # ------------------------------------------------------------------

    async def compact(self) -> int:
        """Physically remove tombstones past tombstone_expires_at."""
        now = time.time()
        async with self.lock:
            expired = [
                k for k, v in self.store.items()
                if v.get("deleted") and v.get("tombstone_expires_at", float("inf")) <= now
            ]
            for key in expired:
                del self.store[key]
        if expired:
            logger.info(f"Compaction: removed {len(expired)} expired tombstones")
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

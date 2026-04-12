"""
Write-Ahead Log (WAL) implementation for crash recovery.

The WAL ensures durability by logging all mutations before they're applied
to the in-memory store. On restart, we replay the log to restore state.

Durability modes:
  - strict (default): every append fsync's to disk — safest, slower PUTs
  - relaxed: writes buffer in memory, flushed every 100ms or 100 ops —
    trades up to 100ms of data for ~5-10× PUT throughput (like Kafka acks=1)

Format: JSON-lines (one operation per line)
Example:
    {"op":"PUT","key":"user:1","value":"alice","ver":3,"ts":1705612800}
    {"op":"DELETE","key":"user:1","ver":0,"ts":1705612802}
"""
import json
import os
from typing import Optional
import aiofiles
import asyncio
import time
import logging

logger = logging.getLogger(__name__)

# Relaxed-mode tunables
_FLUSH_INTERVAL_SEC = 0.1   # 100ms
_FLUSH_BATCH_SIZE = 100     # ops


class WriteAheadLog:
    """
    Append-only log for recording storage mutations.

    Each mutation (PUT/DELETE) is written to the log before being applied
    to memory. On restart, replay() reconstructs the current state.

    Thread-safe through async lock.  Durability mode is configured via
    the `durability` constructor parameter.
    """

    def __init__(self, file_path: str, durability: str = "strict"):
        """
        Args:
            file_path: Path to the WAL file
            durability: "strict" (flush every write) or "relaxed" (batch flush)
        """
        self.file_path = file_path
        self.durability = durability
        self.lock = asyncio.Lock()

        # Relaxed-mode buffer
        self._buffer: list[str] = []
        self._flush_task: Optional[asyncio.Task] = None

        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

        # Create file if it doesn't exist
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                pass
            logger.info(f"Created new WAL file: {file_path}")
        else:
            logger.info(f"Using existing WAL file: {file_path}")

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------

    async def start_flush_loop(self) -> None:
        """Start the background flush loop (only used in relaxed mode)."""
        if self.durability == "relaxed" and self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())
            logger.info("WAL: relaxed-mode flush loop started")

    async def _flush_loop(self) -> None:
        """Background coroutine: flushes the buffer every _FLUSH_INTERVAL_SEC."""
        try:
            while True:
                await asyncio.sleep(_FLUSH_INTERVAL_SEC)
                await self._flush_buffer()
        except asyncio.CancelledError:
            # Final flush on shutdown
            await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Write buffered lines to disk in one batch."""
        async with self.lock:
            if not self._buffer:
                return
            batch = "".join(self._buffer)
            self._buffer.clear()
        # Write outside the lock (I/O bound)
        async with aiofiles.open(self.file_path, mode='a') as f:
            await f.write(batch)
        logger.debug(f"WAL: flushed {batch.count(chr(10))} entries (relaxed)")

    # -----------------------------------------------------------------
    # Append
    # -----------------------------------------------------------------

    async def append(
        self, operation: str, key: str, value: Optional[str] = None, version: int = 0
    ) -> None:
        """
        Append an operation to the log.

        Args:
            operation: "PUT" or "DELETE"
            key: The key being modified
            value: The value (required for PUT, None for DELETE)
            version: Monotonic version counter for this key

        The entry includes a timestamp for debugging and ordering.
        """
        entry = {
            "op": operation,
            "key": key,
            "ver": version,
            "ts": int(time.time()),
        }
        if value is not None:
            entry["value"] = value

        line = json.dumps(entry) + "\n"

        if self.durability == "relaxed":
            async with self.lock:
                self._buffer.append(line)
                # Force flush if buffer exceeds batch size
                if len(self._buffer) >= _FLUSH_BATCH_SIZE:
                    batch = "".join(self._buffer)
                    self._buffer.clear()
                    # Release lock before I/O
                    async with aiofiles.open(self.file_path, mode='a') as f:
                        await f.write(batch)
        else:
            # strict: flush to disk immediately
            async with self.lock:
                async with aiofiles.open(self.file_path, mode='a') as f:
                    await f.write(line)

        logger.debug(f"WAL: {operation} key={key} ver={version}")

    # -----------------------------------------------------------------
    # Replay
    # -----------------------------------------------------------------

    async def replay(self) -> dict[str, dict]:
        """
        Replay the log to reconstruct current state.

        Reads all entries and applies them in order to build the current
        key-value mapping.  Each entry includes a version counter.

        Returns:
            Dict mapping keys to {"value": str, "version": int}

        Handles corrupted lines gracefully by logging warnings.
        Legacy WAL entries without a "ver" field default to version 1.
        """
        state: dict[str, dict] = {}
        line_number = 0

        async with aiofiles.open(self.file_path, mode='r') as f:
            async for line in f:
                line_number += 1
                line = line.strip()

                if not line:
                    continue

                try:
                    entry = json.loads(line)
                    op = entry["op"]
                    key = entry["key"]
                    ver = entry.get("ver", 1)  # backwards-compat

                    if op == "PUT":
                        state[key] = {"value": entry["value"], "version": ver}
                    elif op == "DELETE":
                        state.pop(key, None)  # Remove if exists
                    else:
                        logger.warning(f"Unknown operation '{op}' at line {line_number}")

                except json.JSONDecodeError as e:
                    logger.warning(f"Corrupted WAL entry at line {line_number}: {e}")
                except KeyError as e:
                    logger.warning(f"Invalid WAL entry at line {line_number}: missing {e}")

        logger.info(f"WAL replay complete: {len(state)} keys from {line_number} entries")
        return state

    # -----------------------------------------------------------------
    # Close
    # -----------------------------------------------------------------

    async def close(self) -> None:
        """Flush any buffered writes and stop the flush loop."""
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        # Final flush of anything left in the buffer
        await self._flush_buffer()
        logger.debug("WAL closed")


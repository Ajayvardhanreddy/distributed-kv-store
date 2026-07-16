"""
Write-Ahead Log (WAL) — Phase 1: Binary format with CRC32.

File layout
-----------
Header  : [4-byte magic 'BWAL'][1-byte format-version = 1]
Records : repeated [4-byte payload-length][4-byte CRC32][msgpack payload]

Payload (msgpack dict, schema_version is always the first key)
--------------------------------------------------------------
PUT    : {schema_version:1, op:"PUT",    key:str, value:str, ver:int, ts:int}
DELETE : {schema_version:1, op:"DELETE", key:str,            ver:int, ts:int}

Durability modes
----------------
strict  (default) — every append flushes to disk immediately.
relaxed           — writes buffer in memory, flushed every 100 ms or 100 ops.

Migration
---------
If a WAL file lacks the magic header it is treated as a legacy JSON-lines log.
Its contents are replayed (corrupt lines skipped), a fresh binary log is written
in its place, and the old file is renamed to <path>.bak.

Replay safety
-------------
On CRC mismatch or short read the replay stops at the last successfully applied
record, logs the byte offset, and returns the state built so far.  A torn write
at the end of the file never raises.

Metrics
-------
wal_corrupt_records_total  — records skipped due to CRC or framing errors.
wal_replay_records_total   — records successfully applied during replay.
"""
from __future__ import annotations

import asyncio
import binascii
import json
import logging
import os
import struct
import time
from typing import Optional

import aiofiles
import msgpack

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File constants
# ---------------------------------------------------------------------------
MAGIC = b"BWAL"
FORMAT_VERSION = 1
FILE_HEADER = MAGIC + struct.pack("B", FORMAT_VERSION)   # 5 bytes
RECORD_HEADER_SIZE = 8   # 4-byte length + 4-byte CRC32

# ---------------------------------------------------------------------------
# Relaxed-mode tunables
# ---------------------------------------------------------------------------
_FLUSH_INTERVAL_SEC = 0.1
_FLUSH_BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# Prometheus counters (graceful fallback when library is absent)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter
    _wal_corrupt_total = Counter(
        "wal_corrupt_records_total",
        "WAL records skipped due to CRC or framing errors",
    )
    _wal_replay_total = Counter(
        "wal_replay_records_total",
        "WAL records successfully applied during replay",
    )
except ImportError:  # pragma: no cover
    class _StubCounter:  # type: ignore[no-redef]
        class _val:
            def get(self): return 0.0
        _value = _val()
        def inc(self, n: float = 1) -> None: ...
    _wal_corrupt_total = _StubCounter()  # type: ignore[assignment]
    _wal_replay_total = _StubCounter()   # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Record encode / decode helpers
# ---------------------------------------------------------------------------

def _encode_record(payload: dict) -> bytes:
    """Serialise a payload dict → framed binary record."""
    raw = msgpack.packb(payload, use_bin_type=True)
    crc = binascii.crc32(raw) & 0xFFFFFFFF
    return struct.pack(">II", len(raw), crc) + raw


def _decode_record(data: bytes, offset: int) -> tuple[Optional[dict], int]:
    """
    Try to read one record starting at *offset* in *data*.

    Returns (payload_dict, new_offset) on success.
    Returns (None, offset) when *data* ends cleanly at a record boundary (EOF).
    Returns ("corrupt", offset) when a CRC mismatch or framing error is found.
    """
    if offset >= len(data):
        return None, offset  # clean EOF

    remaining = len(data) - offset
    if remaining < RECORD_HEADER_SIZE:
        # Partial header — torn write at file end
        return "corrupt", offset

    length, stored_crc = struct.unpack_from(">II", data, offset)
    payload_start = offset + RECORD_HEADER_SIZE
    payload_end = payload_start + length

    if payload_end > len(data):
        # Partial payload — torn write
        return "corrupt", offset

    raw = data[payload_start:payload_end]
    computed_crc = binascii.crc32(raw) & 0xFFFFFFFF
    if computed_crc != stored_crc:
        return "corrupt", offset

    payload = msgpack.unpackb(raw, raw=False)
    return payload, payload_end


# ---------------------------------------------------------------------------
# WriteAheadLog
# ---------------------------------------------------------------------------

class WriteAheadLog:
    """
    Append-only binary WAL.  Replays into {key: {"value": str, "version": int}}.
    """

    def __init__(self, file_path: str, durability: str = "strict"):
        self.file_path = file_path
        self.durability = durability
        self.lock = asyncio.Lock()
        self._buffer: list[bytes] = []
        self._flush_task: Optional[asyncio.Task] = None

        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

        if not os.path.exists(file_path):
            # Brand-new WAL — write the file header immediately (sync is fine here)
            with open(file_path, "wb") as f:
                f.write(FILE_HEADER)
            logger.info(f"WAL: created new binary file {file_path}")
        else:
            logger.info(f"WAL: using existing file {file_path}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start_flush_loop(self) -> None:
        if self.durability == "relaxed" and self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())
            logger.info("WAL: relaxed-mode flush loop started")

    async def _flush_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(_FLUSH_INTERVAL_SEC)
                await self._flush_buffer()
        except asyncio.CancelledError:
            await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        async with self.lock:
            if not self._buffer:
                return
            batch = b"".join(self._buffer)
            self._buffer.clear()
        async with aiofiles.open(self.file_path, mode="ab") as f:
            await f.write(batch)
        logger.debug(f"WAL: relaxed flush {len(batch)} bytes")

    async def close(self) -> None:
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        await self._flush_buffer()
        logger.debug("WAL closed")

    # ------------------------------------------------------------------
    # Append
    # ------------------------------------------------------------------

    async def append(
        self,
        operation: str,
        key: str,
        value: Optional[str] = None,
        version: int = 0,
        tombstone_expires_at: Optional[float] = None,
        expires_at: Optional[float] = None,
    ) -> None:
        payload: dict = {
            "schema_version": 1,
            "op": operation,
            "key": key,
            "ver": version,
            "ts": int(time.time()),
        }
        if value is not None:
            payload["value"] = value
        if tombstone_expires_at is not None:
            payload["tombstone_expires_at"] = tombstone_expires_at
        if expires_at is not None:
            payload["expires_at"] = expires_at

        record = _encode_record(payload)

        if self.durability == "relaxed":
            async with self.lock:
                self._buffer.append(record)
                if len(self._buffer) >= _FLUSH_BATCH_SIZE:
                    batch = b"".join(self._buffer)
                    self._buffer.clear()
                    async with aiofiles.open(self.file_path, mode="ab") as f:
                        await f.write(batch)
        else:
            async with self.lock:
                async with aiofiles.open(self.file_path, mode="ab") as f:
                    await f.write(record)

        logger.debug(f"WAL: {operation} key={key} ver={version}")

    # ------------------------------------------------------------------
    # Replay
    # ------------------------------------------------------------------

    async def replay(self) -> dict[str, dict]:
        """
        Replay the WAL and return the final key-value state.

        Detects legacy JSON format and migrates automatically.
        Stops at the first corrupt / truncated record; prior records are kept.

        Returns:
            {key: {"value": str, "version": int}}
        """
        # Detect format by checking the file header
        with open(self.file_path, "rb") as f:
            header = f.read(len(FILE_HEADER))

        if header != FILE_HEADER:
            # Legacy JSON WAL — migrate then return
            return await self._migrate_from_json()

        return await self._replay_binary()

    async def _replay_binary(self) -> dict[str, dict]:
        async with aiofiles.open(self.file_path, mode="rb") as f:
            data = await f.read()

        state: dict[str, dict] = {}
        offset = len(FILE_HEADER)   # skip file header

        while True:
            result, new_offset = _decode_record(data, offset)

            if result is None:
                break   # clean EOF

            if result == "corrupt":
                logger.warning(
                    f"WAL: corrupt/truncated record at byte offset {offset}, "
                    f"stopping replay ({len(state)} keys recovered)"
                )
                _wal_corrupt_total.inc()
                break

            offset = new_offset
            _wal_replay_total.inc()
            self._apply_entry(state, result)

        logger.info(f"WAL replay complete: {len(state)} keys")
        return state

    @staticmethod
    def _apply_entry(state: dict, entry: dict) -> None:
        op = entry.get("op")
        key = entry.get("key")
        ver = entry.get("ver", 1)

        if op == "PUT":
            state[key] = {
                "value": entry.get("value", ""),
                "version": ver,
                "deleted": False,
                "expires_at": entry.get("expires_at"),   # None = no TTL
            }
        elif op == "DELETE":
            # Phase 2: DELETE produces a tombstone, never a physical removal.
            # tombstone_expires_at may be absent in very old records — default far future.
            expires = entry.get("tombstone_expires_at", time.time() + 86400)
            state[key] = {
                "deleted": True,
                "version": ver,
                "tombstone_expires_at": expires,
            }
        else:
            logger.warning(f"WAL: unknown op '{op}' for key '{key}' — skipped")

    # ------------------------------------------------------------------
    # JSON → binary migration
    # ------------------------------------------------------------------

    async def _migrate_from_json(self) -> dict[str, dict]:
        logger.info(f"WAL: legacy JSON format detected — migrating {self.file_path}")
        state = await self._replay_json_legacy()

        bak_path = self.file_path + ".bak"
        os.rename(self.file_path, bak_path)
        logger.info(f"WAL: old JSON file renamed to {bak_path}")

        # Write fresh binary WAL from replayed state
        with open(self.file_path, "wb") as f:
            f.write(FILE_HEADER)

        for key, entry in state.items():
            payload = {
                "schema_version": 1,
                "op": "PUT",
                "key": key,
                "ver": entry["version"],
                "ts": int(time.time()),
                "value": entry["value"],
            }
            record = _encode_record(payload)
            with open(self.file_path, "ab") as f:
                f.write(record)

        logger.info(
            f"WAL: migration complete — {len(state)} keys written to binary WAL"
        )
        return state

    async def _replay_json_legacy(self) -> dict[str, dict]:
        """Replay a legacy JSON-lines WAL file. Skips corrupt/incomplete lines."""
        state: dict[str, dict] = {}
        line_number = 0

        async with aiofiles.open(self.file_path, mode="r") as f:
            async for line in f:
                line_number += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    op = entry["op"]
                    key = entry["key"]
                    ver = entry.get("ver", 1)
                    if op == "PUT":
                        state[key] = {"value": entry["value"], "version": ver}
                    elif op == "DELETE":
                        state.pop(key, None)
                    else:
                        logger.warning(f"WAL legacy: unknown op '{op}' at line {line_number}")
                except json.JSONDecodeError as e:
                    logger.warning(f"WAL legacy: corrupt JSON at line {line_number}: {e}")
                except KeyError as e:
                    logger.warning(f"WAL legacy: missing field {e} at line {line_number}")

        logger.info(f"WAL legacy replay: {len(state)} keys from {line_number} lines")
        return state

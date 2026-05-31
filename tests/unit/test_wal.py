"""
Unit tests for Write-Ahead Log (WAL) — Phase 1: Binary format with CRC32.

Tests cover both logical behaviour (replay returns correct state) and
physical format (magic header, record framing, CRC integrity, migration).
"""
import os
import struct
import tempfile
import binascii

import msgpack
import pytest

from app.storage.wal import WriteAheadLog, MAGIC, FORMAT_VERSION, RECORD_HEADER_SIZE


# ---------------------------------------------------------------------------
# Logical behaviour (unchanged from Phase 0 — must all still pass)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wal_append_and_replay():
    """Basic append and replay returns correct final state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)

        await wal.append("PUT", "key1", "value1")
        await wal.append("PUT", "key2", "value2")
        await wal.append("DELETE", "key1")

        state = await wal.replay()
        assert "key1" not in state
        assert state["key2"]["value"] == "value2"

        await wal.close()


@pytest.mark.asyncio
async def test_wal_empty_replay():
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        state = await wal.replay()
        assert state == {}
        await wal.close()


@pytest.mark.asyncio
async def test_wal_multiple_puts_same_key():
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)

        await wal.append("PUT", "key1", "value1")
        await wal.append("PUT", "key1", "value2")
        await wal.append("PUT", "key1", "value3")

        state = await wal.replay()
        assert state["key1"]["value"] == "value3"

        await wal.close()


@pytest.mark.asyncio
async def test_wal_delete_nonexistent():
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        await wal.append("DELETE", "nonexistent")
        state = await wal.replay()
        assert state == {}
        await wal.close()


@pytest.mark.asyncio
async def test_wal_persistence_across_instances():
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")

        wal1 = WriteAheadLog(wal_path)
        await wal1.append("PUT", "persistent", "data")
        await wal1.close()

        wal2 = WriteAheadLog(wal_path)
        state = await wal2.replay()
        assert state["persistent"]["value"] == "data"
        await wal2.close()


# ---------------------------------------------------------------------------
# Binary format assertions (Phase 1 new tests)
# ---------------------------------------------------------------------------

def _read_file_header(path: str) -> tuple[bytes, int]:
    """Return (magic_bytes, format_version) from the file header."""
    with open(path, "rb") as f:
        magic = f.read(4)
        fmt_ver = struct.unpack("B", f.read(1))[0]
    return magic, fmt_ver


@pytest.mark.asyncio
async def test_binary_wal_magic_header():
    """New WAL files start with the correct magic bytes and format version."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        await wal.append("PUT", "k", "v")
        await wal.close()

        magic, fmt_ver = _read_file_header(wal_path)
        assert magic == MAGIC
        assert fmt_ver == FORMAT_VERSION


@pytest.mark.asyncio
async def test_binary_wal_record_structure():
    """Each record is [4B length][4B CRC32][msgpack payload] with schema_version=1."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        await wal.append("PUT", "hello", "world", version=7)
        await wal.close()

        with open(wal_path, "rb") as f:
            f.read(5)  # skip file header
            raw_hdr = f.read(8)
            assert len(raw_hdr) == 8, "record header must be 8 bytes"
            length, stored_crc = struct.unpack(">II", raw_hdr)
            payload_bytes = f.read(length)
            assert len(payload_bytes) == length

        computed_crc = binascii.crc32(payload_bytes) & 0xFFFFFFFF
        assert computed_crc == stored_crc, "CRC32 must match"

        payload = msgpack.unpackb(payload_bytes, raw=False)
        assert payload["schema_version"] == 1
        assert payload["op"] == "PUT"
        assert payload["key"] == "hello"
        assert payload["value"] == "world"
        assert payload["ver"] == 7
        assert "ts" in payload


@pytest.mark.asyncio
async def test_binary_wal_crc_corruption_stops_at_last_good_record():
    """CRC mismatch stops replay at last good record; prior records still applied."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        await wal.append("PUT", "good1", "v1")
        await wal.append("PUT", "good2", "v2")
        await wal.close()

        # Corrupt the CRC of the second record
        with open(wal_path, "r+b") as f:
            f.read(5)  # skip file header
            # skip first record entirely
            hdr1 = f.read(8)
            length1 = struct.unpack(">II", hdr1)[0]
            f.read(length1)
            # now at second record header — overwrite its CRC
            rec2_start = f.tell()
            hdr2 = f.read(8)
            length2 = struct.unpack(">II", hdr2)[0]
            f.seek(rec2_start + 4)  # CRC is bytes 4-7 of record header
            f.write(b'\xFF\xFF\xFF\xFF')  # garbage CRC

        wal2 = WriteAheadLog(wal_path)
        state = await wal2.replay()
        await wal2.close()

        assert "good1" in state       # first record survived
        assert "good2" not in state   # second record discarded (CRC bad)


@pytest.mark.asyncio
async def test_binary_wal_short_read_stops_gracefully():
    """Truncated file (torn write) stops replay without raising."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        await wal.append("PUT", "safe", "yes")
        await wal.close()

        # Append a partial record header (4 bytes of garbage — not a full 8-byte header)
        with open(wal_path, "ab") as f:
            f.write(struct.pack(">I", 9999))   # length only, no CRC, no payload

        wal2 = WriteAheadLog(wal_path)
        state = await wal2.replay()
        await wal2.close()

        assert state["safe"]["value"] == "yes"


@pytest.mark.asyncio
async def test_binary_wal_migration_from_json():
    """Legacy JSON WAL is migrated to binary; .bak file is created."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "legacy.wal")

        # Write a legacy JSON WAL by hand
        import json, time
        with open(wal_path, "w") as f:
            f.write(json.dumps({"op": "PUT", "key": "migrated", "value": "yes",
                                 "ver": 1, "ts": int(time.time())}) + "\n")
            f.write(json.dumps({"op": "PUT", "key": "also", "value": "ok",
                                 "ver": 1, "ts": int(time.time())}) + "\n")

        wal = WriteAheadLog(wal_path)
        state = await wal.replay()
        await wal.close()

        # State must be correct
        assert state["migrated"]["value"] == "yes"
        assert state["also"]["value"] == "ok"

        # Original file must now be binary
        magic, _ = _read_file_header(wal_path)
        assert magic == MAGIC, "migrated file must have binary magic"

        # Backup must exist
        assert os.path.exists(wal_path + ".bak"), ".bak file must be created"


@pytest.mark.asyncio
async def test_binary_wal_migration_with_corrupt_json_line():
    """Migration skips corrupt JSON lines; valid lines still make it to state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "corrupt_legacy.wal")
        import json, time
        with open(wal_path, "w") as f:
            f.write(json.dumps({"op": "PUT", "key": "key1", "value": "value1",
                                 "ver": 1, "ts": int(time.time())}) + "\n")
            f.write("this is not valid JSON\n")
            f.write(json.dumps({"op": "PUT", "key": "key2", "value": "value2",
                                 "ver": 1, "ts": int(time.time())}) + "\n")

        wal = WriteAheadLog(wal_path)
        state = await wal.replay()
        await wal.close()

        assert state["key1"]["value"] == "value1"
        assert state["key2"]["value"] == "value2"


@pytest.mark.asyncio
async def test_binary_wal_migration_with_missing_field():
    """Migration skips JSON entries with missing required fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "missing_field.wal")
        import json, time
        with open(wal_path, "w") as f:
            f.write(json.dumps({"op": "PUT", "value": "value1",
                                 "ver": 1, "ts": int(time.time())}) + "\n")  # missing key
            f.write(json.dumps({"op": "PUT", "key": "key2", "value": "value2",
                                 "ver": 1, "ts": int(time.time())}) + "\n")

        wal = WriteAheadLog(wal_path)
        state = await wal.replay()
        await wal.close()

        assert "key2" in state
        assert len(state) == 1


@pytest.mark.asyncio
async def test_binary_wal_replay_metrics():
    """wal_replay_records_total increments for each good record replayed."""
    from app.storage import wal as wal_module

    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "metrics.wal")
        w = WriteAheadLog(wal_path)
        await w.append("PUT", "a", "1")
        await w.append("PUT", "b", "2")
        await w.append("DELETE", "a")
        await w.close()

        before = wal_module._wal_replay_total._value.get()
        w2 = WriteAheadLog(wal_path)
        await w2.replay()
        await w2.close()
        after = wal_module._wal_replay_total._value.get()

        assert after - before == 3   # 2 PUTs + 1 DELETE


@pytest.mark.asyncio
async def test_binary_wal_corrupt_metric():
    """wal_corrupt_records_total increments when a record is skipped."""
    from app.storage import wal as wal_module

    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "corr_metric.wal")
        w = WriteAheadLog(wal_path)
        await w.append("PUT", "x", "y")
        await w.close()

        # Corrupt the single record's CRC
        with open(wal_path, "r+b") as f:
            f.read(5)  # file header
            f.read(4)  # length field of record
            f.write(b'\x00\x00\x00\x00')  # zero out CRC

        before = wal_module._wal_corrupt_total._value.get()
        w2 = WriteAheadLog(wal_path)
        await w2.replay()
        await w2.close()
        after = wal_module._wal_corrupt_total._value.get()

        assert after - before == 1

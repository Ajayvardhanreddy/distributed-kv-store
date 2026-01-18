"""
Unit tests for Write-Ahead Log (WAL).

Tests WAL append, replay, and error handling.
"""
import pytest
import tempfile
import os
import json
from app.storage.wal import WriteAheadLog


@pytest.mark.asyncio
async def test_wal_append_and_replay():
    """Test basic append and replay"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        
        # Append some operations
        await wal.append("PUT", "key1", "value1")
        await wal.append("PUT", "key2", "value2")
        await wal.append("DELETE", "key1")
        
        # Replay should give us current state
        state = await wal.replay()
        assert "key1" not in state  # Was deleted
        assert state["key2"] == "value2"
        
        await wal.close()


@pytest.mark.asyncio
async def test_wal_empty_replay():
    """Test replaying an empty WAL"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        
        state = await wal.replay()
        assert state == {}
        
        await wal.close()


@pytest.mark.asyncio
async def test_wal_multiple_puts_same_key():
    """Test multiple PUTs to the same key"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        
        # Multiple updates to same key
        await wal.append("PUT", "key1", "value1")
        await wal.append("PUT", "key1", "value2")
        await wal.append("PUT", "key1", "value3")
        
        # Should have final value
        state = await wal.replay()
        assert state["key1"] == "value3"
        
        await wal.close()


@pytest.mark.asyncio
async def test_wal_delete_nonexistent():
    """Test deleting a key that was never added"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        
        await wal.append("DELETE", "nonexistent")
        
        state = await wal.replay()
        assert state == {}
        
        await wal.close()


@pytest.mark.asyncio
async def test_wal_corrupted_line():
    """Test that corrupted lines are skipped gracefully"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # Write a corrupted WAL manually
        with open(wal_path, 'w') as f:
            f.write('{"op":"PUT","key":"key1","value":"value1"}\n')
            f.write('this is not valid JSON\n')  # Corrupted line
            f.write('{"op":"PUT","key":"key2","value":"value2"}\n')
        
        wal = WriteAheadLog(wal_path)
        state = await wal.replay()
        
        # Should have valid entries despite corruption
        assert state["key1"] == "value1"
        assert state["key2"] == "value2"
        
        await wal.close()


@pytest.mark.asyncio
async def test_wal_missing_field():
    """Test that entries with missing fields are skipped"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # Write an entry missing the "key" field
        with open(wal_path, 'w') as f:
            f.write('{"op":"PUT","value":"value1"}\n')  # Missing "key"
            f.write('{"op":"PUT","key":"key2","value":"value2"}\n')
        
        wal = WriteAheadLog(wal_path)
        state = await wal.replay()
        
        # Should only have the valid entry
        assert state.get("key2") == "value2"
        assert len(state) == 1
        
        await wal.close()


@pytest.mark.asyncio
async def test_wal_persistence_across_instances():
    """Test that WAL persists across multiple instances"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        
        # First instance: write some data
        wal1 = WriteAheadLog(wal_path)
        await wal1.append("PUT", "persistent", "data")
        await wal1.close()
        
        # Second instance: replay should recover data
        wal2 = WriteAheadLog(wal_path)
        state = await wal2.replay()
        assert state["persistent"] == "data"
        await wal2.close()


@pytest.mark.asyncio
async def test_wal_format():
    """Test that WAL entries are properly formatted"""
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "test.wal")
        wal = WriteAheadLog(wal_path)
        
        await wal.append("PUT", "key1", "value1")
        await wal.close()
        
        # Read the WAL file directly
        with open(wal_path, 'r') as f:
            line = f.readline().strip()
            entry = json.loads(line)
            
            # Verify structure
            assert entry["op"] == "PUT"
            assert entry["key"] == "key1"
            assert entry["value"] == "value1"
            assert "ts" in entry  # Timestamp should be present

"""
Basic integration tests for the KV store API.

These tests verify the core CRUD operations work correctly with sharding.
"""
import pytest
import tempfile
import os
from fastapi.testclient import TestClient
from app.main import app
import app.main as main_module
from app.cluster.shard_manager import ShardManager


@pytest.fixture(autouse=True)
async def setup_storage():
    """Initialize sharded storage for each test"""
    # Create temp directory for test WALs
    tmpdir = tempfile.mkdtemp()
    
    # Initialize shard manager with 3 shards
    shard_ids = ["test-shard-0", "test-shard-1", "test-shard-2"]
    main_module.storage = ShardManager(shard_ids, tmpdir)
    await main_module.storage.initialize()
    
    yield
    
    # Cleanup
    await main_module.storage.close()


client = TestClient(app)


def test_health_check():
    """Test that health check endpoint returns expected data"""
    response = client.get("/health")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "keys_stored" in data


def test_put_and_get():
    """Test storing and retrieving a value"""
    # Put a value
    put_response = client.put(
        "/kv/test-key",
        json={"key": "test-key", "value": "test-value"}
    )
    assert put_response.status_code == 200
    assert put_response.json()["key"] == "test-key"
    
    # Get the value back
    get_response = client.get("/kv/test-key")
    assert get_response.status_code == 200
    assert get_response.json()["value"] == "test-value"


def test_get_nonexistent_key():
    """Test that getting a nonexistent key returns 404"""
    response = client.get("/kv/does-not-exist")
    
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_delete():
    """Test deleting a key"""
    # First put a value
    client.put(
        "/kv/delete-me",
        json={"key": "delete-me", "value": "temporary"}
    )
    
    # Delete it
    delete_response = client.delete("/kv/delete-me")
    assert delete_response.status_code == 200
    assert delete_response.json()["key"] == "delete-me"
    
    # Verify it's gone
    get_response = client.get("/kv/delete-me")
    assert get_response.status_code == 404


def test_delete_nonexistent_key():
    """Test that deleting a nonexistent key returns 404"""
    response = client.delete("/kv/does-not-exist")
    
    assert response.status_code == 404

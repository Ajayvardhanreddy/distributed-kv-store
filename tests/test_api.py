"""
API integration tests for the KV store.

Tests run against a single-node ClusterRouter (no peers), so all
operations are local — no HTTP forwarding takes place.
"""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.main import app
from app.storage.engine import StorageEngine


@pytest.fixture(autouse=True)
async def setup_cluster(monkeypatch):
    """
    Wire up a single-node ClusterRouter before every test.
    Sets NODE_ID=node-0, PEERS=http://localhost:8000 so the router
    treats every key as local (no HTTP calls needed).
    """
    monkeypatch.setenv("NODE_ID", "node-0")
    monkeypatch.setenv("PEERS", "http://localhost:8000")

    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = NodeConfig()
        ring = ConsistentHashRing(num_vnodes=10)
        ring.add_node("node-0")

        storage = StorageEngine(os.path.join(tmpdir, "node-0.wal"))
        await storage.initialize()

        r = ClusterRouter(cfg, storage, ring)
        await r.initialize()

        main_module.config = cfg
        main_module.router = r

        yield

        await r.close()
        await storage.close()


client = TestClient(app)


def test_health_check():
    """Test that health check endpoint returns expected data"""
    response = client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "local_keys" in data
    assert "node_id" in data


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

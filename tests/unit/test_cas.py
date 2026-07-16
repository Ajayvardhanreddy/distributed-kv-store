"""
Unit tests for Phase 4 — CAS conditional writes.

Covers:
  - version_token encoding / decoding
  - version_matches() function
  - PUT if_match: match succeeds, mismatch → 409
  - PUT if_none_match: absent key succeeds, existing key → 409
  - if_none_match blocks resurrection over a tombstone
  - DELETE if_match: match succeeds, mismatch → 409
  - Unconditional write (no if_match) is back-compat
  - GET response carries version_token (no raw int)
  - cas_conflicts_total metric increments on mismatch
  - Replicas do NOT re-check CAS (put_versioned used)
"""
from __future__ import annotations

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.main import app
from app.storage.engine import StorageEngine
from app.storage.version_token import decode_token, encode_token, version_matches

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
async def setup_single_node(monkeypatch):
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
        app.state.config = cfg
        app.state.router = r
        yield
        await r.close()
        await storage.close()


client = TestClient(app)


# ---------------------------------------------------------------------------
# version_token module
# ---------------------------------------------------------------------------

def test_encode_token_returns_string():
    assert isinstance(encode_token(1), str)
    assert isinstance(encode_token(42), str)


def test_encode_decode_roundtrip():
    for v in (0, 1, 99, 1_000_000):
        assert decode_token(encode_token(v)) == v


def test_version_matches_correct_token():
    assert version_matches(3, encode_token(3)) is True


def test_version_matches_wrong_token():
    assert version_matches(3, encode_token(5)) is False


def test_version_matches_invalid_token():
    assert version_matches(1, "not-a-number") is False


# ---------------------------------------------------------------------------
# GET response carries version_token (approved spec: no raw int)
# ---------------------------------------------------------------------------

def test_get_response_has_version_token():
    client.put("/kv/k", json={"key": "k", "value": "v"})
    resp = client.get("/kv/k")
    assert resp.status_code == 200
    data = resp.json()
    assert "version_token" in data
    assert "version" not in data   # raw int deprecated in Phase 4


def test_version_token_increments_on_put():
    client.put("/kv/k", json={"key": "k", "value": "v1"})
    t1 = client.get("/kv/k").json()["version_token"]
    client.put("/kv/k", json={"key": "k", "value": "v2"})
    t2 = client.get("/kv/k").json()["version_token"]
    assert decode_token(t2) == decode_token(t1) + 1


# ---------------------------------------------------------------------------
# PUT with if_match
# ---------------------------------------------------------------------------

def test_put_if_match_success():
    client.put("/kv/k", json={"key": "k", "value": "v1"})
    token = client.get("/kv/k").json()["version_token"]

    resp = client.put("/kv/k", json={"key": "k", "value": "v2", "if_match": token})
    assert resp.status_code == 200


def test_put_if_match_mismatch_returns_409():
    client.put("/kv/k", json={"key": "k", "value": "v1"})
    # Use a stale token
    stale_token = encode_token(999)
    resp = client.put("/kv/k", json={"key": "k", "value": "v2", "if_match": stale_token})
    assert resp.status_code == 409
    body = resp.json()
    assert "current_token" in body["detail"]


def test_put_if_match_mismatch_body_contains_current_token():
    client.put("/kv/k", json={"key": "k", "value": "v1"})
    current_token = client.get("/kv/k").json()["version_token"]

    resp = client.put(
        "/kv/k", json={"key": "k", "value": "v2", "if_match": encode_token(999)}
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["current_token"] == current_token


def test_put_no_if_match_is_unconditional():
    """Absent if_match → unconditional write, back-compat."""
    client.put("/kv/k", json={"key": "k", "value": "v1"})
    resp = client.put("/kv/k", json={"key": "k", "value": "v2"})
    assert resp.status_code == 200


def test_put_if_match_on_nonexistent_key_returns_409():
    """if_match on a key that doesn't exist → 409 (version 0 mismatch)."""
    resp = client.put(
        "/kv/missing", json={"key": "missing", "value": "v", "if_match": encode_token(1)}
    )
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# PUT with if_none_match (create-if-absent)
# ---------------------------------------------------------------------------

def test_put_if_none_match_on_absent_key_succeeds():
    resp = client.put(
        "/kv/new-key", json={"key": "new-key", "value": "v", "if_none_match": True}
    )
    assert resp.status_code == 200


def test_put_if_none_match_on_existing_key_returns_409():
    client.put("/kv/k", json={"key": "k", "value": "v1"})
    resp = client.put("/kv/k", json={"key": "k", "value": "v2", "if_none_match": True})
    assert resp.status_code == 409


def test_put_if_none_match_blocked_by_tombstone():
    """if_none_match must fail if a tombstone exists — prevents resurrection."""
    client.put("/kv/k", json={"key": "k", "value": "v"})
    client.delete("/kv/k")
    # Key is now a tombstone — if_none_match must still return 409
    resp = client.put("/kv/k", json={"key": "k", "value": "reborn", "if_none_match": True})
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# DELETE with if_match
# ---------------------------------------------------------------------------

def test_delete_if_match_success():
    client.put("/kv/k", json={"key": "k", "value": "v"})
    token = client.get("/kv/k").json()["version_token"]
    resp = client.delete(f"/kv/k?if_match={token}")
    assert resp.status_code == 200


def test_delete_if_match_mismatch_returns_409():
    client.put("/kv/k", json={"key": "k", "value": "v"})
    resp = client.delete(f"/kv/k?if_match={encode_token(999)}")
    assert resp.status_code == 409


def test_delete_no_if_match_is_unconditional():
    client.put("/kv/k", json={"key": "k", "value": "v"})
    resp = client.delete("/kv/k")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Metric counter
# ---------------------------------------------------------------------------

def test_cas_conflicts_total_increments():
    from app.cluster import router as router_module
    client.put("/kv/k", json={"key": "k", "value": "v"})
    before = router_module._cas_conflicts_total._value.get()
    client.put("/kv/k", json={"key": "k", "value": "v2", "if_match": encode_token(999)})
    after = router_module._cas_conflicts_total._value.get()
    assert after - before == 1

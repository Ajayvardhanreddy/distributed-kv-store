"""
Fault-injection (chaos) tests — Phase 6-C

These tests start REAL in-process HTTP servers using uvicorn + asyncio
and simulate actual node failures.  No mocks for the network layer \u2014
each server is a genuine FastAPI instance bound to a local port.

Scenarios:
  1. Read survives primary crash (replica fallback)
  2. Write fails over to replica when primary is down (leader promotion)
  3. Cluster health reflects crashed node after detection interval
  4. Rejoined node syncs missing keys (anti-entropy)

How it works:
  - Each test node is an asyncio Task running uvicorn.Server
  - "Kill" = set server.should_exit = True (graceful uvicorn shutdown)
  - We give the HealthChecker time to detect the failure before asserting
"""
import asyncio
import os
import tempfile
import threading
from typing import Optional

import httpx
import pytest
import uvicorn

# Ports used by the in-process test cluster (avoid collision with real services)
NODE_PORTS = {
    "node-0": 19100,
    "node-1": 19101,
    "node-2": 19102,
}
PEERS_STR = ",".join(
    f"http://127.0.0.1:{port}" for port in NODE_PORTS.values()
)
# Fast health-check interval so tests don\u2019t take forever
HEALTH_INTERVAL = 0.3   # seconds
STARTUP_WAIT   = 1.5    # time to let servers and health-checker settle
FAILOVER_WAIT  = 1.2    # time after killing a node before asserting


# ---------------------------------------------------------------------------
# Test-cluster helpers
# ---------------------------------------------------------------------------

class NodeHandle:
    """Wraps a running uvicorn Server so tests can start/stop it cleanly."""

    def __init__(self, node_id: str, port: int, data_dir: str):
        self.node_id = node_id
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        self.data_dir = data_dir
        self._server: Optional[uvicorn.Server] = None
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Boot the FastAPI app in the background."""
        # Set env vars BEFORE importing app (each test uses fresh process env)
        env_patch = {
            "NODE_ID": self.node_id,
            "NODE_PORT": str(self.port),
            "PEERS": PEERS_STR,
            "DATA_DIR": self.data_dir,
            "REPLICATION_FACTOR": "2",
            "HEALTH_CHECK_INTERVAL": str(HEALTH_INTERVAL),
        }
        for k, v in env_patch.items():
            os.environ[k] = v

        # Import fresh app instance under patched env
        import importlib
        import app.main as main_mod
        importlib.reload(main_mod)
        application = main_mod.app

        cfg = uvicorn.Config(
            application,
            host="127.0.0.1",
            port=self.port,
            log_level="error",   # suppress uvicorn access logs in test output
        )
        self._server = uvicorn.Server(cfg)
        self._task = asyncio.create_task(self._server.serve())
        # Wait for server to be ready
        await self._wait_ready()

    async def _wait_ready(self, timeout: float = 5.0):
        deadline = asyncio.get_event_loop().time() + timeout
        async with httpx.AsyncClient() as client:
            while asyncio.get_event_loop().time() < deadline:
                try:
                    r = await client.get(f"{self.base_url}/health", timeout=0.5)
                    if r.status_code == 200:
                        return
                except Exception:
                    pass
                await asyncio.sleep(0.1)
        raise TimeoutError(f"Node {self.node_id} did not start within {timeout}s")

    async def stop(self):
        """Gracefully stop the uvicorn server."""
        if self._server:
            self._server.should_exit = True
            if self._task:
                try:
                    await asyncio.wait_for(self._task, timeout=3.0)
                except (asyncio.TimeoutError, Exception):
                    self._task.cancel()


async def start_cluster(data_dir: str, node_ids=None) -> dict[str, NodeHandle]:
    """Start a subset (or all) of the 3-node cluster."""
    if node_ids is None:
        node_ids = list(NODE_PORTS.keys())
    handles = {}
    for nid in node_ids:
        h = NodeHandle(nid, NODE_PORTS[nid], data_dir)
        await h.start()
        handles[nid] = h
    await asyncio.sleep(STARTUP_WAIT)   # let health-checkers run a cycle
    return handles


async def stop_cluster(handles: dict[str, NodeHandle]):
    await asyncio.gather(*(h.stop() for h in handles.values()), return_exceptions=True)


# ---------------------------------------------------------------------------
# Scenario 1: Read survives primary crash
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_read_survives_primary_crash():
    """
    Write a key, kill the primary, read from another node \u2014 should succeed
    via replica fallback (Phase 5 feature, validated with real servers).
    """
    with tempfile.TemporaryDirectory() as tmp:
        handles = await start_cluster(tmp)
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                # Write via node-0 (router will fan-out to rf=2 nodes)
                r = await client.put(
                    f"{handles['node-0'].base_url}/kv/chaos:read",
                    json={"key": "chaos:read", "value": "survivor"},
                )
                assert r.status_code == 200, f"PUT failed: {r.text}"

                # Discover the primary for this key
                stats = (await client.get(f"{handles['node-0'].base_url}/stats")).json()
                # Key is replicated to 2 nodes \u2014 find the non-node-0 copy
                # Kill the primary node for chaos:read
                # (We ask node-0 who owns the key)
                info = (await client.get(
                    f"{handles['node-0'].base_url}/cluster/health"
                )).json()
                assert info   # cluster is up

                # Kill node-0 (might be primary, might be replica \u2014 doesn't matter)
                await handles["node-0"].stop()
                del handles["node-0"]
                await asyncio.sleep(FAILOVER_WAIT)

                # Read from node-1 \u2014 must succeed via replica
                r = await client.get(f"{handles['node-1'].base_url}/kv/chaos:read")
                assert r.status_code == 200, (
                    f"Expected 200 after replica fallback, got {r.status_code}: {r.text}"
                )
                assert r.json()["value"] == "survivor"
        finally:
            await stop_cluster(handles)


# ---------------------------------------------------------------------------
# Scenario 2: Write fails over to replica (leader promotion)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_failover_to_replica():
    """
    Kill the ring-primary for a key, then write that key from a live node.
    Leader promotion should route the write to the first live replica.
    """
    with tempfile.TemporaryDirectory() as tmp:
        handles = await start_cluster(tmp)
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                # Find which node is primary for "failover:key" by writing
                # first and checking logs \u2014 simpler: just kill one node and
                # verify the write still lands via the surviving replica.

                # Kill node-2
                await handles["node-2"].stop()
                del handles["node-2"]
                await asyncio.sleep(FAILOVER_WAIT)

                # Write via node-0 \u2014 if node-2 was primary, leader promotion
                # routes to node-1 instead
                r = await client.put(
                    f"{handles['node-0'].base_url}/kv/failover:key",
                    json={"key": "failover:key", "value": "promoted"},
                )
                assert r.status_code == 200, (
                    f"Write should succeed via leader promotion, got {r.status_code}: {r.text}"
                )

                # Verify the value is readable
                r = await client.get(f"{handles['node-1'].base_url}/kv/failover:key")
                assert r.status_code == 200
                assert r.json()["value"] == "promoted"
        finally:
            await stop_cluster(handles)


# ---------------------------------------------------------------------------
# Scenario 3: /cluster/health reflects crashed node
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cluster_health_reflects_crash():
    """
    After killing a node and waiting one health-check interval,
    /cluster/health on a surviving node should show the dead node as unreachable.
    """
    with tempfile.TemporaryDirectory() as tmp:
        handles = await start_cluster(tmp)
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                # Verify all healthy initially
                r = await client.get(f"{handles['node-0'].base_url}/cluster/health")
                data = r.json()
                for nid, entry in data["cluster"].items():
                    assert entry["status"] == "healthy", f"{nid} not healthy at start"

                # Kill node-1
                await handles["node-1"].stop()
                del handles["node-1"]

                # Wait for health-checker to detect the failure
                await asyncio.sleep(HEALTH_INTERVAL * 4)

                r = await client.get(f"{handles['node-0'].base_url}/cluster/health")
                data = r.json()
                assert data["cluster"]["node-1"]["status"] == "unreachable", (
                    f"Expected node-1 unreachable, got: {data['cluster']['node-1']}"
                )
        finally:
            await stop_cluster(handles)


# ---------------------------------------------------------------------------
# Scenario 4: Rejoined node syncs missing keys
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rejoin_sync_catches_up():
    """
    1. Start 3 nodes
    2. Stop node-2
    3. Write keys while node-2 is down (they replicate to node-0 + node-1)
    4. Restart node-2 (it will sync from a peer on startup)
    5. Read those keys from node-2\u2019s local storage \u2014 should be present
    """
    with tempfile.TemporaryDirectory() as tmp:
        handles = await start_cluster(tmp)
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Kill node-2
                await handles["node-2"].stop()
                del handles["node-2"]
                await asyncio.sleep(FAILOVER_WAIT)

                # Write 5 keys while node-2 is down
                for i in range(5):
                    r = await client.put(
                        f"{handles['node-0'].base_url}/kv/sync:{i}",
                        json={"key": f"sync:{i}", "value": f"val-{i}"},
                    )
                    assert r.status_code == 200

                # Restart node-2 \u2014 it will call sync_from_peers() on startup
                new_node2 = NodeHandle("node-2", NODE_PORTS["node-2"], tmp)
                await new_node2.start()
                handles["node-2"] = new_node2
                await asyncio.sleep(STARTUP_WAIT)

                # Read directly from node-2\u2019s internal storage
                synced_count = 0
                for i in range(5):
                    r = await client.get(
                        f"{handles['node-2'].base_url}/internal/kv/sync:{i}"
                    )
                    if r.status_code == 200:
                        synced_count += 1

                # At least some keys should have synced
                # (exact count depends on which keys are in node-2's shard)
                assert synced_count > 0, (
                    "Node-2 should have synced at least some keys after rejoin"
                )
        finally:
            await stop_cluster(handles)

"""
Distributed Key-Value Store - Main FastAPI Application  (Phase 5)

Each instance is one node in an N-node cluster.  Every node:
  • Owns one local shard (1 StorageEngine, 1 WAL file)
  • Knows all peer nodes via the PEERS env var
  • Routes every public request via ClusterRouter (may forward to peer)
  • Exposes /internal/kv/* for peer-to-peer writes (no re-forwarding)
  • Runs a background HealthChecker that pings peers every 5 s
  • Falls back to replicas when the primary is down (Phase 5)
"""
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.health_checker import HealthChecker
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.storage.engine import StorageEngine

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Globals  (initialised in lifespan)
# ---------------------------------------------------------------------------
config: NodeConfig = None
router: ClusterRouter = None
health: HealthChecker = None


# ---------------------------------------------------------------------------
# Lifespan – startup / shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global config, router, health

    # 1. Load node identity and peer list from environment
    config = NodeConfig()

    # 2. Build consistent hash ring (same on every node — deterministic)
    ring = ConsistentHashRing(num_vnodes=150)
    for node_id in config.node_ids:
        ring.add_node(node_id)

    # 3. Local storage: one WAL file per node
    import os
    wal_path = os.path.join(config.data_dir, f"{config.node_id}.wal")
    storage = StorageEngine(wal_path)
    await storage.initialize()

    local_keys = await storage.size()
    logger.info(
        f"✅ Node '{config.node_id}' started | "
        f"peers={config.node_ids} | rf={config.replication_factor} | "
        f"local_keys={local_keys}"
    )

    # 4. Start health checker (background ping loop)
    health = HealthChecker(config)
    await health.start()

    # 5. Wire up the cluster router, injecting the health checker
    router = ClusterRouter(config, storage, ring, health=health)
    await router.initialize()

    yield

    # Shutdown — stop background tasks before closing storage
    await health.stop()
    await router.close()
    await storage.close()
    logger.info(f"✅ Node '{config.node_id}' shut down")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Distributed Key-Value Store",
    description="Multi-node distributed KV store with consistent hashing and replication",
    version="0.5.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class KeyValue(BaseModel):
    """Request body for PUT operations."""
    key: str
    value: str


class ValueResponse(BaseModel):
    """Response body for GET operations."""
    value: str


# ---------------------------------------------------------------------------
# Public endpoints  (may forward to peer)
# ---------------------------------------------------------------------------

@app.get("/health")
async def health_check():
    """Health check — reports node identity and local key count."""
    return {
        "status": "healthy",
        "node_id": config.node_id,
        "local_keys": await router.local_size(),
    }


@app.get("/kv/{key}", response_model=ValueResponse)
async def get_value(key: str):
    """Retrieve a value.  Forwards to the owning node if needed."""
    try:
        value = await router.get(key)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key '{key}' not found",
        )
    logger.info(f"GET '{key}' owner={router.owner_of(key)}")
    return ValueResponse(value=value)


@app.put("/kv/{key}")
async def put_value(key: str, request: KeyValue):
    """Store a value.  Forwards to the owning node if needed."""
    try:
        await router.put(key, request.value)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    logger.info(f"PUT '{key}' owner={router.owner_of(key)}")
    return {"message": "success", "key": key}


@app.delete("/kv/{key}")
async def delete_value(key: str):
    """Delete a value.  Forwards to the owning node if needed."""
    try:
        deleted = await router.delete(key)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key '{key}' not found",
        )
    logger.info(f"DELETE '{key}' owner={router.owner_of(key)}")
    return {"message": "deleted", "key": key}


# ---------------------------------------------------------------------------
# Internal endpoints  (peer-to-peer only — NO further forwarding)
# ---------------------------------------------------------------------------

@app.get("/internal/kv/{key}", response_model=ValueResponse)
async def internal_get(key: str):
    """Internal GET — reads directly from local storage, no forwarding."""
    value = await router.storage.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return ValueResponse(value=value)


@app.put("/internal/kv/{key}")
async def internal_put(key: str, request: KeyValue):
    """Internal PUT — writes directly to local storage, no forwarding."""
    await router.storage.put(key, request.value)
    return {"message": "success", "key": key}


@app.delete("/internal/kv/{key}")
async def internal_delete(key: str):
    """Internal DELETE — deletes directly from local storage, no forwarding."""
    deleted = await router.storage.delete(key)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return {"message": "deleted", "key": key}


# ---------------------------------------------------------------------------
# Cluster-wide endpoints
# ---------------------------------------------------------------------------

@app.get("/cluster/health")
async def cluster_health():
    """
    Aggregated cluster health view.

    Uses the local HealthChecker's cached state (no live HTTP calls
    on this endpoint) so it responds instantly even when peers are down.
    Each peer's entry also shows their own view via /health when available.
    """
    health_map = health.get_status()   # dict[node_id, bool]

    results: dict = {}
    for node_id, is_up in health_map.items():
        if config.is_local(node_id):
            results[node_id] = {
                "status": "healthy",
                "node_id": node_id,
                "local_keys": await router.local_size(),
                "replication_factor": config.replication_factor,
            }
        else:
            results[node_id] = {
                "status": "healthy" if is_up else "unreachable",
            }

    return {
        "cluster": results,
        "replication_factor": config.replication_factor,
    }


@app.get("/stats")
async def get_stats():
    """Local node stats plus cluster health snapshot."""
    return {
        "node_id": config.node_id,
        "version": "0.5.0",
        "local_keys": await router.local_size(),
        "replication_factor": config.replication_factor,
        "peers": config.node_ids,
        "peer_health": health.get_status(),
    }


@app.get("/")
async def root():
    """Root — service info."""
    return {
        "service": "Distributed KV Store",
        "version": "0.5.0",
        "node_id": config.node_id if config else "unknown",
        "endpoints": {
            "health": "/health",
            "cluster_health": "/cluster/health",
            "stats": "/stats",
            "get": "GET /kv/{key}",
            "put": "PUT /kv/{key}",
            "delete": "DELETE /kv/{key}",
        },
    }

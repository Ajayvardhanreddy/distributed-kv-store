"""
Distributed Key-Value Store - Main FastAPI Application  (Phase 3)

Each instance is one node in an N-node cluster.  Every node:
  • Owns one local shard (1 StorageEngine, 1 WAL file)
  • Knows all peer nodes via the PEERS env var
  • Routes every public request to the correct owner via ClusterRouter
  • Exposes /internal/kv/* for peer-to-peer forwarding (no re-forwarding)
"""
from contextlib import asynccontextmanager
import logging

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from app.cluster.consistent_hash import ConsistentHashRing
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


# ---------------------------------------------------------------------------
# Lifespan – startup / shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global config, router

    # 1. Load node config from environment
    config = NodeConfig()

    # 2. Build consistent hash ring with one entry per node
    ring = ConsistentHashRing(num_vnodes=150)
    for node_id in config.node_ids:
        ring.add_node(node_id)

    # 3. Create local storage (this node only owns its own shard)
    import os
    wal_path = os.path.join(config.data_dir, f"{config.node_id}.wal")
    storage = StorageEngine(wal_path)
    await storage.initialize()

    local_keys = await storage.size()
    logger.info(
        f"✅ Node '{config.node_id}' started | "
        f"peers={config.node_ids} | local_keys={local_keys}"
    )

    # 4. Wire up the cluster router
    router = ClusterRouter(config, storage, ring)
    await router.initialize()

    yield

    # Shutdown
    await router.close()
    await storage.close()
    logger.info(f"✅ Node '{config.node_id}' shut down")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Distributed Key-Value Store",
    description="Multi-node distributed KV store with consistent hashing",
    version="0.3.0",
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
    Ping every peer and report their health.
    This node's entry is filled locally; peers are called via HTTP.
    """
    results = {}
    async with httpx.AsyncClient(timeout=2.0) as client:
        for node_id, base_url in config.peers.items():
            if config.is_local(node_id):
                results[node_id] = {
                    "status": "healthy",
                    "local_keys": await router.local_size(),
                }
            else:
                try:
                    resp = await client.get(f"{base_url}/health")
                    results[node_id] = resp.json()
                except httpx.RequestError:
                    results[node_id] = {"status": "unreachable"}
    return {"cluster": results}


@app.get("/stats")
async def get_stats():
    """Local shard stats for this node."""
    return {
        "node_id": config.node_id,
        "local_keys": await router.local_size(),
        "owner_node": config.node_id,
        "peers": config.node_ids,
    }


@app.get("/")
async def root():
    """Root — service info."""
    return {
        "service": "Distributed KV Store",
        "version": "0.3.0",
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

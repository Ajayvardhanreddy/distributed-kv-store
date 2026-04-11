"""
Distributed Key-Value Store - Main FastAPI Application  (Phase 7)

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
import os

import httpx
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

    # 6. Anti-entropy: sync missing keys from a live peer on startup.
    #    This ensures a rejoining node catches up on writes it missed
    #    while it was down — closing the stale-replica gap.
    await sync_from_peers(config, storage)

    yield

    # Shutdown — stop background tasks before closing storage
    await health.stop()
    await router.close()
    await storage.close()
    logger.info(f"✅ Node '{config.node_id}' shut down")

# ---------------------------------------------------------------------------
# Anti-entropy helper — sync-on-rejoin
# ---------------------------------------------------------------------------

async def sync_from_peers(cfg: NodeConfig, storage: StorageEngine) -> None:
    """
    Pull missing keys from the first reachable peer on startup.

    Strategy (simple anti-entropy):
      1. Try each peer in order until one responds to GET /internal/sync
      2. For every key in the peer's snapshot: write it locally ONLY if
         we don't already have it (WAL replay takes precedence — a key
         we already have is guaranteed to be at least as fresh)
      3. Log how many keys were synced

    Limitation (documented as AP trade-off): if a key was written to this
    node AND the peer after this node went down, both versions exist and
    we keep ours. A sequence-number per key (vector clocks) would resolve
    this — deferred as a known limitation in the README.
    """
    peers_to_try = [
        (nid, url)
        for nid, url in cfg.peers.items()
        if not cfg.is_local(nid)
    ]
    if not peers_to_try:
        logger.info("Sync-on-rejoin: single-node cluster, nothing to sync")
        return

    async with httpx.AsyncClient(timeout=3.0) as client:
        for node_id, base_url in peers_to_try:
            try:
                resp = await client.get(f"{base_url}/internal/sync")
                if resp.status_code != 200:
                    continue
                peer_data: dict[str, str] = resp.json().get("keys", {})
                synced = 0
                for key, value in peer_data.items():
                    if await storage.get(key) is None:   # only fill gaps
                        await storage.put(key, value)
                        synced += 1
                logger.info(
                    f"Sync-on-rejoin: pulled {synced}/{len(peer_data)} "
                    f"keys from {node_id}"
                )
                return   # one successful sync is enough
            except httpx.RequestError:
                logger.warning(f"Sync-on-rejoin: {node_id} unreachable, trying next")

    logger.warning("Sync-on-rejoin: no peers reachable, starting with local WAL state only")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Distributed Key-Value Store",
    description="Multi-node distributed KV store with consistent hashing and replication",
    version="0.7.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Prometheus metrics  (auto-instruments all endpoints → /metrics)
# ---------------------------------------------------------------------------
try:
    from prometheus_fastapi_instrumentator import Instrumentator
    Instrumentator(
        should_group_status_codes=False,
        excluded_handlers=["/metrics"],
    ).instrument(app).expose(app, include_in_schema=False)
    logger.info("Prometheus metrics enabled at /metrics")
except ImportError:
    logger.warning("prometheus-fastapi-instrumentator not installed — /metrics disabled")


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


@app.get("/internal/sync")
async def internal_sync():
    """
    Anti-entropy sync endpoint.

    Returns a full snapshot of this node's local key-value store.
    Called by a rejoining peer during startup to catch up on missed writes.
    The caller will merge this snapshot with its WAL-replayed state,
    taking ours only for keys it doesn't already have.
    """
    snapshot = await router.storage.snapshot()
    return {"node_id": config.node_id, "keys": snapshot}


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

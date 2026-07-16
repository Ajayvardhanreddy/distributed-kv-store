"""
Distributed Key-Value Store — FastAPI application (Phase 2: Tombstone DELETEs)

Each node:
  • Owns one local StorageEngine + WAL file
  • Routes all public requests via ClusterRouter
  • Exposes /internal/* endpoints for peer-to-peer writes (no re-forwarding)
  • Runs a HealthChecker background loop (peer heartbeats)
  • Runs a Compaction background loop (physical tombstone removal)
  • Performs sync-on-rejoin at startup (pulls missing keys AND tombstones from peers)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.health_checker import HealthChecker
from app.cluster.node_config import NodeConfig
from app.cluster.router import ClusterRouter
from app.storage.engine import StorageEngine
from app.storage.version_token import CASConflictError, encode_token, version_matches

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Globals (initialised in lifespan)
# ---------------------------------------------------------------------------
config: NodeConfig = None
router: ClusterRouter = None
health: HealthChecker = None

# Background task intervals
_COMPACT_INTERVAL_SECONDS = 300.0   # 5 minutes
_SWEEPER_INTERVAL_SECONDS = 60.0    # 1 minute
_SWEEPER_SAMPLE_SIZE = 100          # keys sampled per run


# ---------------------------------------------------------------------------
# Background loops
# ---------------------------------------------------------------------------

async def _compact_loop(storage: StorageEngine, interval: float) -> None:
    """Periodically remove tombstones that have passed tombstone_expires_at."""
    try:
        while True:
            await asyncio.sleep(interval)
            removed = await storage.compact()
            if removed:
                logger.info(f"Compaction: removed {removed} expired tombstones")
    except asyncio.CancelledError:
        pass


async def _sweeper_loop(
    kv_router: "ClusterRouter",
    interval: float,
    sample_size: int,
) -> None:
    """
    Active TTL sweeper — samples keys, tombstones expired ones via router.delete()
    so the tombstone fans out to all replicas (Phase 2 contract).
    """
    from app.storage.engine import _keys_expired_total, _sweeper_runs_total
    try:
        while True:
            await asyncio.sleep(interval)
            _sweeper_runs_total.inc()
            expired = await kv_router.storage.get_expired_keys(sample_size)
            for key in expired:
                try:
                    deleted = await kv_router.delete(key)
                    if deleted:
                        _keys_expired_total.inc()
                        logger.debug(f"Sweeper: tombstoned expired key '{key}'")
                except RuntimeError as exc:
                    logger.warning(f"Sweeper: could not tombstone '{key}': {exc}")
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global config, router, health

    config = NodeConfig()

    ring = ConsistentHashRing(num_vnodes=150)
    for node_id in config.node_ids:
        ring.add_node(node_id)

    wal_path = os.path.join(config.data_dir, f"{config.node_id}.wal")
    storage = StorageEngine(
        wal_path,
        durability=config.durability,
        tombstone_retention_seconds=config.tombstone_retention_seconds,
    )
    await storage.initialize()
    await storage.wal.start_flush_loop()

    live = await storage.size()
    logger.info(
        f"✅ Node '{config.node_id}' started | peers={config.node_ids} | "
        f"rf={config.replication_factor} | live_keys={live}"
    )

    health = HealthChecker(config)
    await health.start()

    router = ClusterRouter(config, storage, ring, health=health)
    await router.initialize()

    await sync_from_peers(config, storage)

    # Start background tasks
    compact_task = asyncio.create_task(
        _compact_loop(storage, _COMPACT_INTERVAL_SECONDS)
    )
    sweeper_task = asyncio.create_task(
        _sweeper_loop(router, _SWEEPER_INTERVAL_SECONDS, _SWEEPER_SAMPLE_SIZE)
    )

    yield

    sweeper_task.cancel()
    compact_task.cancel()
    for task in (sweeper_task, compact_task):
        try:
            await task
        except asyncio.CancelledError:
            pass

    await health.stop()
    await router.close()
    await storage.close()
    logger.info(f"✅ Node '{config.node_id}' shut down")


# ---------------------------------------------------------------------------
# Anti-entropy — sync-on-rejoin
# ---------------------------------------------------------------------------

async def sync_from_peers(cfg: NodeConfig, storage: StorageEngine) -> None:
    """
    Pull missing or stale keys (and tombstones) from the first reachable peer.

    Merge rules (version wins):
      - peer_version > local_version AND peer entry is live  → put_versioned()
      - peer_version > local_version AND peer entry is tombstone → put_tombstone()
      - peer_version <= local_version → keep ours (already up-to-date)

    Carrying tombstones prevents key resurrection: if a key was deleted while
    this node was offline, the tombstone will suppress the stale WAL value.
    """
    peers_to_try = [
        (nid, url) for nid, url in cfg.peers.items() if not cfg.is_local(nid)
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
                peer_data: dict = resp.json().get("keys", {})
                synced = 0

                for key, entry in peer_data.items():
                    if not isinstance(entry, dict):
                        continue

                    peer_version = entry.get("version", 1)
                    local_version = await storage.get_version(key)

                    if peer_version <= local_version:
                        continue  # our version is at least as recent

                    if entry.get("deleted"):
                        expires = entry.get("tombstone_expires_at", time.time() + 86400)
                        await storage.put_tombstone(key, peer_version, expires)
                    else:
                        # Carry expires_at so this node expires at the same time as the peer
                        await storage.put_versioned(
                            key, entry["value"], peer_version,
                            expires_at=entry.get("expires_at"),
                        )
                    synced += 1

                logger.info(
                    f"Sync-on-rejoin: pulled {synced}/{len(peer_data)} "
                    f"entries from {node_id}"
                )
                return
            except httpx.RequestError:
                logger.warning(f"Sync-on-rejoin: {node_id} unreachable, trying next")

    logger.warning("Sync-on-rejoin: no peers reachable, starting with local WAL state only")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Distributed Key-Value Store",
    description="Multi-node distributed KV store with consistent hashing and replication",
    version="0.9.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Prometheus metrics
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

try:
    from prometheus_client import Counter
    _tombstones_compacted = Counter(
        "tombstones_compacted_total",
        "Tombstones physically removed by the compaction loop",
    )
except ImportError:
    class _Stub:
        def inc(self, n=1): pass
        def set(self, v): pass
    _tombstones_compacted = _Stub()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class KeyValue(BaseModel):
    key: str
    value: str
    version: Optional[int] = None
    ttl_seconds: Optional[int] = None     # public PUT — converted to expires_at by router
    expires_at: Optional[float] = None    # internal fan-out — absolute epoch seconds
    if_match: Optional[str] = None        # CAS: token from prior GET
    if_none_match: Optional[bool] = False # CAS: create-if-absent


class TombstoneRequest(BaseModel):
    version: int
    tombstone_expires_at: float


# ---------------------------------------------------------------------------
# Public endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "node_id": config.node_id,
        "local_keys": await router.local_size(),
    }


@app.get("/kv/{key}")
async def get_value(key: str):
    try:
        value, version = await router.get(key)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    if value is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Key '{key}' not found")
    logger.info(f"GET '{key}' ver={version}")
    # Phase 4: return opaque version_token only (raw int deprecated)
    return {"value": value, "version_token": encode_token(version)}


@app.put("/kv/{key}")
async def put_value(key: str, request: KeyValue):
    try:
        version = await router.put(
            key, request.value,
            ttl_seconds=request.ttl_seconds,
            if_match=request.if_match,
            if_none_match=bool(request.if_none_match),
        )
    except CASConflictError as e:
        raise HTTPException(
            status_code=409,
            detail={"message": "version mismatch", "current_token": e.current_token},
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    logger.info(f"PUT '{key}' ver={version}")
    return {"message": "success", "key": key, "version_token": encode_token(version)}


@app.delete("/kv/{key}")
async def delete_value(key: str, if_match: Optional[str] = None):
    try:
        deleted = await router.delete(key, if_match=if_match)
    except CASConflictError as e:
        raise HTTPException(
            status_code=409,
            detail={"message": "version mismatch", "current_token": e.current_token},
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Key '{key}' not found")
    logger.info(f"DELETE '{key}' (tombstone written)")
    return {"message": "deleted", "key": key}


# ---------------------------------------------------------------------------
# Internal endpoints (peer-to-peer — no further forwarding)
# ---------------------------------------------------------------------------

@app.get("/internal/kv/{key}")
async def internal_get(key: str):
    """Internal GET — reads directly from local storage."""
    value, version = await router.storage.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return {"value": value, "version": version}


@app.put("/internal/kv/{key}")
async def internal_put(key: str, request: KeyValue):
    """
    Internal PUT — writes directly to local storage (leader or replica path).

    version set     → replica fan-out: put_versioned, no CAS check
    version absent  → leader path: CAS check if if_match / if_none_match present
    """
    if request.version is not None:
        # Replica path — apply the leader's version, no CAS
        await router.storage.put_versioned(
            key, request.value, request.version, expires_at=request.expires_at
        )
        return {"message": "success", "key": key, "version": request.version}

    # Leader path — apply CAS if requested
    if request.if_match is not None or request.if_none_match:
        _, current_ver = await router.storage.get(key)
        raw_entry = router.storage.store.get(key)
        key_present = raw_entry is not None
        if request.if_none_match and key_present:
            from app.storage.version_token import encode_token as _enc
            raise HTTPException(
                status_code=409,
                detail={"message": "version mismatch", "current_token": _enc(current_ver)},
            )
        if request.if_match is not None and not version_matches(current_ver, request.if_match):
            raise HTTPException(
                status_code=409,
                detail={"message": "version mismatch", "current_token": encode_token(current_ver)},
            )

    version = await router.storage.put(
        key, request.value, expires_at=request.expires_at
    )
    return {"message": "success", "key": key, "version": version}


@app.delete("/internal/kv/{key}")
async def internal_delete(key: str):
    """
    Internal DELETE — writes a tombstone to local storage (leader path).

    Returns version and tombstone_expires_at so the calling router can
    fan-out the exact same tombstone to replicas.
    """
    deleted = await router.storage.delete(key)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")

    entry = router.storage.store.get(key, {})
    return {
        "message": "deleted",
        "key": key,
        "version": entry.get("version"),
        "tombstone_expires_at": entry.get("tombstone_expires_at"),
    }


@app.post("/internal/tombstone/{key}")
async def internal_tombstone(key: str, request: TombstoneRequest):
    """
    Apply a tombstone received from the write-leader (replica fan-out path).

    The version and tombstone_expires_at come from the leader so all replicas
    store identical tombstones.
    """
    await router.storage.put_tombstone(
        key, request.version, request.tombstone_expires_at
    )
    return {"message": "tombstone applied", "key": key, "version": request.version}


@app.get("/internal/sync")
async def internal_sync():
    """
    Anti-entropy endpoint — returns full local snapshot including tombstones.

    Tombstones are included so rejoining peers can learn about deletions and
    cannot resurrect keys that were removed while they were offline.
    """
    snapshot = await router.storage.snapshot()
    return {"node_id": config.node_id, "keys": snapshot}


# ---------------------------------------------------------------------------
# Cluster-wide endpoints
# ---------------------------------------------------------------------------

@app.get("/cluster/health")
async def cluster_health():
    health_map = health.get_status()
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
            results[node_id] = {"status": "healthy" if is_up else "unreachable"}
    return {"cluster": results, "replication_factor": config.replication_factor}


@app.get("/stats")
async def get_stats():
    return {
        "node_id": config.node_id,
        "version": "0.9.0",
        "local_keys": await router.local_size(),
        "replication_factor": config.replication_factor,
        "peers": config.node_ids,
        "peer_health": health.get_status(),
    }


@app.get("/")
async def root():
    return {
        "service": "Distributed KV Store",
        "version": "0.9.0",
        "node_id": config.node_id if config else "unknown",
        "endpoints": {
            "health": "/health",
            "cluster_health": "/cluster/health",
            "get": "GET /kv/{key}",
            "put": "PUT /kv/{key}",
            "delete": "DELETE /kv/{key}",
        },
    }

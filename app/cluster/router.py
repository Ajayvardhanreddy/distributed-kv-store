"""
Cluster-aware request router.

ClusterRouter wraps a local StorageEngine and a ConsistentHashRing.
For every operation it determines the owner node:

  - If owner == this node  → handle locally (fast path, no network)
  - If owner != this node  → forward the request via HTTP to that peer

The forwarding target is always the /internal/kv/ endpoint, which
handles the request directly without further forwarding (prevents loops).
"""
import logging
from typing import Optional

import httpx

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.node_config import NodeConfig
from app.storage.engine import StorageEngine

logger = logging.getLogger(__name__)

# Timeout for peer-to-peer HTTP calls (seconds)
FORWARD_TIMEOUT = 2.0


class ClusterRouter:
    """
    Routes KV operations to the correct node using consistent hashing.

    Usage:
        router = ClusterRouter(config, storage, ring)
        await router.put("user:1", "alice")   # auto-forwarded if needed
        value = await router.get("user:1")    # same
    """

    def __init__(
        self,
        config: NodeConfig,
        storage: StorageEngine,
        ring: ConsistentHashRing,
    ):
        self.config = config
        self.storage = storage
        self.ring = ring
        # Shared async HTTP client; created in initialize(), closed in close()
        self._client: Optional[httpx.AsyncClient] = None

    async def initialize(self) -> None:
        """Open the shared HTTP client."""
        self._client = httpx.AsyncClient(timeout=FORWARD_TIMEOUT)
        logger.info("ClusterRouter HTTP client initialized")

    async def close(self) -> None:
        """Close the shared HTTP client."""
        if self._client:
            await self._client.aclose()
            logger.info("ClusterRouter HTTP client closed")

    # ------------------------------------------------------------------
    # Public API (same shape as StorageEngine so main.py is unchanged)
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[str]:
        owner = self.ring.get_node(key)
        if self.config.is_local(owner):
            return await self.storage.get(key)
        return await self._forward_get(owner, key)

    async def put(self, key: str, value: str) -> None:
        owner = self.ring.get_node(key)
        if self.config.is_local(owner):
            await self.storage.put(key, value)
        else:
            await self._forward_put(owner, key, value)

    async def delete(self, key: str) -> bool:
        owner = self.ring.get_node(key)
        if self.config.is_local(owner):
            return await self.storage.delete(key)
        return await self._forward_delete(owner, key)

    async def exists(self, key: str) -> bool:
        value = await self.get(key)
        return value is not None

    async def local_size(self) -> int:
        """Number of keys stored on THIS node only."""
        return await self.storage.size()

    def owner_of(self, key: str) -> str:
        """Return the node_id that owns a key (useful for debugging)."""
        return self.ring.get_node(key)

    # ------------------------------------------------------------------
    # Internal HTTP forwarding helpers
    # ------------------------------------------------------------------

    def _internal_url(self, node_id: str, key: str) -> str:
        base = self.config.peer_url(node_id)
        return f"{base}/internal/kv/{key}"

    async def _forward_get(self, node_id: str, key: str) -> Optional[str]:
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding GET {key} → {node_id} ({url})")
        try:
            resp = await self._client.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()["value"]
        except httpx.RequestError as e:
            logger.error(f"Forward GET failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_put(self, node_id: str, key: str, value: str) -> None:
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding PUT {key} → {node_id} ({url})")
        try:
            resp = await self._client.put(url, json={"key": key, "value": value})
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"Forward PUT failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_delete(self, node_id: str, key: str) -> bool:
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding DELETE {key} → {node_id} ({url})")
        try:
            resp = await self._client.delete(url)
            if resp.status_code == 404:
                return False
            resp.raise_for_status()
            return True
        except httpx.RequestError as e:
            logger.error(f"Forward DELETE failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

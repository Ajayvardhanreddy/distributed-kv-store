"""
Cluster-aware request router — Phase 5: Replica Read Fallback

Write path (Phase 4): fan-out PUT/DELETE to ALL N nodes in parallel.
Read path  (Phase 5): try nodes in replication-set order, skipping any
  the HealthChecker has marked down.  Falls back to replicas when the
  primary is unavailable — AP trade-off from the CAP theorem.
"""
import asyncio
import logging
from typing import TYPE_CHECKING, Optional

import httpx

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.node_config import NodeConfig
from app.storage.engine import StorageEngine

if TYPE_CHECKING:
    from app.cluster.health_checker import HealthChecker

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
        health: Optional["HealthChecker"] = None,
    ):
        self.config = config
        self.storage = storage
        self.ring = ring
        self.replication_factor: int = config.replication_factor
        # HealthChecker: injected at startup (None → treat all nodes healthy)
        self.health = health
        self._client: Optional[httpx.AsyncClient] = None

    async def initialize(self) -> None:
        """Open the shared HTTP client."""
        self._client = httpx.AsyncClient(timeout=FORWARD_TIMEOUT)
        logger.info(
            f"ClusterRouter ready (rf={self.replication_factor}, "
            f"health_checker={'on' if self.health else 'off'})"
        )

    async def close(self) -> None:
        """Close the shared HTTP client."""
        if self._client:
            await self._client.aclose()
            logger.info("ClusterRouter HTTP client closed")

    # ------------------------------------------------------------------
    # Public API (same shape as StorageEngine so main.py is unchanged)
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[str]:
        """
        Read with replica fallback.

        Tries each node in the replication set in order (primary first).
        Skips nodes the HealthChecker has already marked down.
        On a network error, marks the node down immediately and tries the next.
        Raises RuntimeError only if every replica in the set is exhausted.
        """
        candidates = self.ring.get_nodes(key, n=self.replication_factor)

        for node_id in candidates:
            # Skip nodes we already know are down
            if self.health and not self.health.is_healthy(node_id):
                logger.debug(f"GET '{key}': skipping {node_id} (marked down)")
                continue

            try:
                if self.config.is_local(node_id):
                    return await self.storage.get(key)
                return await self._forward_get(node_id, key)
            except RuntimeError:
                # Network failure — mark down immediately, try next replica
                if self.health:
                    self.health.mark_down(node_id)
                logger.warning(f"GET '{key}': {node_id} failed, trying next replica")

        raise RuntimeError(f"All replicas unreachable for key '{key}'") from None

    async def put(self, key: str, value: str) -> None:
        """
        Write to ALL nodes in the replication set in parallel.

        Uses asyncio.gather so every HTTP call fires concurrently.
        All must succeed; any failure raises RuntimeError → HTTP 503.
        """
        owners = self.ring.get_nodes(key, n=self.replication_factor)
        logger.info(f"PUT '{key}' replication_set={owners}")
        await self._write_all(owners, key, value, op="put")

    async def delete(self, key: str) -> bool:
        """
        Delete from ALL nodes in the replication set.

        Returns True if the key existed on the primary.
        """
        owners = self.ring.get_nodes(key, n=self.replication_factor)
        primary = owners[0]

        # Check existence on primary before fanning out
        if self.config.is_local(primary):
            if not await self.storage.exists(key):
                return False
        else:
            primary_val = await self._forward_get(primary, key)
            if primary_val is None:
                return False

        logger.info(f"DELETE '{key}' replication_set={owners}")
        await self._write_all(owners, key, value=None, op="delete")
        return True

    async def exists(self, key: str) -> bool:
        value = await self.get(key)
        return value is not None

    async def local_size(self) -> int:
        """Number of keys stored on THIS node only."""
        return await self.storage.size()

    def owner_of(self, key: str) -> str:
        """Primary node for a key."""
        return self.ring.get_node(key)

    def replicas_of(self, key: str) -> list[str]:
        """Full replication set [primary, replica-1, ...] for a key."""
        return self.ring.get_nodes(key, n=self.replication_factor)

    # ------------------------------------------------------------------
    # Fan-out helper
    # ------------------------------------------------------------------

    async def _write_all(
        self, nodes: list[str], key: str, value: Optional[str], op: str
    ) -> None:
        """
        Fan-out a PUT or DELETE to every node in `nodes` concurrently.

        asyncio.gather fires all coroutines at the same time and waits
        for ALL of them. First exception cancels the rest and propagates.
        """
        tasks = []
        for node_id in nodes:
            if self.config.is_local(node_id):
                if op == "put":
                    tasks.append(self.storage.put(key, value))
                else:
                    tasks.append(self.storage.delete(key))
            else:
                if op == "put":
                    tasks.append(self._forward_put(node_id, key, value))
                else:
                    tasks.append(self._forward_delete_replica(node_id, key))
        await asyncio.gather(*tasks)

    # ------------------------------------------------------------------
    # Internal HTTP forwarding helpers
    # ------------------------------------------------------------------

    def _internal_url(self, node_id: str, key: str) -> str:
        base = self.config.peer_url(node_id)
        return f"{base}/internal/kv/{key}"

    async def _forward_get(self, node_id: str, key: str) -> Optional[str]:
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding GET {key} → {node_id}")
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
        logger.debug(f"Forwarding PUT {key} → {node_id}")
        try:
            resp = await self._client.put(url, json={"key": key, "value": value})
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"Forward PUT failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_delete(self, node_id: str, key: str) -> bool:
        """Delete from a remote node; used for the primary-only path."""
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding DELETE {key} → {node_id}")
        try:
            resp = await self._client.delete(url)
            if resp.status_code == 404:
                return False
            resp.raise_for_status()
            return True
        except httpx.RequestError as e:
            logger.error(f"Forward DELETE failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_delete_replica(self, node_id: str, key: str) -> None:
        """
        Delete from a replica node during fan-out.
        404 is treated as OK (replica may never have had the key).
        """
        url = self._internal_url(node_id, key)
        try:
            resp = await self._client.delete(url)
            if resp.status_code not in (200, 404):
                resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"Replica DELETE failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

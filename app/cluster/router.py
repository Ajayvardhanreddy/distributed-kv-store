"""
Cluster-aware request router — Phase 8: Versioned Writes

Read  path (Phase 5): try replication set in order, skip down nodes.
Write path (Phase 6): route to the first HEALTHY node in the replication
  set — the "write-leader".  If the ring's original primary is down, the
  first live replica is automatically promoted as the new write target.

This gives automatic leader failover without Raft.  The trade-off:
  Two nodes may briefly disagree on leader identity if their HealthChecker
  views diverge — a split-brain window of at most one check-interval (~5 s).
  Raft would close this window but adds significant complexity.  We document
  this as an explicit AP choice (see README).
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

    async def get(self, key: str) -> tuple[Optional[str], int]:
        """
        Read with replica fallback.  Returns (value, version).

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

    async def put(self, key: str, value: str) -> int:
        """
        Write to the current write-leader + all other healthy replicas.
        Returns the new version number.

        The write-leader decides the version (auto-increment on its
        local StorageEngine.put()).  Replicas receive the same version
        via put_versioned() so all copies are consistent.
        """
        owners = self.ring.get_nodes(key, n=self.replication_factor)
        leader = self._write_leader(owners)
        if leader is None:
            raise RuntimeError(f"No healthy node available for key '{key}'")

        # Step 1: write to leader → get the authoritative version
        if self.config.is_local(leader):
            version = await self.storage.put(key, value)
        else:
            version = await self._forward_put(leader, key, value)

        # Step 2: fan-out to remaining healthy replicas with that version
        other_healthy = [
            n for n in owners
            if n != leader and (self.health is None or self.health.is_healthy(n))
        ]
        if other_healthy:
            await self._replicate(other_healthy, key, value, version)

        logger.info(f"PUT '{key}' ver={version} leader={leader}")
        return version

    async def delete(self, key: str) -> bool:
        """
        Delete from all healthy nodes in the replication set.

        Existence is checked on the write-leader (not necessarily the
        original ring primary).  Returns True only if the key existed.
        """
        owners = self.ring.get_nodes(key, n=self.replication_factor)
        leader = self._write_leader(owners)
        if leader is None:
            raise RuntimeError(f"No healthy node available for key '{key}'")

        # Check existence on the current write-leader
        if self.config.is_local(leader):
            if not await self.storage.exists(key):
                return False
        else:
            value, _ = await self._forward_get(leader, key)
            if value is None:
                return False

        healthy_owners = [
            n for n in owners
            if self.health is None or self.health.is_healthy(n)
        ]
        logger.info(f"DELETE '{key}' leader={leader} healthy_replicas={healthy_owners}")
        await self._delete_all(healthy_owners, key)
        return True

    async def exists(self, key: str) -> bool:
        value, _ = await self.get(key)
        return value is not None

    async def local_size(self) -> int:
        """Number of keys stored on THIS node only."""
        return await self.storage.size()

    def owner_of(self, key: str) -> str:
        """Ring-assigned primary for a key (may be down; use write_leader_of() for routing)."""
        return self.ring.get_node(key)

    def write_leader_of(self, key: str) -> Optional[str]:
        """Current write-leader for a key — first healthy node in replication set."""
        return self._write_leader(self.ring.get_nodes(key, n=self.replication_factor))

    def replicas_of(self, key: str) -> list[str]:
        """Full replication set [primary, replica-1, ...] for a key."""
        return self.ring.get_nodes(key, n=self.replication_factor)

    # ------------------------------------------------------------------
    # Leader selection
    # ------------------------------------------------------------------

    def _write_leader(self, ordered_nodes: list[str]) -> Optional[str]:
        """
        Return the first healthy node from an ordered candidate list.

        This is the write-leader: the node that will coordinate the write.
        If HealthChecker is not configured, the first node always wins
        (original Phase 3/4 behaviour, all nodes assumed healthy).
        """
        for node_id in ordered_nodes:
            if self.health is None or self.health.is_healthy(node_id):
                return node_id
        return None   # all candidates are down

    # ------------------------------------------------------------------
    # Fan-out helper
    # ------------------------------------------------------------------

    async def _replicate(
        self, nodes: list[str], key: str, value: str, version: int
    ) -> None:
        """
        Replicate a versioned PUT to all given nodes concurrently.
        Replicas use put_versioned() to accept the leader's version.
        """
        tasks = []
        for node_id in nodes:
            if self.config.is_local(node_id):
                tasks.append(self.storage.put_versioned(key, value, version))
            else:
                tasks.append(self._forward_put_versioned(node_id, key, value, version))
        await asyncio.gather(*tasks)

    async def _delete_all(self, nodes: list[str], key: str) -> None:
        """Fan-out a DELETE to every node in `nodes` concurrently."""
        tasks = []
        for node_id in nodes:
            if self.config.is_local(node_id):
                tasks.append(self.storage.delete(key))
            else:
                tasks.append(self._forward_delete_replica(node_id, key))
        await asyncio.gather(*tasks)

    # ------------------------------------------------------------------
    # Internal HTTP forwarding helpers
    # ------------------------------------------------------------------

    def _internal_url(self, node_id: str, key: str) -> str:
        base = self.config.peer_url(node_id)
        return f"{base}/internal/kv/{key}"

    async def _forward_get(self, node_id: str, key: str) -> tuple[Optional[str], int]:
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding GET {key} → {node_id}")
        try:
            resp = await self._client.get(url)
            if resp.status_code == 404:
                return None, 0
            resp.raise_for_status()
            data = resp.json()
            return data["value"], data.get("version", 0)
        except httpx.RequestError as e:
            logger.error(f"Forward GET failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_put(self, node_id: str, key: str, value: str) -> int:
        """Forward PUT to leader node; returns the version assigned by the leader."""
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding PUT {key} → {node_id}")
        try:
            resp = await self._client.put(url, json={"key": key, "value": value})
            resp.raise_for_status()
            return resp.json().get("version", 1)
        except httpx.RequestError as e:
            logger.error(f"Forward PUT failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_put_versioned(
        self, node_id: str, key: str, value: str, version: int
    ) -> None:
        """Forward a versioned PUT to a replica node."""
        url = self._internal_url(node_id, key)
        try:
            resp = await self._client.put(
                url, json={"key": key, "value": value, "version": version}
            )
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"Forward PUT_VERSIONED failed for {key} → {node_id}: {e}")
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

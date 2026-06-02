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
import time
from typing import TYPE_CHECKING, Optional

import httpx

from app.cluster.consistent_hash import ConsistentHashRing
from app.cluster.node_config import NodeConfig
from app.storage.engine import StorageEngine
from app.storage.version_token import CASConflictError, version_matches

if TYPE_CHECKING:
    from app.cluster.health_checker import HealthChecker

logger = logging.getLogger(__name__)

# Timeout for peer-to-peer HTTP calls (seconds)
FORWARD_TIMEOUT = 2.0

# ---------------------------------------------------------------------------
# Prometheus counter
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter
    _cas_conflicts_total = Counter(
        "cas_conflicts_total",
        "Conditional write (CAS) failures due to version mismatch",
    )
except ImportError:
    class _Stub:
        class _val:
            def get(self): return 0.0
        _value = _val()
        def inc(self, n=1): ...
    _cas_conflicts_total = _Stub()


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

    async def put(
        self,
        key: str,
        value: str,
        ttl_seconds: Optional[int] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False,
    ) -> int:
        """
        Write to the write-leader + all healthy replicas.  Returns new version.

        ttl_seconds   : sets expires_at on the leader; fanned out unchanged.
        if_match      : token from a prior GET; fails with CASConflictError if stale.
        if_none_match : fails with CASConflictError if key exists or is tombstoned.

        Replicas receive put_versioned() — no CAS re-check on the replica path.
        """
        expires_at: Optional[float] = (
            time.time() + ttl_seconds if ttl_seconds is not None else None
        )

        owners = self.ring.get_nodes(key, n=self.replication_factor)
        leader = self._write_leader(owners)
        if leader is None:
            raise RuntimeError(f"No healthy node available for key '{key}'")

        # CAS check on local leader
        if self.config.is_local(leader):
            if if_none_match or if_match is not None:
                _, current_ver = await self.storage.get(key)
                # For if_none_match we also check tombstones
                raw_entry = self.storage.store.get(key)
                key_present = raw_entry is not None
                if if_none_match and key_present:
                    _cas_conflicts_total.inc()
                    raise CASConflictError(current_ver)
                if if_match is not None and not version_matches(current_ver, if_match):
                    _cas_conflicts_total.inc()
                    raise CASConflictError(current_ver)
            version = await self.storage.put(key, value, expires_at=expires_at)
        else:
            version = await self._forward_put(
                leader, key, value,
                expires_at=expires_at,
                if_match=if_match,
                if_none_match=if_none_match,
            )

        other_healthy = [
            n for n in owners
            if n != leader and (self.health is None or self.health.is_healthy(n))
        ]
        if other_healthy:
            await self._replicate(other_healthy, key, value, version, expires_at=expires_at)

        logger.info(f"PUT '{key}' ver={version} leader={leader}")
        return version

    async def delete(self, key: str, if_match: Optional[str] = None) -> bool:
        """
        Write a tombstone on the write-leader then fan-out to healthy replicas.

        All replicas receive the same (version, tombstone_expires_at) so every
        copy is identical — prevents version skew during sync-on-rejoin.
        Returns True if the key existed (was live), False otherwise.
        """
        owners = self.ring.get_nodes(key, n=self.replication_factor)
        leader = self._write_leader(owners)
        if leader is None:
            raise RuntimeError(f"No healthy node available for key '{key}'")

        # CAS check on local leader before writing tombstone
        if self.config.is_local(leader) and if_match is not None:
            _, current_ver = await self.storage.get(key)
            if not version_matches(current_ver, if_match):
                _cas_conflicts_total.inc()
                raise CASConflictError(current_ver)

        # Step 1: write tombstone on the leader → get the authoritative version
        if self.config.is_local(leader):
            existed = await self.storage.delete(key)
            if not existed:
                return False
            # Read the tombstone data the engine just wrote
            ts_entry = self.storage.store.get(key, {})
            version = ts_entry["version"]
            tombstone_expires_at = ts_entry["tombstone_expires_at"]
        else:
            result = await self._forward_delete(leader, key)
            if result is None:
                return False
            version = result["version"]
            tombstone_expires_at = result["tombstone_expires_at"]

        # Step 2: fan-out tombstone (with leader's version) to other live replicas
        other_healthy = [
            n for n in owners
            if n != leader and (self.health is None or self.health.is_healthy(n))
        ]
        if other_healthy:
            await self._replicate_tombstone(other_healthy, key, version, tombstone_expires_at)

        logger.info(f"DELETE '{key}' tombstone ver={version} leader={leader}")
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
        self, nodes: list[str], key: str, value: str, version: int,
        expires_at: Optional[float] = None,
    ) -> None:
        """Replicate a versioned PUT (with optional TTL) to all given nodes concurrently."""
        tasks = []
        for node_id in nodes:
            if self.config.is_local(node_id):
                tasks.append(
                    self.storage.put_versioned(key, value, version, expires_at=expires_at)
                )
            else:
                tasks.append(
                    self._forward_put_versioned(node_id, key, value, version, expires_at=expires_at)
                )
        await asyncio.gather(*tasks)

    async def _replicate_tombstone(
        self, nodes: list[str], key: str, version: int, tombstone_expires_at: float
    ) -> None:
        """Fan-out a tombstone (with leader-assigned version) to replica nodes."""
        tasks = []
        for node_id in nodes:
            if self.config.is_local(node_id):
                tasks.append(
                    self.storage.put_tombstone(key, version, tombstone_expires_at)
                )
            else:
                tasks.append(
                    self._forward_tombstone(node_id, key, version, tombstone_expires_at)
                )
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

    async def _forward_put(
        self, node_id: str, key: str, value: str,
        expires_at: Optional[float] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False,
    ) -> int:
        """Forward PUT to leader node; returns the version assigned by the leader."""
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding PUT {key} → {node_id}")
        body: dict = {"key": key, "value": value}
        if expires_at is not None:
            body["expires_at"] = expires_at
        if if_match is not None:
            body["if_match"] = if_match
        if if_none_match:
            body["if_none_match"] = True
        try:
            resp = await self._client.put(url, json=body)
            if resp.status_code == 409:
                data = resp.json()
                current_token = data.get("detail", {}).get("current_token", "0")
                from app.storage.version_token import decode_token
                raise CASConflictError(decode_token(current_token))
            resp.raise_for_status()
            return resp.json().get("version", 1)
        except httpx.RequestError as e:
            logger.error(f"Forward PUT failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_put_versioned(
        self, node_id: str, key: str, value: str, version: int,
        expires_at: Optional[float] = None,
    ) -> None:
        """Forward a versioned PUT (with optional TTL) to a replica node."""
        url = self._internal_url(node_id, key)
        body: dict = {"key": key, "value": value, "version": version}
        if expires_at is not None:
            body["expires_at"] = expires_at
        try:
            resp = await self._client.put(url, json=body)
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"Forward PUT_VERSIONED failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_delete(self, node_id: str, key: str) -> Optional[dict]:
        """
        Forward DELETE to the leader node.

        Returns {"version": int, "tombstone_expires_at": float} on success,
        None if the key did not exist on the leader (404).
        """
        url = self._internal_url(node_id, key)
        logger.debug(f"Forwarding DELETE {key} → {node_id}")
        try:
            resp = await self._client.delete(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            return {
                "version": data["version"],
                "tombstone_expires_at": data["tombstone_expires_at"],
            }
        except httpx.RequestError as e:
            logger.error(f"Forward DELETE failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

    async def _forward_tombstone(
        self, node_id: str, key: str, version: int, tombstone_expires_at: float
    ) -> None:
        """Apply a tombstone on a replica node (fan-out path)."""
        base = self.config.peer_url(node_id)
        url = f"{base}/internal/tombstone/{key}"
        try:
            resp = await self._client.post(
                url,
                json={"version": version, "tombstone_expires_at": tombstone_expires_at},
            )
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"Forward tombstone failed for {key} → {node_id}: {e}")
            raise RuntimeError(f"Peer {node_id} unreachable") from e

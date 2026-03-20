"""
HealthChecker — background failure detection for cluster nodes.

Runs an asyncio task that pings every peer's /health endpoint at a
configurable interval. Maintains a simple in-memory health map so
ClusterRouter can skip known-dead nodes on read operations.

Two paths to mark a node down:
  1. Background loop   — periodic HTTP ping fails
  2. Immediate         — router calls mark_down() on first RequestError

Two paths to mark a node back up:
  1. Background loop   — next successful ping restores it
  (Router never marks nodes UP — only the background loop does that,
   to avoid flip-flopping on transient errors.)
"""
import asyncio
import logging
from typing import Optional

import httpx

from app.cluster.node_config import NodeConfig

logger = logging.getLogger(__name__)

# Separate short timeout for health pings so we don't block the loop
HEALTH_PING_TIMEOUT = 1.0


class HealthChecker:
    """
    Tracks the liveness of every node in the cluster.

    Usage:
        hc = HealthChecker(config)
        await hc.start()                 # begin background checking

        hc.is_healthy("node-1")          # → True / False
        hc.mark_down("node-1")           # immediate reaction from router

        await hc.stop()                  # clean shutdown
    """

    def __init__(self, config: NodeConfig, check_interval: float = 5.0):
        self.config = config
        self.check_interval = check_interval

        # Optimistic start: assume all peers are healthy until proven otherwise.
        # This node itself is always considered healthy (we're running!).
        self._healthy: dict[str, bool] = {
            node_id: True for node_id in config.node_ids
        }

        self._task: Optional[asyncio.Task] = None
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Open HTTP client and launch the background ping loop."""
        self._client = httpx.AsyncClient(timeout=HEALTH_PING_TIMEOUT)
        self._task = asyncio.create_task(self._loop(), name="health-checker")
        logger.info(
            f"HealthChecker started (interval={self.check_interval}s, "
            f"watching {list(self._healthy.keys())})"
        )

    async def stop(self) -> None:
        """Cancel background loop and close HTTP client."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.aclose()
        logger.info("HealthChecker stopped")

    # ------------------------------------------------------------------
    # Public API (called by ClusterRouter and /cluster/health endpoint)
    # ------------------------------------------------------------------

    def is_healthy(self, node_id: str) -> bool:
        """
        Return True if the node is currently believed to be alive.

        This node itself always returns True — if we're answering requests,
        we're obviously healthy.
        """
        if self.config.is_local(node_id):
            return True
        return self._healthy.get(node_id, True)  # default optimistic

    def mark_down(self, node_id: str) -> None:
        """
        Immediately mark a peer as unhealthy.

        Called by ClusterRouter the moment a forwarded request fails,
        without waiting for the next health check tick.
        """
        if not self.config.is_local(node_id):
            if self._healthy.get(node_id, True):   # only log on change
                logger.warning(f"HealthChecker: marking {node_id} DOWN (immediate)")
            self._healthy[node_id] = False

    def get_status(self) -> dict[str, bool]:
        """Return the full health map — used by /cluster/health endpoint."""
        return dict(self._healthy)

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Ping every peer periodically and update health state."""
        while True:
            await asyncio.sleep(self.check_interval)
            await self._check_all()

    async def _check_all(self) -> None:
        """Ping every peer concurrently."""
        tasks = [
            self._ping(node_id, url)
            for node_id, url in self.config.peers.items()
            if not self.config.is_local(node_id)   # no need to ping ourselves
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _ping(self, node_id: str, base_url: str) -> None:
        """Ping one peer and update its health entry."""
        try:
            resp = await self._client.get(f"{base_url}/health")
            resp.raise_for_status()
            # Node is reachable — restore if it was down
            if not self._healthy.get(node_id, True):
                logger.info(f"HealthChecker: {node_id} is back UP")
            self._healthy[node_id] = True
        except Exception:
            if self._healthy.get(node_id, True):   # only log on change
                logger.warning(f"HealthChecker: {node_id} is DOWN (ping failed)")
            self._healthy[node_id] = False

"""
Consistent Hashing with virtual nodes.

Distributes keys across nodes such that:
  1. Keys are evenly distributed (via virtual nodes)
  2. Adding/removing a node moves only ~1/N of keys
  3. Same key always maps to same node (deterministic)

Phase 4 adds get_nodes(key, n) to select N distinct physical nodes for
replication — primary first, then replicas in clockwise ring order.
"""
import bisect
import hashlib
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class ConsistentHashRing:
    """
    A sorted ring of hash positions, each mapped to a physical node.

    Finding the owner of a key:
      hash(key) → position → binary-search ring → next position clockwise
      → physical node at that position

    Virtual nodes: each physical node is placed at `num_vnodes` positions
    so that the ring arcs are roughly equal, giving ~equal key load.
    """

    def __init__(self, num_vnodes: int = 150):
        self.num_vnodes = num_vnodes
        self.ring: list[int] = []           # sorted list of positions
        self.ring_map: dict[int, str] = {}  # position → node_id
        self.nodes: set[str] = set()        # all physical node IDs
        logger.info(f"ConsistentHashRing created ({num_vnodes} vnodes/node)")

    # ------------------------------------------------------------------
    # Ring management
    # ------------------------------------------------------------------

    def _hash(self, key: str) -> int:
        """SHA-256 → first 64 bits as integer (uniform distribution)."""
        return int(hashlib.sha256(key.encode()).hexdigest()[:16], 16)

    def add_node(self, node_id: str) -> None:
        """Add a physical node with `num_vnodes` positions on the ring."""
        if node_id in self.nodes:
            logger.warning(f"Node {node_id} already in ring")
            return
        self.nodes.add(node_id)
        for i in range(self.num_vnodes):
            pos = self._hash(f"{node_id}:{i}")
            bisect.insort(self.ring, pos)
            self.ring_map[pos] = node_id
        logger.info(f"Added node {node_id} ({self.num_vnodes} vnodes)")

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node and all its virtual positions."""
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} not in ring")
            return
        self.nodes.discard(node_id)
        to_remove = [p for p, n in self.ring_map.items() if n == node_id]
        for pos in to_remove:
            self.ring.remove(pos)
            del self.ring_map[pos]
        logger.info(f"Removed node {node_id} ({len(to_remove)} vnodes)")

    # ------------------------------------------------------------------
    # Key → node mapping
    # ------------------------------------------------------------------

    def get_node(self, key: str) -> Optional[str]:
        """Return the PRIMARY node for a key (first result of get_nodes)."""
        result = self.get_nodes(key, n=1)
        return result[0] if result else None

    def get_nodes(self, key: str, n: int) -> list[str]:
        """
        Return an ordered list of up to n DISTINCT physical nodes for a key.

        Algorithm:
          1. Hash the key to a ring position
          2. Walk clockwise from that position
          3. Collect distinct physical nodes until we have n of them

        The first element is the PRIMARY; the rest are REPLICAS.
        This is the same strategy used by Cassandra and Amazon Dynamo.

        Args:
            key: the lookup key
            n:   desired replication count (capped at number of nodes)

        Returns:
            [primary, replica-1, ..., replica-(n-1)]

        Example — ring has nodes A, B, C; n=2:
            key hashes near B → returns ["B", "C"]
            B  = primary (writes go here first)
            C  = replica (also receives every write)
        """
        if not self.ring:
            return []

        # Cannot replicate to more nodes than we have
        n = min(n, len(self.nodes))

        result: list[str] = []
        seen: set[str] = set()

        # Start just clockwise of the key's hash position
        start = bisect.bisect_right(self.ring, self._hash(key))
        total = len(self.ring)

        for offset in range(total):
            idx = (start + offset) % total
            node_id = self.ring_map[self.ring[idx]]
            if node_id not in seen:
                seen.add(node_id)
                result.append(node_id)
            if len(result) == n:
                break

        return result

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    def get_distribution(self) -> dict[str, int]:
        """Virtual-node count per physical node (should be equal)."""
        dist = {n: 0 for n in self.nodes}
        for node_id in self.ring_map.values():
            dist[node_id] += 1
        return dist

    def get_node_count(self) -> int:
        return len(self.nodes)

    def get_vnode_count(self) -> int:
        return len(self.ring)

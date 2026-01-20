"""
Consistent Hashing implementation with virtual nodes.

Consistent hashing distributes keys across nodes such that:
1. Keys are evenly distributed
2. Adding/removing nodes only affects ~1/N of keys
3. Same key always maps to same node (deterministic)

Virtual nodes (vnodes) improve distribution by giving each physical
node multiple positions on the hash ring.
"""
import hashlib
import bisect
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ConsistentHashRing:
    """
    Consistent hash ring with virtual nodes for even distribution.
    
    The ring is a sorted array of hash positions, each mapped to a node.
    To find which node owns a key, we hash the key and find the next
    position clockwise on the ring.
    
    Example with 2 nodes and 3 vnodes each:
        Node A: positions [100, 500, 900]
        Node B: positions [200, 600, 800]
        Ring: [100→A, 200→B, 500→A, 600→B, 800→B, 900→A]
        
        Key "foo" hashes to 550 → next position is 600 → goes to Node B
    """
    
    def __init__(self, num_vnodes: int = 150):
        """
        Initialize the hash ring.
        
        Args:
            num_vnodes: Number of virtual nodes per physical node.
                       More vnodes = better distribution but more memory.
                       150 is a good balance (used by industry systems).
        """
        self.num_vnodes = num_vnodes
        self.ring: list[int] = []  # Sorted positions
        self.ring_map: dict[int, str] = {}  # position → node_id
        self.nodes: set[str] = set()  # All node IDs
        
        logger.info(f"Initialized hash ring with {num_vnodes} virtual nodes per node")
    
    def _hash(self, key: str) -> int:
        """
        Hash a string to a position on the ring.
        
        Uses SHA-256 for uniform distribution. We take first 16 hex chars
        (64 bits) to get a position in range [0, 2^64).
        
        Args:
            key: String to hash
            
        Returns:
            Integer position on the ring
        """
        hash_digest = hashlib.sha256(key.encode('utf-8')).hexdigest()
        # Take first 16 hex characters (64 bits) and convert to int
        return int(hash_digest[:16], 16)
    
    def add_node(self, node_id: str) -> None:
        """
        Add a node to the hash ring.
        
        Creates num_vnodes virtual nodes for this physical node,
        each at a different position on the ring.
        
        Args:
            node_id: Unique identifier for the node (e.g., "shard-0")
        """
        if node_id in self.nodes:
            logger.warning(f"Node {node_id} already exists in ring")
            return
        
        self.nodes.add(node_id)
        
        # Create virtual nodes
        for vnode_idx in range(self.num_vnodes):
            # Create unique vnode identifier
            vnode_key = f"{node_id}:{vnode_idx}"
            position = self._hash(vnode_key)
            
            # Add to ring (maintaining sorted order)
            bisect.insort(self.ring, position)
            self.ring_map[position] = node_id
        
        logger.info(f"Added node {node_id} with {self.num_vnodes} virtual nodes")
    
    def remove_node(self, node_id: str) -> None:
        """
        Remove a node from the hash ring.
        
        Removes all virtual nodes belonging to this physical node.
        
        Args:
            node_id: Node to remove
        """
        if node_id not in self.nodes:
            logger.warning(f"Node {node_id} not found in ring")
            return
        
        self.nodes.remove(node_id)
        
        # Remove all virtual nodes for this physical node
        positions_to_remove = [
            pos for pos, nid in self.ring_map.items()
            if nid == node_id
        ]
        
        for position in positions_to_remove:
            self.ring.remove(position)
            del self.ring_map[position]
        
        logger.info(f"Removed node {node_id} ({len(positions_to_remove)} virtual nodes)")
    
    def get_node(self, key: str) -> Optional[str]:
        """
        Find which node owns a given key.
        
        Hashes the key to a position, then finds the next virtual node
        clockwise on the ring. Returns the physical node that vnode belongs to.
        
        Args:
            key: The key to look up
            
        Returns:
            Node ID that owns this key, or None if ring is empty
        """
        if not self.ring:
            return None
        
        # Hash key to position
        position = self._hash(key)
        
        # Find next position clockwise (binary search)
        idx = bisect.bisect_right(self.ring, position)
        
        # Wrap around if we're past the end
        if idx == len(self.ring):
            idx = 0
        
        # Get the node at that position
        ring_position = self.ring[idx]
        node_id = self.ring_map[ring_position]
        
        return node_id
    
    def get_distribution(self) -> dict[str, int]:
        """
        Get statistics on how virtual nodes are distributed.
        
        Useful for verifying even distribution across nodes.
        
        Returns:
            Dictionary mapping node_id → count of virtual nodes
        """
        distribution: dict[str, int] = {node: 0 for node in self.nodes}
        
        for node_id in self.ring_map.values():
            distribution[node_id] += 1
        
        return distribution
    
    def get_node_count(self) -> int:
        """Get number of physical nodes in the ring"""
        return len(self.nodes)
    
    def get_vnode_count(self) -> int:
        """Get total number of virtual nodes in the ring"""
        return len(self.ring)

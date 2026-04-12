"""
Node configuration for the distributed cluster.

Each node reads its identity and peer list from environment variables.
This allows the same Docker image to run as any node in the cluster.

Environment variables:
    NODE_ID            - Unique identifier for this node (e.g. "node-0")
    NODE_PORT          - Port this node listens on (default 8000)
    PEERS              - Comma-separated HTTP URLs of ALL nodes including self
                         e.g. "http://node-0:8000,http://node-1:8001"
    REPLICATION_FACTOR - How many nodes each key is written to (default 2)
    DATA_DIR           - Directory for WAL files (default "data")
"""
import os
import logging

logger = logging.getLogger(__name__)


class NodeConfig:
    """
    Holds configuration for one cluster node.

    Parses environment variables at startup and exposes a clean API
    so the rest of the code never touches os.getenv directly.
    """

    def __init__(self):
        self.node_id: str = os.getenv("NODE_ID", "node-0")
        self.port: int = int(os.getenv("NODE_PORT", "8000"))

        # Parse peer list: "http://node-0:8000,http://node-1:8001"
        raw_peers = os.getenv(
            "PEERS",
            f"http://localhost:{self.port}"
        )
        self.peers: dict[str, str] = {}   # node_id → base URL
        for url in raw_peers.split(","):
            url = url.strip()
            if not url:
                continue
            # Derive node_id from URL index position so we don't need
            # a separate mapping env var.
            # Convention: peers are listed in node order (node-0, node-1, …)
            idx = len(self.peers)
            nid = f"node-{idx}"
            self.peers[nid] = url

        # WAL is stored per-node so each container has isolated storage
        self.data_dir: str = os.getenv("DATA_DIR", "data")

        # How many physical nodes store each key.
        # N=1 → no replication (Phase 3 behaviour)
        # N=2 → 1 primary + 1 replica (default)
        self.replication_factor: int = int(os.getenv("REPLICATION_FACTOR", "2"))

        # WAL durability mode: "strict" (fsync per write) or "relaxed" (batched)
        self.durability: str = os.getenv("DURABILITY", "strict")

        logger.info(
            f"NodeConfig: id={self.node_id} port={self.port} "
            f"peers={list(self.peers.keys())} rf={self.replication_factor} "
            f"durability={self.durability}"
        )

    def peer_url(self, node_id: str) -> str:
        """Return base URL for a peer node."""
        return self.peers[node_id]

    def is_local(self, node_id: str) -> bool:
        """True if the given node_id is this node."""
        return node_id == self.node_id

    @property
    def node_ids(self) -> list[str]:
        """Sorted list of all node IDs in the cluster."""
        return sorted(self.peers.keys())

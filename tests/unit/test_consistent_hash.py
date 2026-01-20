"""
Unit tests for ConsistentHashRing

Tests the consistent hashing algorithm and virtual node distribution.
"""
import pytest
from app.cluster.consistent_hash import ConsistentHashRing


def test_hash_ring_initialization():
    """Test creating a hash ring"""
    ring = ConsistentHashRing(num_vnodes=10)
    
    assert ring.get_node_count() == 0
    assert ring.get_vnode_count() == 0


def test_add_single_node():
    """Test adding a single node"""
    ring = ConsistentHashRing(num_vnodes=10)
    ring.add_node("node-1")
    
    assert ring.get_node_count() == 1
    assert ring.get_vnode_count() == 10
    
    # All keys should map to this node
    assert ring.get_node("key1") == "node-1"
    assert ring.get_node("key2") == "node-1"


def test_add_multiple_nodes():
    """Test adding multiple nodes"""
    ring = ConsistentHashRing(num_vnodes=10)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")
    
    assert ring.get_node_count() == 3
    assert ring.get_vnode_count() == 30


def test_remove_node():
    """Test removing a node"""
    ring = ConsistentHashRing(num_vnodes=10)
    ring.add_node("node-1")
    ring.add_node("node-2")
    
    ring.remove_node("node-1")
    
    assert ring.get_node_count() == 1
    assert ring.get_vnode_count() == 10
    
    # All keys should now map to node-2
    assert ring.get_node("key1") == "node-2"


def test_consistent_routing():
    """Test that same key always maps to same node"""
    ring = ConsistentHashRing(num_vnodes=10)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")
    
    # Same key should consistently map to same node
    node1 = ring.get_node("user:123")
    node2 = ring.get_node("user:123")
    node3 = ring.get_node("user:123")
    
    assert node1 == node2 == node3


def test_distribution_with_many_keys():
    """Test that keys are distributed relatively evenly"""
    ring = ConsistentHashRing(num_vnodes=150)  # Industry standard
    ring.add_node("shard-0")
    ring.add_node("shard-1")
    ring.add_node("shard-2")
    
    # Count keys per node
    distribution = {
        "shard-0": 0,
        "shard-1": 0,
        "shard-2": 0
    }
    
    # Hash 10000 keys
    for i in range(10000):
        key = f"key:{i}"
        node = ring.get_node(key)
        distribution[node] += 1
    
    # Each node should get roughly 1/3 of keys (±15%)
    # With 150 vnodes, distribution should be good but some variance is expected
    for node, count in distribution.items():
        assert 2800 < count < 3800, f"{node} got {count} keys (expected ~3333)"
    
    print(f"Distribution: {distribution}")


def test_vnode_distribution():
    """Test that virtual nodes are evenly distributed"""
    ring = ConsistentHashRing(num_vnodes=150)
    ring.add_node("shard-0")
    ring.add_node("shard-1")
    ring.add_node("shard-2")
    
    vnode_dist = ring.get_distribution()
    
    # Each node should have exactly num_vnodes virtual nodes
    assert vnode_dist["shard-0"] == 150
    assert vnode_dist["shard-1"] == 150
    assert vnode_dist["shard-2"] == 150


def test_add_duplicate_node():
    """Test that adding duplicate node is handled gracefully"""
    ring = ConsistentHashRing(num_vnodes=10)
    ring.add_node("node-1")
    ring.add_node("node-1")  # Duplicate
    
    # Should still only have 1 node with 10 vnodes
    assert ring.get_node_count() == 1
    assert ring.get_vnode_count() == 10


def test_remove_nonexistent_node():
    """Test that removing nonexistent node is handled gracefully"""
    ring = ConsistentHashRing(num_vnodes=10)
    ring.add_node("node-1")
    
    ring.remove_node("node-2")  # Doesn't exist
    
    # node-1 should still be there
    assert ring.get_node_count() == 1


def test_empty_ring():
    """Test getting node from empty ring"""
    ring = ConsistentHashRing()
    
    # Should return None when ring is empty
    assert ring.get_node("any-key") is None


def test_deterministic_hashing():
    """Test that hash function is deterministic"""
    ring1 = ConsistentHashRing(num_vnodes=10)
    ring1.add_node("node-1")
    ring1.add_node("node-2")
    
    ring2 = ConsistentHashRing(num_vnodes=10)
    ring2.add_node("node-1")
    ring2.add_node("node-2")
    
    # Same keys should map to same nodes in both rings
    for i in range(100):
        key = f"key:{i}"
        assert ring1.get_node(key) == ring2.get_node(key)


def test_rebalancing_minimal_keys_moved():
    """Test that adding a node only affects ~1/N of keys"""
    ring = ConsistentHashRing(num_vnodes=150)
    ring.add_node("shard-0")
    ring.add_node("shard-1")
    
    # Map 10000 keys with 2 nodes
    original_mapping = {}
    for i in range(10000):
        key = f"key:{i}"
        original_mapping[key] = ring.get_node(key)
    
    # Add third node
    ring.add_node("shard-2")
    
    # Count how many keys moved
    moved = 0
    for key, original_node in original_mapping.items():
        new_node = ring.get_node(key)
        if new_node != original_node:
            moved += 1
    
    # With 3 nodes, only ~1/3 of keys should move
    # Allow some variance (25-40%)
    move_percent = (moved / 10000) * 100
    assert 25 < move_percent < 40, f"{move_percent}% keys moved (expected ~33%)"
    
    print(f"Added 1 node: {move_percent:.1f}% of keys moved")

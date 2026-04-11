# ADR-001: Consistent Hashing over Modulo Hashing

## Status
Accepted

## Context
We need to distribute keys across N nodes. The two common approaches are:

1. **Modulo hashing:** `node = hash(key) % N`
2. **Consistent hashing:** place nodes and keys on a hash ring, walk clockwise to find the owner

The cluster must support adding/removing nodes without massive data redistribution.

## Options Considered

### Option A: Modulo Hashing (`hash(key) % N`)
- **Pros:** Simple, O(1) lookup, no data structures needed
- **Cons:** Adding a node changes `N`, which reshuffles **~67% of keys** (in a 3→4 node expansion). Every node's key set changes. This makes scaling extremely expensive.

### Option B: Consistent Hashing with Virtual Nodes
- **Pros:** Adding a node moves only `~1/N` of keys (25% for 3→4 nodes). Deterministic — every node independently computes the same mapping. Virtual nodes solve the uneven-distribution problem of basic consistent hashing.
- **Cons:** Slightly more complex (sorted ring + binary search). ~150 virtual positions per node uses a small amount of memory.

## Decision
**Consistent hashing with 150 virtual nodes per physical node.**

The key-movement cost of modulo hashing is unacceptable for a system that needs horizontal scaling. Consistent hashing is what DynamoDB, Cassandra, and memcached use for the same reason.

We chose 150 vnodes because empirical testing shows <5% variance in key distribution across 3 nodes at this value. Lower values (e.g., 10) produce noticeable skew.

## Trade-offs
- More complex code (`ConsistentHashRing` class with `bisect.insort`)
- Memory: ~450 ring entries for a 3-node cluster (negligible)
- Lookup is O(log V) binary search instead of O(1) modulo (V = total vnodes, ~450 — sub-microsecond)

## References
- Karger et al., "Consistent Hashing and Random Trees" (1997)
- DynamoDB paper (Dynamo: Amazon's Highly Available Key-Value Store, 2007)
- Our implementation: `app/cluster/consistent_hash.py`

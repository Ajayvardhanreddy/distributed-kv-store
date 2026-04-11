# ADR-003: Why Not Raft for Leader Election

## Status
Accepted

## Context
When the primary node for a key goes down, we need to decide who takes over writes. This is the "leader election" problem. The two approaches:

1. **Raft consensus** — nodes vote on a new leader, majority required
2. **Deterministic ring-based selection** — first healthy node in the replication set is the leader

## Options Considered

### Option A: Raft Consensus
- **Pros:** Eliminates split-brain entirely. Leader is agreed upon by majority. This is what etcd, CockroachDB, and TiKV use.
- **Cons:** ~1500 lines of complex, subtle code (log replication, term management, snapshotting). Election storms under network partitions. Significant testing overhead. Raft is a **CP** algorithm — it sacrifices availability during leader election (no writes until a majority agrees).

### Option B: Deterministic Ring-Based Selection (our choice)
- **Pros:** Zero additional code beyond the existing hash ring + HealthChecker. Every node independently computes the same leader from the same ring order + health state. No voting, no coordination, no election delays. Writes resume immediately when a healthy replica exists.
- **Cons:** **Split-brain window.** If node-0's HealthChecker sees node-1 as down but node-2's HealthChecker still sees node-1 as up, they'll route writes to different leaders for ~5 seconds (one health-check interval). This can cause conflicting writes.

### Option C: External Coordination (ZooKeeper/etcd)
- **Pros:** Battle-tested leader election. Eliminates split-brain.
- **Cons:** External dependency. Defeats the purpose of a from-scratch project. ZooKeeper itself uses a variant of Paxos internally.

## Decision
**Deterministic ring-based leader selection.**

The split-brain window (~5s) is an acceptable trade-off for this project because:
1. It occurs only during the narrow window between a node failing and all health-checkers converging
2. It aligns with our explicit AP design choice
3. It avoids ~1500 lines of Raft code that would dominate the codebase
4. DynamoDB itself uses a similar ring-based approach (sloppy quorum + hinted handoff)

## Trade-offs

| | Raft | Our approach |
|---|---|---|
| Split-brain | Impossible | ~5s window |
| Code complexity | ~1500 LOC | 0 additional LOC |
| Write availability during election | Blocked (no leader) | Immediate (first healthy) |
| CAP position | CP | AP |

## What Raft Would Add (Interview Answer)
"We chose ring-based leader selection for availability — writes resume immediately. The trade-off is a ~5s split-brain window. In production I'd add Raft for the control plane (cluster membership, config changes) while keeping the ring-based data path. This is what Cassandra does: gossip for membership, ring for data routing."

## References
- Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (2014)
- Amazon Dynamo paper, Section 4.6 (Sloppy Quorum)
- Our implementation: `app/cluster/router.py` → `_write_leader()`

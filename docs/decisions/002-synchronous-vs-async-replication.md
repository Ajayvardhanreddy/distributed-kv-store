# ADR-002: Synchronous Replication over Asynchronous

## Status
Accepted

## Context
When a client writes `PUT /kv/user:1 = "alice"`, we need to decide **when** the write is applied to replicas:

1. **Synchronous:** write to primary + all replicas *before* responding 200
2. **Asynchronous:** respond 200 after the primary confirms; replicas get the write "eventually"

## Options Considered

### Option A: Asynchronous Replication
- **Pros:** Lower write latency (primary-only). Higher write throughput.
- **Cons:** **Data loss window.** If the primary crashes 50ms after responding 200 but before the async replica write lands, the data is gone. The client thinks the write succeeded — it didn't. This is exactly what happened in [GitHub's 2018 MySQL incident](https://github.blog/2018-10-30-oct21-post-incident-analysis/).

### Option B: Synchronous Replication (our choice)
- **Pros:** No data loss on single-node failure. Client gets 200 only after all live replicas confirm. Strong durability guarantee.
- **Cons:** Higher write latency (network RTT to replicas). If any replica is unreachable, the write fails.

### Option C: Quorum Writes (W=2 of N=3)
- **Pros:** Tolerates 1 slow/down replica while still providing strong durability.
- **Cons:** More complex conflict resolution. Would need read-repair or anti-entropy to bring the missed replica up to date.

## Decision
**Synchronous replication to all healthy nodes, with leader promotion for the write path.**

We fan out writes to ALL healthy replicas via `asyncio.gather` (parallel, not sequential). If a replica is known-down via the HealthChecker, we skip it and let it catch up via sync-on-rejoin.

This gives us strong durability without the data-loss window of async replication, while avoiding the write-latency penalty of waiting for unhealthy nodes.

## Trade-offs
- Write latency = max(RTT to all replicas) instead of just local
- In practice with `asyncio.gather`, the overhead is one network round-trip (~0.5ms in a local Docker network)
- A truly partitioned replica will cause writes to skip it, creating a stale-data window until it rejoins and syncs

## References
- "Designing Data-Intensive Applications" by Martin Kleppmann, Chapter 5 (Replication)
- Our implementation: `app/cluster/router.py` → `_write_all()` method

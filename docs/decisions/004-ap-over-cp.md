# ADR-004: AP over CP — Availability over Consistency

## Status
Accepted

## Context
The CAP theorem states that in the presence of a network partition (P), a distributed system must choose between:

- **Consistency (C):** every read returns the most recent write
- **Availability (A):** every request gets a response (even if stale)

Our system must make an explicit choice.

## Options Considered

### Option A: CP (Consistency + Partition Tolerance)
Systems like ZooKeeper, etcd, Google Spanner.
- **Pros:** No stale reads. Linearizable guarantees.
- **Cons:** Requests block during partitions/leader elections. Write unavailability until majority quorum is established. Higher tail latency.

### Option B: AP (Availability + Partition Tolerance) — our choice
Systems like DynamoDB, Cassandra, Riak.
- **Pros:** Reads always return (potentially stale). Writes succeed as long as any replica is alive. Low latency. Simpler operational model.
- **Cons:** Stale reads possible. Conflicting writes during split-brain. Requires conflict resolution strategy.

## Decision
**AP — Availability over Consistency.**

Specifically, our system makes these AP choices:

| Scenario | Behaviour | Consistency implication |
|----------|-----------|----------------------|
| Primary down, read arrives | Serve from replica | May be stale if primary had recent unsynced writes |
| Primary down, write arrives | Route to promoted replica | Two nodes could briefly accept conflicting writes |
| Node rejoins after crash | Sync missing keys from peer | WAL-replayed state takes precedence (last-write-wins) |
| Two nodes disagree on leader | Both accept writes for ~5s | Conflict window — resolved by eventual convergence |

## Why AP is Right for This Project
1. A cache/KV store values availability over strict consistency — a slightly stale response is better than a timeout
2. This matches real-world systems like DynamoDB (which defaults to eventual consistency)
3. CP requires Raft/Paxos — substantial implementation complexity with unclear benefit for a KV store

## What CP Would Require (Interview Answer)
"To move to CP, I would add Raft for the write path: a write succeeds only after a majority of replicas in the replication set acknowledge it. Reads would go through the Raft leader to guarantee linearizability. The cost is write latency (majority round-trip) and unavailability during leader elections. Google Spanner solves this with TrueTime for globally-consistent reads, but requires specialized hardware."

## References
- Brewer, "CAP Twelve Years Later" (2012)
- DynamoDB: AP by default, optional strong consistency per-read
- Our implementation reflects AP throughout: `router.py` read fallback, `_write_leader()`, `sync_from_peers()`

# ADR-008 — Tombstone DELETE model

## Status
Accepted (Phase 2)

## Context

The original DELETE implementation physically removed keys from `self.store` and
appended a bare `DELETE` record to the WAL.  This worked fine for a single node
but created a correctness problem in a multi-node cluster:

**Key resurrection on rejoin.**  If node-A goes offline and node-B deletes a key,
when node-A rejoins it replays its WAL (which still has the PUT), then
sync-on-rejoin compares versions.  Because node-A's PUT version equals node-B's
last-known version, node-A "wins" and the deleted key resurfaces — invisible to
the operator, silently violating the DELETE contract.

## Decision

Introduce tombstones as a first-class value state:

```python
# Live entry
{"value": str, "version": int, "deleted": False}

# Tombstone
{"deleted": True, "version": int, "tombstone_expires_at": float}
```

### Rules

| Rule | Detail |
|------|--------|
| **All DELETEs produce tombstones** | engine.delete(), sweeper (Phase 3), WAL replay |
| **Tombstone version = prior_version + 1** | Guarantees tombstone always beats the live entry it replaces in any version comparison |
| **GET on tombstone → (None, 0)** | Callers cannot distinguish tombstone from absent key |
| **exists() / size() exclude tombstones** | Transparent to application logic |
| **snapshot() includes tombstones** | Anti-entropy peers learn about deletions |
| **Physical removal only in compact()** | The single permitted site of `del self.store[key]` |
| **No physical deletion on write path** | Prevents resurrection race during fan-out |

### Resurrection prevention

sync-on-rejoin now compares versions for both live entries **and** tombstones.
If the peer has a tombstone with version N and the rejoining node has a live entry
with version < N, the tombstone wins and the live entry is suppressed.

### Tombstone retention

Tombstones must outlive the maximum peer-down window to guarantee every replica
sees the delete before the tombstone is compacted away.  Default: **24 hours**,
configurable via `TOMBSTONE_RETENTION_SECONDS`.  A background compaction task runs
every 5 minutes and physically removes tombstones past their `tombstone_expires_at`.

### Fan-out carries tombstone metadata

The write-leader assigns `(version, tombstone_expires_at)` and fans it out via
`POST /internal/tombstone/{key}`.  All replicas store identical tombstones —
no version skew during rejoin.

## Alternatives considered

| Option | Reason rejected |
|--------|----------------|
| Physical delete + version broadcast | Replica still resurrects if it rejoins after broadcast completes |
| Epoch-based logical clocks | More complex; version counter is sufficient for the AP model |
| Raft-coordinated delete | Out of scope — AP choice documented in ADR-004 |

## Consequences

- **Positive:** Key resurrection is eliminated for any delete that precedes a rejoin by less than `TOMBSTONE_RETENTION_SECONDS`.
- **Positive:** Phase 3 (TTL sweeper) can write tombstones using the same path — no special case needed.
- **Positive:** `snapshot()` now carries full deletion history to peers, making anti-entropy correct.
- **Negative:** Store memory grows until compaction runs (bounded by retention window × delete rate).
- **Negative:** `DELETE /internal/kv/{key}` response shape changed to include `version` and `tombstone_expires_at` — internal API only, no client impact.

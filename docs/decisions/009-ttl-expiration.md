# ADR-009 — TTL: Lazy GET expiry + active sweeper

## Status
Accepted (Phase 3)

## Context

Without TTL, the store grows unbounded.  Keys that are no longer needed occupy
RAM and WAL space indefinitely.  Redis, DynamoDB, and most production KV stores
support per-key expiration.

## Decision

### Two-tier expiration model

**Tier 1 — Lazy expiry (read path)**
GET, exists, and size treat a key as absent if `expires_at <= now`.
The read path is **side-effect-free** — no tombstone is written, no lock is
promoted.  This keeps p99 read latency unaffected by expiry logic.

**Tier 2 — Active sweeper (background task)**
A background coroutine runs every 60 s, draws a random sample of up to 100
live keys with a TTL, and for each expired key calls `router.delete()`.
`router.delete()` writes a tombstone (Phase 2 contract) and fans the tombstone
out to all replicas.

### Why not physically delete on lazy read?

Physical deletion on the read path would:
- Add a write-lock acquisition to every expired GET
- Require tombstone fan-out on the read path — turning a read into a write RPC
- Break the "read path is side-effect-free" invariant used by quorum reads (Phase 6)

### expires_at is an absolute epoch-seconds float

The write leader computes `expires_at = time.time() + ttl_seconds` **once** and
fans out the absolute timestamp unchanged to all replicas.  Replicas do **not**
recompute from `ttl_seconds`.  This guarantees all copies expire at the same
wall-clock moment regardless of network delay or clock drift (within NTP bounds).

### Sweeper sample strategy

The sweeper uses `random.sample()` on live keys that have a TTL.  This avoids a
full O(N) scan on every tick.  For sparse TTL workloads (few keys have TTL) the
sample is small and fast.  For dense workloads the sample is bounded to 100 keys
per run, spread across ticks.  This is the same probabilistic approach used by
Redis's active expiry.

### Tombstone produced by sweeper = Phase 2 tombstone

The sweeper calls `router.delete()` — the same code path as a manual client
DELETE.  This means:
- Tombstone is WAL'd
- Tombstone is fanned out to replicas
- Tombstone prevents resurrection on rejoin
- Compaction removes it after `tombstone_expires_at`

There is no special-case code for TTL expiry — it reuses the Phase 2 model.

## Metrics added

| Metric | Type | Description |
|--------|------|-------------|
| `keys_expired_total` | Counter | Keys tombstoned by the active sweeper |
| `sweeper_runs_total` | Counter | Active sweeper iterations |

## Consequences

- **Positive:** Bounded memory growth — expired keys are tombstoned within
  `SWEEPER_INTERVAL` + one compaction cycle.
- **Positive:** Read path latency unaffected (lazy expiry is a simple timestamp
  comparison with no I/O).
- **Positive:** Reuses Phase 2 tombstone infrastructure — no special DELETE path.
- **Negative:** Window between key expiry and tombstone creation is up to
  `SWEEPER_INTERVAL` (60 s default).  During that window lazy reads return miss
  but the key is not yet tombstoned on replicas.  Acceptable for cache-style use;
  not suitable for hard access-control TTLs.
- **Negative:** Random sampling may miss expired keys if they are a small fraction
  of the keyspace.  Multiple sweeper ticks will eventually find them.

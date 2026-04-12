# ADR-006: Configurable WAL Durability (Strict vs Relaxed)

## Status
Accepted

## Context
PUT/DELETE operations in our KV store are I/O-bound because each write appends a JSON line to the WAL file and flushes to disk immediately. This gives us ~8,000 PUT ops/sec — safe, but slow for workloads that can tolerate bounded data loss.

Production systems like **Kafka** (`acks=1` vs `acks=all`), **PostgreSQL** (`synchronous_commit = off`), and **Redis** (`appendfsync everysec`) expose similar knobs because different workloads have different durability needs.

## Decision
We introduce a `DURABILITY` environment variable with two modes:

| Mode | Behaviour | Data loss window | Throughput |
|------|-----------|-----------------|-----------|
| `strict` (default) | Every `append()` writes + flushes to disk | 0 | ~8K ops/sec |
| `relaxed` | Writes buffer in memory; flushed every 100ms or 100 ops | ≤100ms | ~50K+ ops/sec |

### Implementation
- **Buffer**: In relaxed mode, `append()` pushes the JSON line into an in-memory list instead of opening the file.
- **Flush loop**: A background `asyncio.Task` runs every 100ms, joining all buffered lines into one string and writing to disk in a single I/O call.
- **Batch threshold**: If the buffer reaches 100 entries before the timer fires, it force-flushes immediately (prevents unbounded memory growth under burst traffic).
- **Shutdown**: `close()` cancels the flush task and performs a final flush.

## Consequences

### Positive
- Throughput increases ~5-10× in relaxed mode (fewer disk I/O calls).
- Demonstrates awareness of the throughput/durability trade-off — a critical production concept.
- Same WAL format; replay() is unmodified.

### Negative
- In relaxed mode, a crash loses up to 100ms of writes (bounded data loss).
- Slightly more complex shutdown path (must cancel background task + final flush).

## Alternatives Considered
- **Group commit with fsync**: More durable than relaxed, but requires OS-level fsync support and is harder to test.
- **Separate WAL thread**: Would isolate I/O but adds threading complexity to an asyncio codebase.

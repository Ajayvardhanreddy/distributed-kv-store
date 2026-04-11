# ADR-005: Write-Ahead Log Before In-Memory Update

## Status
Accepted

## Context
When handling a write (`PUT key=value`), the order of two operations matters:

1. **Write to WAL** (durable, on disk)
2. **Update in-memory store** (fast, volatile)

If the process crashes between these two steps, one of them will be lost. The order determines what we lose.

## Options Considered

### Option A: Memory First, WAL Second
```
store[key] = value    # Step 1: fast, in-memory
wal.append(PUT, key)  # Step 2: durable
# CRASH HERE → client got 200, but WAL doesn't have the write
# On restart: key is GONE. Silent data loss.
```
- **Pros:** Faster perceived latency (memory write is sub-microsecond)
- **Cons:** **Data loss.** If the process crashes after step 1 but before step 2, the write is confirmed to the client but lost on restart. This violates durability guarantees.

### Option B: WAL First, Memory Second (our choice)
```
wal.append(PUT, key)  # Step 1: durable
store[key] = value    # Step 2: fast, in-memory
# CRASH HERE → WAL has the write, even if memory update didn't complete
# On restart: WAL replay restores the write. No data loss.
```
- **Pros:** **No data loss.** The WAL is the source of truth. On startup, `wal.replay()` rebuilds the in-memory store from the log. Even if the process crashes mid-write, the WAL entry is already on disk.
- **Cons:** Write latency includes a disk I/O (WAL append). In practice, this is ~50μs for a buffered write and ~1ms with fsync.

## Decision
**WAL before memory.** This is the universal pattern used by PostgreSQL, SQLite, LevelDB, and every serious storage engine.

Our implementation in `StorageEngine.put()`:
```python
async with self.lock:
    await self.wal.append("PUT", key, value)  # disk first
    self.store[key] = value                    # memory second
```

## Trade-offs
- ~50μs additional latency per write (WAL append before memory update)
- WAL file grows unbounded (no compaction — deferred as a known limitation)
- On restart, replay time is proportional to WAL size

## Future Improvement: WAL Compaction
The WAL currently appends every operation forever. In production, periodic compaction (snapshot + truncate) would bound replay time. We note this as a known limitation.

## References
- "Database Internals" by Alex Petrov, Chapter 3 (Write-Ahead Logging)
- PostgreSQL WAL: https://www.postgresql.org/docs/current/wal-intro.html
- Our implementation: `app/storage/wal.py`, `app/storage/engine.py`

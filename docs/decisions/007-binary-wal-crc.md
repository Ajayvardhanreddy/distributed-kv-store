# ADR-007 — Binary WAL with CRC32 and Schema Versioning

## Status
Accepted (Phase 1)

## Context

The original WAL used JSON-lines: one UTF-8 JSON object per line, terminated with `\n`.  This was readable and easy to debug but had three problems:

1. **No integrity check.** A torn write (power loss mid-line) silently produced a truncated or partially-written JSON object.  The replay loop treated it as a corrupt line and skipped it, but it had no way to distinguish a *torn write at the last record* (safe to ignore) from *corruption in the middle of the file* (dangerous — subsequent records may be offset).

2. **No schema evolution path.** Adding fields like `expires_at` (Phase 3) or a vector-clock `ver` (Phase 5) to an existing JSON entry would be invisible to an older node replaying a newer WAL.  Without a version field there is no dispatch path.

3. **Format efficiency.** JSON is ~5× larger than the equivalent binary representation and requires a UTF-8 decoder on the hot replay path.

## Decision

Replace JSON-lines with a length-prefixed binary format:

```
File  : [4-byte magic 'BWAL'][1-byte format-version = 1]
Record: [4-byte payload-length, big-endian uint32]
        [4-byte CRC32, big-endian uint32]
        [N-byte msgpack payload]
```

The msgpack payload always starts with `schema_version` as the first key.  Current value is `1`; future phases bump this when the payload shape changes (Phase 5 will add a vector-clock `ver` and use `schema_version: 2`).

### Replay safety rule

On a CRC mismatch or short read the replay stops at the **last successfully applied record**, logs the byte offset, and returns the state built so far.  Because the length field lets us locate the start of the next record, a single corrupt record does not invalidate the rest of the file — but we stop anyway to avoid applying records that followed a torn write (their ordering relative to the corrupt record is undefined).

### Migration

Legacy JSON WALs are detected by checking the first 4 bytes for the `BWAL` magic.  On detection:
1. The JSON file is replayed in its entirety (skipping corrupt lines, as before).
2. A fresh binary WAL is written from the replayed state.
3. The original JSON file is renamed to `<path>.bak`.

Migration is transparent: no manual intervention, no data loss.

## Alternatives considered

| Option | Reason rejected |
|--------|----------------|
| JSON-lines with CRC comment | Hack — breaks standard JSON parsers; no schema path |
| Protobuf | Requires code-gen, heavier dependency; msgpack is simpler for a dict-per-record model |
| SQLite WAL | Full dependency, recovery model conflicts with our single-writer async loop |
| Append a CRC at the end of each JSON line | Still no schema versioning; mixes text and binary framing |

## Metrics added

| Metric | Type | Description |
|--------|------|-------------|
| `wal_corrupt_records_total` | Counter | Records skipped due to CRC mismatch or short read |
| `wal_replay_records_total` | Counter | Records successfully applied during replay |

## Consequences

- **Positive:** Torn writes are now detected (corrupt counter increments, replay stops safely).  Schema version field gives Phases 3–5 a clean upgrade path without touching the replay dispatch logic.
- **Positive:** Binary + msgpack is ~5× more compact than JSON-lines.
- **Negative:** WAL files are no longer human-readable with a text editor.  Use `python3 -c "import msgpack, struct, binascii; ..."` or a future `kvctl wal-dump` command to inspect.
- **Neutral:** Existing JSON WAL files are migrated automatically on first startup; the `.bak` file can be deleted once the node is verified healthy.

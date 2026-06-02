# ADR-010 — CAS Conditional Writes and Opaque Version Token

## Status
Accepted (Phase 4)

## Context

Without conditional writes, concurrent clients updating the same key have no way
to detect that another writer has changed the value between their read and write.
The result is a silent last-writer-wins race.  CAS (Compare-And-Swap) gives
clients an optimistic-locking primitive: read a token, write only if the token
still matches.

## Decision

### Opaque version token

`GET /kv/{key}` now returns `version_token: str` — an opaque string the client
stores and passes back on the next write.  The raw integer version is deprecated.
Clients **must not** parse the token; Phase 5 will change its encoding to a
vector clock without any API change.

The token encoding lives in `app/storage/version_token.py`:
```python
encode_token(version: int) -> str          # "1", "42", …
decode_token(token: str) -> int
version_matches(current: int, token: str) -> bool
```
Phase 5 replaces only those three function bodies.

### PUT if_match — optimistic update

```
PUT /kv/key  {"value": "new", "if_match": "<token>"}
```
The write-leader reads the current version, checks `version_matches(current, if_match)`.
- Match  → proceed, return 200 + new `version_token`
- Mismatch → return 409 `{"message": "version mismatch", "current_token": "<token>"}`

Absent `if_match` → unconditional write (back-compat, behaviour unchanged).

### PUT if_none_match — create-if-absent

```
PUT /kv/key  {"value": "new", "if_none_match": true}
```
Fails with 409 if the key exists **or has an active tombstone**.  The tombstone
check prevents resurrection: a client cannot accidentally re-create a key that
another client just deleted via `if_none_match`.

### DELETE if_match

```
DELETE /kv/key?if_match=<token>
```
Same semantics: mismatch → 409.  Absent `if_match` → unconditional delete.

### Replicas do not re-check CAS

CAS is checked once on the write-leader.  Replicas receive `put_versioned()` —
the same unconditional replica path used by normal writes.  This is deliberate:

- Avoids distributed read on every replica to check versions
- CAS is a single-coordinator guarantee, not a distributed consensus guarantee
- The split-brain window (two nodes may briefly be both leaders) means CAS can
  pass independently on two nodes during a partition.  Phase 5 (vector clocks)
  makes this detectable; Phase 6 (quorum) reduces the window further.

### Limitations (explicitly documented)

| Limitation | Impact |
|-----------|--------|
| CAS is not linearizable | During a split-brain window, CAS can succeed on two nodes independently |
| Token is per-key, not per-session | No transaction spanning multiple keys |
| Stale token after network partition | Client may hold a stale token — CAS correctly returns 409 |

## Metrics added

| Metric | Type | Description |
|--------|------|-------------|
| `cas_conflicts_total` | Counter | CAS failures (if_match / if_none_match rejected) |

## Consequences

- **Positive:** Clients can now implement optimistic locking without external
  coordination.
- **Positive:** `version_token.py` isolation means Phase 5 vector clocks require
  no API changes.
- **Positive:** `ShardManager.get()` and `put()` now match the `StorageEngine`
  interface (`(value, version)` tuple / `int`).
- **Negative:** CAS is not linearizable under split-brain — documented limitation,
  not a bug.

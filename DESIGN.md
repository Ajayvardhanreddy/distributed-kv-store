# Distributed KV Store — System Design

> Architecture reference with sequence diagrams for every critical path.

## High-Level Architecture

```mermaid
graph TB
    Client["Client / kvctl"]
    
    subgraph Cluster
        N0["node-0<br/>FastAPI + StorageEngine"]
        N1["node-1<br/>FastAPI + StorageEngine"]
        N2["node-2<br/>FastAPI + StorageEngine"]
    end
    
    Client -->|"PUT /kv/key"| N0
    N0 -->|"fan-out write"| N1
    N0 -->|"fan-out write"| N2
    N0 <-->|"heartbeat"| N1
    N0 <-->|"heartbeat"| N2
    N1 <-->|"heartbeat"| N2
    
    N0 --- WAL0["WAL + In-Memory Store"]
    N1 --- WAL1["WAL + In-Memory Store"]
    N2 --- WAL2["WAL + In-Memory Store"]
```

## Data Model

```
Key: string (e.g., "user:123")
Value: string
Version: monotonic integer per key (starts at 1, increments each PUT)
```

Each node stores: `{key → {value: str, version: int}}`

---

## 1. Versioned Write Path (PUT)

Single-leader write coordination with version tracking:

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Router (any node)
    participant L as Leader (ring primary)
    participant Rep as Replica(s)

    C->>R: PUT /kv/key {value: "hello"}
    R->>R: ring.get_node(key) → leader_id
    
    alt Leader is local
        R->>R: storage.put(key, value) → version=N
        R->>R: WAL.append(PUT, key, value, ver=N)
    else Leader is remote
        R->>L: HTTP PUT /kv/key {value}
        L->>L: storage.put(key, value) → version=N
        L->>L: WAL.append(PUT, key, value, ver=N)
        L-->>R: {version: N}
    end
    
    par Fan-out to replicas
        R->>Rep: PUT /internal/kv/key {value, version: N}
        Rep->>Rep: storage.put_versioned(key, value, N)
        Rep->>Rep: WAL.append(PUT, key, value, ver=N)
        Rep-->>R: 200 OK
    end
    
    R-->>C: {key, value, version: N}
```

### Why single-leader?
- Version counter is authoritative on the leader — no conflicts
- Replicas receive the exact version, ensuring consistency
- Trade-off: leader is a bottleneck per key, but keys are sharded across nodes

---

## 2. Read Path (GET) with Replica Fallback

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Router (any node)
    participant P as Primary
    participant Rep as Replica

    C->>R: GET /kv/key
    R->>R: ring.get_nodes(key, n=rf) → [primary, replica]
    
    alt Primary is healthy
        alt Primary is local
            R->>R: storage.get(key) → (value, version)
        else Primary is remote
            R->>P: HTTP GET /kv/key
            P-->>R: {value, version}
        end
    else Primary is down (health checker)
        R->>R: Skip primary, try next healthy node
        R->>Rep: HTTP GET /kv/key
        Rep-->>R: {value, version}
    end
    
    R-->>C: {key, value, version}
```

### Consistency note
Reads from replicas may return stale data (eventual consistency). The version field lets clients detect staleness.

---

## 3. Failure Detection (Heartbeat)

```mermaid
sequenceDiagram
    participant HC as HealthChecker (per node)
    participant P as Peer Node

    loop Every 2s (configurable)
        HC->>P: GET /health (timeout=1s)
        alt Peer responds 200
            HC->>HC: mark_healthy(peer_id)
        else Timeout / connection refused
            HC->>HC: mark_down(peer_id)
            Note over HC: Triggers read fallback<br/>and leader promotion
        end
    end
```

---

## 4. Leader Promotion (Write Failover)

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Router
    participant HC as HealthChecker

    C->>R: PUT /kv/key {value}
    R->>R: ring.get_nodes(key, n=rf) → [node-1, node-2]
    R->>HC: is_healthy(node-1)?
    HC-->>R: false (node-1 is down)
    R->>HC: is_healthy(node-2)?
    HC-->>R: true
    
    Note over R: Promote node-2 as write leader
    R->>R: Forward PUT to node-2 (promoted leader)
    R-->>C: {version: N}
```

### Trade-off
Leader promotion means the promoted node assigns a **new** version counter. If the original leader comes back, its version may conflict. We resolve this during sync-on-rejoin (higher version wins).

---

## 5. Sync-on-Rejoin (Anti-Entropy)

```mermaid
sequenceDiagram
    participant N as Rejoining Node
    participant P as Peer Node

    Note over N: Node starts up after crash
    N->>P: GET /internal/sync (fetch full snapshot)
    P-->>N: {key1: {value, version: 3},<br/>key2: {value, version: 1}, ...}
    
    loop For each key in peer snapshot
        N->>N: local_val, local_ver = get(key)
        alt peer_version > local_version
            N->>N: put_versioned(key, peer_value, peer_version)
            Note over N: Higher version wins
        else local_version >= peer_version
            Note over N: Keep local (already up-to-date)
        end
    end
    
    Note over N: Node is now consistent with peer
```

---

## 6. WAL Durability Modes

```mermaid
sequenceDiagram
    participant App as StorageEngine
    participant WAL as WriteAheadLog
    participant Disk as Filesystem

    alt DURABILITY=strict (default)
        App->>WAL: append(PUT, key, value, ver)
        WAL->>Disk: write JSON line + flush
        Disk-->>WAL: ack
        WAL-->>App: done (synchronous)
    else DURABILITY=relaxed
        App->>WAL: append(PUT, key, value, ver)
        WAL->>WAL: buffer.push(line)
        WAL-->>App: done (async, in-memory only)
        
        Note over WAL: Background flush loop
        loop Every 100ms or 100 ops
            WAL->>Disk: write batch (single I/O)
            Disk-->>WAL: ack
        end
    end
```

| Mode | Throughput | Data loss window |
|------|-----------|-----------------|
| strict | ~8K PUT/s | 0 |
| relaxed | ~50K+ PUT/s | ≤100ms |

---

## 7. Consistent Hashing (Key Routing)

```mermaid
graph LR
    subgraph Hash Ring
        direction LR
        V1["node-0 vnode"] --> V2["node-1 vnode"]
        V2 --> V3["node-2 vnode"]
        V3 --> V4["node-0 vnode"]
        V4 --> V5["node-1 vnode"]
        V5 --> V6["node-2 vnode"]
        V6 --> V1
    end
    
    K1["key: user:42"] -.->|"hash → ring position"| V3
    K2["key: order:99"] -.->|"hash → ring position"| V5
```

- **150 virtual nodes** per physical node (configurable)
- SHA-256 hash for uniform distribution
- Adding/removing a node moves only ~1/N of keys

---

## ADR Index

| ADR | Decision | Trade-off |
|-----|----------|-----------|
| [001](docs/decisions/001-ap-over-cp.md) | AP over CP | Availability over strict consistency |
| [002](docs/decisions/002-consistent-hashing.md) | Consistent hashing | Uniform distribution vs ring complexity |
| [003](docs/decisions/003-wal-json-lines.md) | JSON-lines WAL | Human-readable vs binary efficiency |
| [004](docs/decisions/004-synchronous-replication.md) | Synchronous replication | Consistency vs write latency |
| [005](docs/decisions/005-single-leader-writes.md) | Single-leader writes | Simplicity vs multi-master throughput |
| [006](docs/decisions/006-batched-wal-durability.md) | Batched WAL | Throughput vs bounded data loss |

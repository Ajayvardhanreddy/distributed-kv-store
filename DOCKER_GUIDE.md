# Docker Guide: Building and Running the KV Store

## Key Components

1. **Dockerfile** — builds the app image using `uv` for fast, reproducible installs
2. **docker-compose.yml** — runs the full 3-node cluster + Prometheus + Grafana
3. **pyproject.toml + uv.lock** — dependency manifest and lockfile (replaces `requirements.txt`)

---

## Quick Start

```bash
# Build and start the full cluster (3 nodes + monitoring)
docker compose up --build

# Or in detached mode
docker compose up --build -d
```

Nodes are available at:
- Node-0: http://localhost:8000
- Node-1: http://localhost:8001
- Node-2: http://localhost:8002
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin / admin)

---

## Common Workflows

### Test the cluster

```bash
# Health check
curl http://localhost:8000/health

# Write a key (replicated to 2 nodes)
curl -X PUT http://localhost:8000/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"hello from Docker!"}'

# Read from any node
curl http://localhost:8001/kv/mykey

# Cluster-wide health view
curl http://localhost:8000/cluster/health | python3 -m json.tool
```

### View logs

```bash
docker compose logs -f            # all services
docker compose logs -f node-0     # single node
docker compose logs --tail=50     # last 50 lines
```

### Stop and clean up

```bash
docker compose down               # stop containers, keep volumes
docker compose down -v            # stop + delete WAL data (destructive)
docker system prune               # reclaim disk space from unused images
```

### Rebuild after dependency changes

Whenever `pyproject.toml` or `uv.lock` changes, rebuild the image:

```bash
docker compose up --build
```

The Dockerfile copies `pyproject.toml` and `uv.lock` before the app code, so layer caching means dependency installs are only re-run when those files change — not on every code edit.

---

## How the Dockerfile Works

```dockerfile
# 1. Copy uv binary from official image (no separate install step)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# 2. Copy only dependency files first (cached layer)
COPY pyproject.toml uv.lock ./

# 3. Install production deps with exact locked versions
RUN uv sync --no-group dev --frozen --no-install-project

# 4. Copy app code (separate layer — invalidated only on code changes)
COPY app/ ./app/
```

---

## Troubleshooting

**Port already in use**
```bash
lsof -i :8000   # find what's using the port
# change the host port in docker-compose.yml if needed
```

**Container won't start — check logs**
```bash
docker compose logs node-0
```

**Clear build cache and rebuild from scratch**
```bash
docker compose build --no-cache
```

**Shell into a running node**
```bash
docker compose exec node-0 /bin/bash
```

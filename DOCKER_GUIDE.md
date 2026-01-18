# Docker Guide: Building and Running Your KV Store

This guide explains how to use Docker for this project, from basics to common workflows.

## 📚 What is Docker?

**Docker** packages your application with all its dependencies into isolated "containers" that run consistently anywhere. Think of it like a lightweight virtual machine.

### Key Components We Use

1. **Dockerfile** - Recipe for building your application image
2. **docker-compose.yml** - Configuration for running containers (including multi-node setups later)
3. **Docker Image** - Executable package built from Dockerfile
4. **Docker Container** - Running instance of an image

---

## 🔧 Step-by-Step: Building and Running

### Step 1: Build the Docker Image

**Command:**
```bash
docker-compose build
```

**What it does:**
- Reads `Dockerfile` and `docker-compose.yml`
- Downloads Python 3.11-slim base image (if not cached)
- Installs all dependencies from `requirements.txt`
- Copies your application code into the image
- Creates a reusable image named `distributed-kv-store-kv-node`

**When to run:**
- First time setting up
- After changing `Dockerfile` or `requirements.txt`
- After adding new Python dependencies

**Output you'll see:**
```
[+] Building 4.0s (13/13) FINISHED
 => [1/5] FROM docker.io/library/python:3.11-slim
 => [2/5] WORKDIR /app
 => [3/5] COPY requirements.txt .
 => [4/5] RUN pip install --no-cache-dir -r requirements.txt
 => [5/5] COPY app/ ./app/
 => exporting to image
```

---

### Step 2: Start the Container

**Command:**
```bash
docker-compose up
```

**What it does:**
- Creates a new container from your image
- Maps port 8000 (container) → 8000 (your machine)
- Mounts volumes for live code reload
- Starts uvicorn server inside the container

**Flags:**
- `docker-compose up` - Run in foreground (see logs in terminal)
- `docker-compose up -d` - Run in background/detached mode

**Output you'll see:**
```
[+] Running 1/1
 ✔ Container distributed-kv-store-kv-node-1 Started
INFO:     Started server process [1]
INFO:     Uvicorn running on http://0.0.0.0:8000
```

---

### Step 3: Test the Running Container

**Health check:**
```bash
curl http://localhost:8000/health
```

**Expected:**
```json
{"status":"healthy","node_id":"node-1","keys_stored":0}
```

**Store a value:**
```bash
curl -X PUT http://localhost:8000/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"hello from Docker!"}'
```

**Retrieve it:**
```bash
curl http://localhost:8000/kv/mykey
```

**Expected:**
```json
{"value":"hello from Docker!"}
```

---

### Step 4: View Logs

**See container logs:**
```bash
docker-compose logs
```

**Follow logs in real-time:**
```bash
docker-compose logs -f
```

**See last 50 lines:**
```bash
docker-compose logs --tail=50
```

---

### Step 5: Stop the Container

**Stop and remove containers:**
```bash
docker-compose down
```

**What it does:**
- Stops running containers
- Removes containers (but keeps images)
- Removes networks created by compose

**Note:** Your data in volumes (`./data`) persists!

---

## 🔄 Common Docker Workflows

### Development Workflow (Live Reload)

Thanks to volume mounts in `docker-compose.yml`, code changes are reflected immediately:

**Terminal 1:**
```bash
docker-compose up
```

**Terminal 2 - Edit code:**
```bash
# Edit app/main.py in your editor
# Uvicorn auto-reloads when it detects changes
```

**Important:** Volume mounts only work for file changes, not dependency changes. If you modify `requirements.txt`, rebuild:
```bash
docker-compose down
docker-compose build
docker-compose up
```

---

### Rebuild After Changes

**When to rebuild:**
- Changed `requirements.txt`
- Changed `Dockerfile`
- Added new Python packages

**Quick rebuild and restart:**
```bash
docker-compose down
docker-compose build
docker-compose up -d
```

**Or in one command:**
```bash
docker-compose up --build -d
```

---

### Check Container Status

**See running containers:**
```bash
docker ps
```

**See all containers (including stopped):**
```bash
docker ps -a
```

**See images:**
```bash
docker images
```

---

### Execute Commands Inside Container

**Open a shell inside the running container:**
```bash
docker-compose exec kv-node /bin/bash
```

**Run a one-off command:**
```bash
docker-compose exec kv-node python -c "print('Hello from inside container')"
```

**Run tests inside container:**
```bash
docker-compose exec kv-node pytest -v
```

---

### Clean Up

**Remove containers and networks:**
```bash
docker-compose down
```

**Remove everything including volumes (⚠️ deletes WAL data):**
```bash
docker-compose down -v
```

**Remove unused images and free disk space:**
```bash
docker system prune
```

**Remove specific image:**
```bash
docker rmi distributed-kv-store-kv-node
```

---

## 📁 Understanding Volume Mounts

In `docker-compose.yml`:

```yaml
volumes:
  - ./app:/app/app        # Live reload for code
  - ./data:/app/data      # Persistent WAL storage
```

**What this means:**
- Changes to files in `./app` on your machine are immediately visible in `/app/app` inside the container
- Data written to `/app/data` inside the container is saved to `./data` on your machine
- Even if you delete the container, `./data` survives

---

## 🔍 Troubleshooting

### Port already in use

**Error:** `Bind for 0.0.0.0:8000 failed: port is already allocated`

**Fix:**
```bash
# Find what's using port 8000
lsof -i :8000

# Kill the process or change port in docker-compose.yml
ports:
  - "8001:8000"  # Use 8001 on host instead
```

---

### Container won't start

**Check logs:**
```bash
docker-compose logs
```

**Common issues:**
- Syntax error in Python code
- Missing dependencies (rebuild image)
- Port conflict (see above)

---

### Image build fails

**Clear cache and rebuild:**
```bash
docker-compose build --no-cache
```

---

### Changes not reflecting

**If code changes aren't appearing:**
1. Make sure you're editing files in `./app`, not inside the container
2. Check volume mounts in `docker-compose.yml`
3. Restart container: `docker-compose restart`

---

## 🚀 Phase 1 Preview: Multi-Node Setup

In Phase 3, `docker-compose.yml` will look like this:

```yaml
services:
  kv-node-1:
    build: .
    ports: ["8001:8000"]
    environment:
      - NODE_ID=node-1
      
  kv-node-2:
    build: .
    ports: ["8002:8000"]
    environment:
      - NODE_ID=node-2
      
  kv-node-3:
    build: .
    ports: ["8003:8000"]
    environment:
      - NODE_ID=node-3
```

**Start 3 nodes:**
```bash
docker-compose up -d
```

**Access different nodes:**
```bash
curl http://localhost:8001/health  # node-1
curl http://localhost:8002/health  # node-2
curl http://localhost:8003/health  # node-3
```

---

## 📝 Quick Reference

| Task | Command |
|------|---------|
| Build image | `docker-compose build` |
| Start containers | `docker-compose up -d` |
| View logs | `docker-compose logs -f` |
| Stop containers | `docker-compose down` |
| Rebuild & restart | `docker-compose up --build -d` |
| Shell into container | `docker-compose exec kv-node /bin/bash` |
| Run tests | `docker-compose exec kv-node pytest -v` |
| Check status | `docker ps` |
| Clean up | `docker system prune` |

---

## 🎓 Key Takeaways

1. **Docker = Consistency** - Same environment on dev, test, and prod
2. **Images are templates** - Containers are running instances
3. **Volume mounts** - Enable live development and data persistence
4. **docker-compose** - Simplifies multi-container orchestration
5. **Rebuild when needed** - Dependency changes require rebuilding the image

You're now ready to use Docker for development and testing!

# Distributed Key-Value Store

A distributed, sharded, in-memory key-value store built with Python, FastAPI, and Docker. This project demonstrates distributed systems concepts including consistent hashing, leader election, write-ahead logging (WAL), and fault tolerance.

## 🎯 Project Goals

This is a portfolio project showcasing:
- **Sharding** with consistent hashing and virtual nodes
- **Leader-based coordination** for write consistency
- **Fault tolerance** with heartbeat monitoring and leader election
- **Durability** through Write-Ahead Logs (WAL)
- **Distributed deployment** using Docker Compose

## 📁 Project Structure

```
distributed-kv-store/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application and endpoints
│   ├── api/                 # API layer (future)
│   ├── storage/             # Storage engine and WAL (future)
│   ├── cluster/             # Distributed coordination (future)
│   └── utils/               # Helper utilities
├── tests/
│   ├── unit/                # Unit tests
│   └── integration/         # Integration and chaos tests
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Git

### Local Development Setup

1. **Create and activate virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the server locally**:
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```

4. **Test the API**:
   ```bash
   # Health check
   curl http://localhost:8000/health

   # Store a value
   curl -X PUT http://localhost:8000/kv/mykey \
     -H "Content-Type: application/json" \
     -d '{"key": "mykey", "value": "hello world"}'

   # Get a value
   curl http://localhost:8000/kv/mykey

   # Delete a value
   curl -X DELETE http://localhost:8000/kv/mykey
   ```

### Docker Setup

1. **Build the Docker image**:
   ```bash
   docker-compose build
   ```

2. **Run the container**:
   ```bash
   docker-compose up
   ```

3. **Access the API** at `http://localhost:8000`

4. **View logs**:
   ```bash
   docker-compose logs -f
   ```

5. **Stop the container**:
   ```bash
   docker-compose down
   ```

## 🧪 Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app tests/

# Run specific test file
pytest tests/unit/test_storage.py
```

## 📊 Development Phases

- ✅ **Phase 0**: Project setup and structure
- 🚧 **Phase 1**: Single-node core with WAL (in progress)
- ⏳ **Phase 2**: Consistent hashing and sharding
- ⏳ **Phase 3**: Multi-node cluster coordination
- ⏳ **Phase 4**: Leader election and heartbeats
- ⏳ **Phase 5**: WAL recovery and consistency
- ⏳ **Phase 6**: Testing and documentation

## 🛠️ Technology Stack

- **Language**: Python 3.11
- **Web Framework**: FastAPI
- **Async Runtime**: AsyncIO
- **Testing**: Pytest
- **Containerization**: Docker, Docker Compose
- **Type Checking**: Python type hints with Pydantic

## 📝 API Documentation

Once running, visit:
- Interactive API docs: `http://localhost:8000/docs`
- Alternative docs: `http://localhost:8000/redoc`

## 🎓 Learning Resources

This project implements concepts from:
- Consistent hashing (similar to DynamoDB, Cassandra)
- Leader-based replication (simplified Raft/Paxos patterns)
- Write-Ahead Logging (PostgreSQL, Kafka)
- Distributed systems fault tolerance

## 📄 License

This is a portfolio/learning project - feel free to use and modify as needed.

## 🙏 Acknowledgments

Built as a portfolio project to demonstrate distributed systems and backend engineering skills for infrastructure and AI-infrastructure roles.

"""
Distributed Key-Value Store - Main FastAPI Application

This is the entry point for our distributed KV store.
"""
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from contextlib import asynccontextmanager
import logging
import os

from app.cluster.shard_manager import ShardManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global storage instance (now manages multiple shards)
storage: ShardManager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for FastAPI application.
    
    Handles startup (initialize shards, WAL replay) and shutdown (graceful close).
    """
    global storage
    
    # Startup: Initialize shard manager
    num_shards = int(os.getenv("NUM_SHARDS", "3"))
    data_dir = os.getenv("DATA_DIR", "data")
    
    shard_ids = [f"shard-{i}" for i in range(num_shards)]
    storage = ShardManager(shard_ids, data_dir)
    await storage.initialize()
    
    total_keys = await storage.size()
    logger.info(f"✅ ShardManager ready with {num_shards} shards, {total_keys} total keys")
    
    yield
    
    # Shutdown: Close all shards gracefully
    await storage.close()
    logger.info("✅ ShardManager closed")


app = FastAPI(
    title="Distributed Key-Value Store",
    description="A distributed, sharded, in-memory key-value store with WAL support",
    version="0.1.0",
    lifespan=lifespan
)

# Simple in-memory store has been replaced by StorageEngine with WAL


class KeyValue(BaseModel):
    """Request/response model for key-value operations"""
    key: str
    value: str


class ValueResponse(BaseModel):
    """Response model for GET operations"""
    value: str


@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring and container orchestration.
    Returns basic node information.
    """
    node_id = os.getenv("NODE_ID", "unknown")
    return {
        "status": "healthy",
        "node_id": node_id,
        "keys_stored": await storage.size()
    }


@app.get("/kv/{key}", response_model=ValueResponse)
async def get_value(key: str):
    """
    Retrieve a value by key.
    
    Args:
        key: The key to look up
        
    Returns:
        The stored value
        
    Raises:
        404: If key doesn't exist
    """
    value = await storage.get(key)
    
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key '{key}' not found"
        )
    
    logger.info(f"GET key='{key}'")
    return ValueResponse(value=value)


@app.put("/kv/{key}")
async def put_value(key: str, request: KeyValue):
    """
    Store or update a key-value pair.
    
    Args:
        key: The key to store (from URL)
        request: KeyValue containing key and value (value is used)
        
    Returns:
        Success message with the stored key
    """
    # Note: We use key from URL path, not from request body
    # This is intentional for RESTful design
    await storage.put(key, request.value)
    logger.info(f"PUT key='{key}', value_length={len(request.value)}")
    
    return {
        "message": "success",
        "key": key
    }


@app.delete("/kv/{key}")
async def delete_value(key: str):
    """
    Delete a key-value pair.
    
    Args:
        key: The key to delete
        
    Returns:
        Success message
        
    Raises:
        404: If key doesn't exist
    """
    deleted = await storage.delete(key)
    
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key '{key}' not found"
        )
    
    logger.info(f"DELETE key='{key}'")
    
    return {
        "message": "deleted",
        "key": key
    }


@app.get("/stats")
async def get_stats():
    """
    Get shard distribution statistics.
    
    Shows how keys are distributed across shards and virtual node counts.
    """
    return await storage.get_stats()


@app.get("/")
async def root():
    """Root endpoint with basic info"""
    num_shards = len(storage.shards) if storage else 0
    return {
        "service": "Distributed KV Store",
        "version": "0.2.0",
        "sharding": {
            "enabled": True,
            "num_shards": num_shards
        },
        "endpoints": {
            "health": "/health",
            "stats": "/stats",
            "get": "GET /kv/{key}",
            "put": "PUT /kv/{key}",
            "delete": "DELETE /kv/{key}"
        }
    }

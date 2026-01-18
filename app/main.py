"""
Distributed Key-Value Store - Main FastAPI Application

This is the entry point for our distributed KV store.
"""
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from contextlib import asynccontextmanager
import logging
import os

from app.storage.engine import StorageEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global storage engine instance
storage: StorageEngine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager for FastAPI application.
    
    Handles startup (WAL replay) and shutdown (graceful close) events.
    """
    global storage
    
    # Startup: Initialize storage and replay WAL
    wal_path = os.getenv("WAL_PATH", "data/node.wal")
    storage = StorageEngine(wal_path)
    await storage.initialize()
    logger.info(f"✅ Storage engine ready with {await storage.size()} keys")
    
    yield
    
    # Shutdown: Close storage gracefully
    await storage.close()
    logger.info("✅ Storage engine closed")


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


@app.get("/")
async def root():
    """Root endpoint with basic info"""
    return {
        "service": "Distributed KV Store",
        "version": "0.1.0",
        "endpoints": {
            "health": "/health",
            "get": "GET /kv/{key}",
            "put": "PUT /kv/{key}",
            "delete": "DELETE /kv/{key}"
        }
    }

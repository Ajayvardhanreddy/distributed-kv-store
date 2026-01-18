"""
Distributed Key-Value Store - Main FastAPI Application

This is the entry point for our distributed KV store.
"""
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Distributed Key-Value Store",
    description="A distributed, sharded, in-memory key-value store with WAL support",
    version="0.1.0"
)

# Simple in-memory store (will be moved to a separate module later)
store: dict[str, str] = {}


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
        "keys_stored": len(store)
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
    if key not in store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key '{key}' not found"
        )
    
    logger.info(f"GET key='{key}'")
    return ValueResponse(value=store[key])


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
    store[key] = request.value
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
    if key not in store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key '{key}' not found"
        )
    
    del store[key]
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

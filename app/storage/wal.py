"""
Write-Ahead Log (WAL) implementation for crash recovery.

The WAL ensures durability by logging all mutations before they're applied
to the in-memory store. On restart, we replay the log to restore state.

Format: JSON-lines (one operation per line)
Example:
    {"op":"PUT","key":"user:1","value":"alice","ts":1705612800}
    {"op":"DELETE","key":"user:1","ts":1705612802}
"""
import json
import os
from typing import Optional
import aiofiles
import asyncio
import time
import logging

logger = logging.getLogger(__name__)


class WriteAheadLog:
    """
    Append-only log for recording storage mutations.
    
    Each mutation (PUT/DELETE) is written to the log before being applied
    to memory. On restart, replay() reconstructs the current state.
    
    Thread-safe through file system append semantics and async lock.
    """
    
    def __init__(self, file_path: str):
        """
        Initialize WAL at the specified path.
        
        Args:
            file_path: Path to the WAL file (e.g., "data/node.wal")
        """
        self.file_path = file_path
        self.lock = asyncio.Lock()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Create file if it doesn't exist
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                pass
            logger.info(f"Created new WAL file: {file_path}")
        else:
            logger.info(f"Using existing WAL file: {file_path}")
    
    async def append(self, operation: str, key: str, value: Optional[str] = None) -> None:
        """
        Append an operation to the log.
        
        Args:
            operation: "PUT" or "DELETE"
            key: The key being modified
            value: The value (required for PUT, None for DELETE)
            
        The entry includes a timestamp for debugging and ordering.
        """
        async with self.lock:
            entry = {
                "op": operation,
                "key": key,
                "ts": int(time.time())
            }
            
            if value is not None:
                entry["value"] = value
            
            # Write as JSON line (newline-delimited JSON)
            line = json.dumps(entry) + "\n"
            
            async with aiofiles.open(self.file_path, mode='a') as f:
                await f.write(line)
            
            logger.debug(f"WAL: {operation} key={key}")
    
    async def replay(self) -> dict[str, str]:
        """
        Replay the log to reconstruct current state.
        
        Reads all entries and applies them in order to build the current
        key-value mapping.
        
        Returns:
            Dictionary mapping keys to values (current state)
            
        Handles corrupted lines gracefully by logging warnings.
        """
        state: dict[str, str] = {}
        line_number = 0
        
        async with aiofiles.open(self.file_path, mode='r') as f:
            async for line in f:
                line_number += 1
                line = line.strip()
                
                if not line:
                    continue
                
                try:
                    entry = json.loads(line)
                    op = entry["op"]
                    key = entry["key"]
                    
                    if op == "PUT":
                        state[key] = entry["value"]
                    elif op == "DELETE":
                        state.pop(key, None)  # Remove if exists
                    else:
                        logger.warning(f"Unknown operation '{op}' at line {line_number}")
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Corrupted WAL entry at line {line_number}: {e}")
                    # Continue with next line - don't crash on corrupted data
                except KeyError as e:
                    logger.warning(f"Invalid WAL entry at line {line_number}: missing {e}")
        
        logger.info(f"WAL replay complete: {len(state)} keys from {line_number} entries")
        return state
    
    async def close(self) -> None:
        """
        Close the WAL.
        
        Currently a no-op since we don't keep file handles open,
        but provided for API consistency and future optimizations.
        """
        logger.debug("WAL closed")

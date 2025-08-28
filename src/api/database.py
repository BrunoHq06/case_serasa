"""
Database Connection Management for Transaction API

This module handles database connectivity and configuration for the transaction API.
It provides a centralized way to manage DuckDB connections and ensures proper
database initialization.

Features:
    - Automatic database path resolution
    - Database directory creation
    - Connection management for FastAPI dependency injection
"""

from pathlib import Path
import duckdb

# Absolute path to the database
DB_PATH = Path(__file__).resolve().parent.parent / "database" / "transactions.duckdb"

# Ensure the database folder exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_connection():
    """
    Returns a DuckDB connection.
    
    Creates and returns a new DuckDB connection to the transactions database.
    The connection should be properly closed by the caller or used in a context manager.
    
    Returns:
        duckdb.Connection: A DuckDB database connection
        
    Note:
        Can be used with FastAPI Depends for dependency injection.
        The connection is not automatically closed - use with context manager
        or ensure proper cleanup in your code.
        
    Example:
        # Using as context manager (recommended)
        with get_connection() as con:
            result = con.execute("SELECT * FROM transactions").fetchall()
            
        # Using with FastAPI dependency
        def my_endpoint(con = Depends(get_connection)):
            result = con.execute("SELECT * FROM transactions").fetchall()
            return result
    """
    return duckdb.connect(str(DB_PATH))
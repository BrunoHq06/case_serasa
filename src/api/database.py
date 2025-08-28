"""
Database Connection Management for Transaction API

This module handles database connectivity and configuration for the transaction API.
It provides a centralized way to manage DuckDB connections and ensures proper
database initialization.

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
    """
    return duckdb.connect(str(DB_PATH))

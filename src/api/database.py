from pathlib import Path
import duckdb

# Absolute path to the database
DB_PATH = Path(__file__).resolve().parent.parent / "database" / "transactions.duckdb"

# Ensure the database folder exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_connection():
    """
    Returns a DuckDB connection.
    Can be used with FastAPI Depends.
    """
    return duckdb.connect(str(DB_PATH))
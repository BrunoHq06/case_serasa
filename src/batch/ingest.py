"""
Batch Data Ingestion Utilities

This module provides functionality for batch ingestion of transaction data from
partitioned Parquet files into DuckDB. It handles data validation, schema
enforcement, and bulk data loading operations.

Features:
    - Automatic table creation based on schema definitions
    - Column validation and ordering
    - Support for partitioned Parquet data
    - Bulk insertion with DuckDB optimization
"""

import duckdb
from pathlib import Path

def ingest_parquet_to_db(db_path: str, parquet_path: str, definition: dict):
    """
    Ingest partitioned Parquet data into a DuckDB database table.
    
    This function reads partitioned Parquet files and loads them into a DuckDB table
    according to the provided schema definition. It performs validation on the data
    structure and ensures proper column ordering before insertion.
    
    Args:
        db_path (str): Path to the DuckDB database file
        parquet_path (str): Path to the directory containing partitioned Parquet files
        definition (dict): Schema definition containing:
            - 'table_name': Name of the target table
            - 'base_query': SQL for table creation
            - 'expected_columns': List of expected column names in order
            
    Raises:
        FileNotFoundError: If the Parquet path doesn't exist
        ValueError: If required columns are missing or if 'id'/'created_at' columns
                   are present in input data (these are auto-generated)
                   
    Example:
        from src.batch.definitions import TRANSACTION
        
        ingest_parquet_to_db(
            db_path="path/to/database.duckdb",
            parquet_path="path/to/partitioned_data/",
            definition=TRANSACTION
        )
        
    Note:
        - The function automatically creates the target table if it doesn't exist
        - Input data should not contain 'id' or 'created_at' columns
        - Column names are case-sensitive and must match exactly
        - Data is inserted in the order specified by 'expected_columns'
    """
    # Reading attributes from definition file
    expected_columns = definition['expected_columns']
    base_query = definition['base_query']    
    table_name = definition['table_name']

    # Ensure path exists
    if not Path(parquet_path).exists():
        raise FileNotFoundError(f"Path for {parquet_path} does not exist.")

    # connect to DuckDB, if it doesn't exist, it will be created.
    con = duckdb.connect(db_path)

    # path to all parquet files, used to read partitioned data
    scan_path = str(Path(parquet_path) / "*/*.parquet")

    # Lazy reading of parquet files
    df = con.execute(f"SELECT * FROM read_parquet('{scan_path}')").df()

    # columns validation
    existing_cols = df.columns.tolist()
    missing_cols = [c.lower() for c in expected_columns if c not in existing_cols]
    if missing_cols:
        raise ValueError(f"missing columns: {missing_cols}")

    if "id" in existing_cols or "created_at" in existing_cols:
        raise ValueError(" 'id' or/and 'created_at' should not be in the input data. Since they are auto-generated.")

    # Rearrange columns to match the expected order
    df = df[expected_columns]

    # Create table if not exists
    con.execute(base_query)

    # insert data into table
    con.register("temp_df", df)
    con.execute(f"""
        INSERT INTO {table_name} ({", ".join(expected_columns)})
        SELECT {", ".join(expected_columns)} FROM temp_df
    """)

    con.close()
    print(f"Ingestion done for {table_name}, {len(df)} lines processed.")
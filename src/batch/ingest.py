import duckdb
from pathlib import Path

def ingest_parquet_to_db(db_path: str, parquet_path: str, definition: dict):

    # Reading attributes from definition file
    expected_columns = definition['expected_columns']
    base_query = definition['base_query']    
    table_name = definition['table_name']

    # Ensure path exists
    if not Path(parquet_path).exists():
        raise FileNotFoundError(f"O caminho {parquet_path} não existe.")

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
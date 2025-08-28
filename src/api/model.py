"""
Database Schema Definitions for Transaction Management

This module contains SQL queries and schema definitions for the transaction database.
It defines the table structure, sequences, and DDL statements used to initialize
the database schema.

Constants:
    - CREATE_SEQUENCE_QUERY: SQL to create the transaction ID sequence
    - BASE_TABLE_QUERY: SQL to create the main transactions table
"""

CREATE_SEQUENCE_QUERY = "CREATE SEQUENCE IF NOT EXISTS transaction_id_seq START 1 INCREMENT BY 1;"

# Creating table if not exist
BASE_TABLE_QUERY = ("""
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY DEFAULT nextval('transaction_id_seq'),
    time INTEGER,
    v1 FLOAT,
    v2 FLOAT,
    v3 FLOAT,
    v4 FLOAT,
    v5 FLOAT,
    v6 FLOAT,
    v7 FLOAT,
    v8 FLOAT,
    v9 FLOAT,
    v10 FLOAT,
    v11 FLOAT,
    v12 FLOAT,
    v13 FLOAT,
    v14 FLOAT,
    v15 FLOAT,
    v16 FLOAT,
    v17 FLOAT,
    v18 FLOAT,
    v19 FLOAT,
    v20 FLOAT,
    v21 FLOAT,
    v22 FLOAT,
    v23 FLOAT,
    v24 FLOAT,
    v25 FLOAT,
    v26 FLOAT,
    v27 FLOAT,
    v28 FLOAT,
    amount FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")


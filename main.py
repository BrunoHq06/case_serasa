"""
Main Application Entry Point and Demo Script

This module serves as both the main entry point for the transaction management system
and a demonstration script showcasing the API functionality. It demonstrates CRUD
operations and batch data ingestion capabilities.

Features:
    - API endpoint testing with random transaction data
    - Complete CRUD operation demonstration
    - Batch data ingestion from partitioned Parquet files
    - Random data generation for testing purposes

Usage:
    Run this script to test the complete transaction API workflow:
    1. Create multiple transactions
    2. Read, update, and delete transactions
    3. Perform batch ingestion of Parquet data
"""

import requests
import random
from src.batch.ingest import ingest_parquet_to_db
from src.batch.definitions import TRANSACTION

API_URL = "http://localhost:8000/transactions"

def random_positive_payload():
    """
    Generate a random transaction payload with positive values.
    
    Creates a dictionary containing random transaction data with:
    - Random integer time value
    - 28 random float feature variables (v1-v28)
    - Random float amount value
    
    Returns:
        dict: Random transaction data payload
        
    Note:
        All feature values are positive floats between 0.1 and 5.0
        Amount values are between 10.0 and 1000.0
        Time values are random integers between 1 and 100000
    """
    # All values positive, time is int, amount is float
    return {
        "time": random.randint(1, 100000),
        "v1": random.uniform(0.1, 5.0),
        "v2": random.uniform(0.1, 5.0),
        "v3": random.uniform(0.1, 5.0),
        "v4": random.uniform(0.1, 5.0),
        "v5": random.uniform(0.1, 5.0),
        "v6": random.uniform(0.1, 5.0),
        "v7": random.uniform(0.1, 5.0),
        "v8": random.uniform(0.1, 5.0),
        "v9": random.uniform(0.1, 5.0),
        "v10": random.uniform(0.1, 5.0),
        "v11": random.uniform(0.1, 5.0),
        "v12": random.uniform(0.1, 5.0),
        "v13": random.uniform(0.1, 5.0),
        "v14": random.uniform(0.1, 5.0),
        "v15": random.uniform(0.1, 5.0),
        "v16": random.uniform(0.1, 5.0),
        "v17": random.uniform(0.1, 5.0),
        "v18": random.uniform(0.1, 5.0),
        "v19": random.uniform(0.1, 5.0),
        "v20": random.uniform(0.1, 5.0),
        "v21": random.uniform(0.1, 5.0),
        "v22": random.uniform(0.1, 5.0),
        "v23": random.uniform(0.1, 5.0),
        "v24": random.uniform(0.1, 5.0),
        "v25": random.uniform(0.1, 5.0),
        "v26": random.uniform(0.1, 5.0),
        "v27": random.uniform(0.1, 5.0),
        "v28": random.uniform(0.1, 5.0),
        "amount": round(random.uniform(10.0, 1000.0), 2)
    }

# Main demonstration workflow
if __name__ == "__main__":
    print("Starting Transaction API Demo...")
    
    # Step 1: Create multiple transactions
    print("\n=== Creating Transactions ===")
    created_ids = []
    for i in range(3):
        payload = random_positive_payload()
        resp = requests.post(API_URL, json=payload)
        print(f"Created transaction {i+1}:", resp.json())
        created_ids.append(resp.json()['id'])

    # Step 2: Work with the last created transaction
    created_id = created_ids[-1]
    print(f"\n=== Working with Transaction ID: {created_id} ===")

    # Read created transaction
    print("\n--- Reading Transaction ---")
    read_url = f"{API_URL}/{created_id}"
    resp = requests.get(read_url)
    print("Read:", resp.json())

    # Update the transaction
    print("\n--- Updating Transaction ---")
    update_url = f"{API_URL}/{created_id}"
    update_payload = {
        "amount": round(random.uniform(1000.0, 5000.0), 2)
    }
    resp = requests.put(update_url, json=update_payload)
    print("Updated:", resp.json())

    # Delete the transaction
    print("\n--- Deleting Transaction ---")
    delete_url = f"{API_URL}/{created_id}"
    resp = requests.delete(delete_url)
    print("Deleted:", resp.status_code, resp.text)

    # Step 3: Batch ingestion
    print("\n=== Batch Data Ingestion ===")
    try:
        ingest_parquet_to_db(
            db_path="src/database/batch_ingestions.duckdb",
            parquet_path="src/datasets/partitioned_data/",
            definition=TRANSACTION
        )
        print("Batch ingestion completed successfully!")
    except Exception as e:
        print(f"Batch ingestion failed: {e}")
    
    print("\nDemo completed!")
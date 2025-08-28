import requests
import random
from src.batch.ingest import ingest_parquet_to_db
from src.batch.definitions import TRANSACTION

API_URL = "http://localhost:8000/transactions"

def random_positive_payload():
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

created_ids = []
for _ in range(3):
    payload = random_positive_payload()
    resp = requests.post(API_URL, json=payload)
    print("Created:", resp.json())
    created_ids.append(resp.json()['id'])

# Work with the last created transaction
created_id = created_ids[-1]

# Read created transaction
read_url = f"{API_URL}/{created_id}"
resp = requests.get(read_url)
print("Read:", resp.json())

# Update the transaction
update_url = f"{API_URL}/{created_id}"
update_payload = {
    "amount": round(random.uniform(1000.0, 5000.0), 2)
}
resp = requests.put(update_url, json=update_payload)
print("Updated:", resp.json())

# Delete the transaction
delete_url = f"{API_URL}/{created_id}"
resp = requests.delete(delete_url)
print("Deleted:", resp.status_code, resp.text)

# Batch ingestion
ingest_parquet_to_db(
    db_path="src/database/transactions.duckdb",
    parquet_path="src/datasets/partitioned_data/",
    definition=TRANSACTION
)
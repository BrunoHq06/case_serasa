"""
API Testing Module

This module contains test cases for the Transaction API endpoints.
It provides comprehensive testing of all CRUD operations 
to ensure the API functions correctly.

"""

from fastapi.testclient import TestClient
from .api import app, get_db
import duckdb
from .model import BASE_TABLE_QUERY, CREATE_SEQUENCE_QUERY
from pathlib import Path

# -------------------------------
# Setup temporary in-memory DB for tests
# -------------------------------
 # in-memory database for fast tests


conn = duckdb.connect()

# Create sequence and table, same as API startup
conn.execute(CREATE_SEQUENCE_QUERY)
conn.execute(BASE_TABLE_QUERY)

# Override get_db dependency to use the temporary in-memory DB
def override_get_db():
    try:
        yield conn
    finally:
        pass

app.dependency_overrides = {}
app.dependency_overrides['get_db'] = override_get_db

client = TestClient(app)

# -------------------------------
# Helper payload
# -------------------------------
def make_payload(amount=100.0):
    return {
        "time": 1,
        "v1": 0.1, "v2": 0.2, "v3": 0.3, "v4": 0.4, "v5": 0.5,
        "v6": 0.6, "v7": 0.7, "v8": 0.8, "v9": 0.9, "v10": 1.0,
        "v11": 1.1, "v12": 1.2, "v13": 1.3, "v14": 1.4, "v15": 1.5,
        "v16": 1.6, "v17": 1.7, "v18": 1.8, "v19": 1.9, "v20": 2.0,
        "v21": 2.1, "v22": 2.2, "v23": 2.3, "v24": 2.4, "v25": 2.5,
        "v26": 2.6, "v27": 2.7, "v28": 2.8,
        "amount": amount
    }

# -------------------------------
# CRUD Tests
# -------------------------------

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Transaction API", "version": "1.0.0"}

def test_create_transaction():
    payload = make_payload()
    r = client.post("/transactions", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert "id" in data
    assert data["amount"] == 100.0

def test_read_transactions():
    # Ensure at least one transaction exists
    client.post("/transactions", json=make_payload())
    r = client.get("/transactions?limit=10")
    assert r.status_code == 200
    assert isinstance(r.json(), list)
    assert len(r.json()) >= 1

def test_read_transaction():
    payload = make_payload()
    r = client.post("/transactions", json=payload)
    transaction_id = r.json()["id"]
    r2 = client.get(f"/transactions/{transaction_id}")
    assert r2.status_code == 200
    data = r2.json()
    assert data["id"] == transaction_id

def test_read_transaction_not_found():
    r = client.get("/transactions/9999")
    assert r.status_code == 404
    assert r.json()["detail"] == "Transaction not found"

def test_update_transaction():
    payload = make_payload()
    r = client.post("/transactions", json=payload)
    transaction_id = r.json()["id"]

    r2 = client.put(f"/transactions/{transaction_id}", json={"amount": 200.0})
    assert r2.status_code == 200
    data = r2.json()
    assert data["amount"] == 200.0

def test_update_transaction_no_fields():
    payload = make_payload()
    r = client.post("/transactions", json=payload)
    transaction_id = r.json()["id"]

    r2 = client.put(f"/transactions/{transaction_id}", json={})
    assert r2.status_code == 400
    assert r2.json()["detail"] == "No fields to update"

def test_update_transaction_not_found():
    r2 = client.put("/transactions/9999", json={"amount": 200.0})
    assert r2.status_code == 404
    assert r2.json()["detail"] == "No transaction found after update"

def test_delete_transaction():
    payload = make_payload()
    r = client.post("/transactions", json=payload)
    transaction_id = r.json()["id"]

    r2 = client.delete(f"/transactions/{transaction_id}")
    assert r2.status_code == 204

    # Confirm deletion
    r3 = client.get(f"/transactions/{transaction_id}")
    assert r3.status_code == 404

def test_delete_transaction_not_found():
    r = client.delete("/transactions/9999")
    assert r.status_code == 204

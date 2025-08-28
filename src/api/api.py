"""
FastAPI Application for Transaction Management

This module provides a REST API for managing financial transactions with the following features:
- CRUD operations for transactions
- Automatic database initialization
- Data validation using Pydantic schemas
- DuckDB integration for data storage

Endpoints:
    - GET /: API information and version
    - POST /transactions: Create a new transaction
    - GET /transactions: List all transactions with pagination
    - GET /transactions/{id}: Get a specific transaction
    - PUT /transactions/{id}: Update a transaction
    - DELETE /transactions/{id}: Delete a transaction
"""

from fastapi import FastAPI, HTTPException, Depends
from typing import List
from . import schemas
from .database import get_connection
from .model import BASE_TABLE_QUERY, CREATE_SEQUENCE_QUERY
import uvicorn


app = FastAPI(title="Transaction API", version="1.0.0")

# Initialize DB at startup
@app.on_event("startup")
def startup_event():
    """
    Initialize database tables and sequences on application startup.
    
    Creates the transactions table and sequence if they don't exist.
    """
    with get_connection() as con:
        con.execute(CREATE_SEQUENCE_QUERY)
        con.execute(BASE_TABLE_QUERY)

# Dependency
def get_db():
    """
    Database connection dependency for FastAPI endpoints.
    
    Yields:
        duckdb.Connection: A DuckDB database connection
        
    Note:
        Connection is automatically closed after the request is processed.
    """
    con = get_connection()
    try:
        yield con
    finally:
        con.close()

#################
### UTILITIES ###
#################

def convert_to_db_row(transaction: schemas.TransactionCreate):
    """
    Convert a Pydantic transaction model to database row format.
    
    Args:
        transaction: TransactionCreate model instance
        
    Returns:
        tuple: (values_list, fields_list) for database insertion
    """
    data = transaction.model_dump(exclude={"id", "created_at"})
    return list(data.values()), list(data.keys())

def convert_to_response(row, columns):
    """
    Convert a database row to a dictionary response.
    
    Args:
        row: Database row tuple
        columns: List of column names
        
    Returns:
        dict: Dictionary with column names as keys and row values as values
    """
    return dict(zip(columns, row))

#################
### ENDPOINTS ###
#################

@app.get("/")
def read_root():
    """
    Root endpoint providing API information.
    
    Returns:
        dict: API name and version information
    """
    return {"message": "Transaction API", "version": "1.0.0"}

@app.post("/transactions", response_model=schemas.TransactionResponse)
def create_transaction(transaction: schemas.TransactionCreate, con = Depends(get_db)):
    """
    Create a new transaction record.
    
    Args:
        transaction: Transaction data from request body
        con: Database connection dependency
        
    Returns:
        TransactionResponse: Created transaction with generated ID and timestamp
        
    Raises:
        HTTPException: If database operation fails
    """
    values, fields = convert_to_db_row(transaction)
    placeholders = ", ".join(["?"] * len(values))
    con.execute(f"INSERT INTO transactions ({', '.join(fields)}) VALUES ({placeholders})", values)
    last_id = con.execute("SELECT max(id) FROM transactions").fetchone()[0]
    row = con.execute("SELECT * FROM transactions WHERE id = ?", [last_id]).fetchone()
    return convert_to_response(row, [d[0] for d in con.description])

@app.get("/transactions", response_model=List[schemas.TransactionResponse])
def read_transactions(limit: int = 100, con = Depends(get_db)):
    """
    Retrieve a list of transactions with optional pagination.
    
    Args:
        limit: Maximum number of transactions to return (default: 100)
        con: Database connection dependency
        
    Returns:
        List[TransactionResponse]: List of transaction records
        
    Raises:
        HTTPException: If database operation fails
    """
    rows = con.execute("SELECT * FROM transactions LIMIT ?", [limit]).fetchall()
    columns = [d[0] for d in con.description]
    return [convert_to_response(r, columns) for r in rows]

@app.get("/transactions/{transaction_id}", response_model=schemas.TransactionResponse)
def read_transaction(transaction_id: int, con = Depends(get_db)):
    """
    Retrieve a specific transaction by ID.
    
    Args:
        transaction_id: Unique identifier of the transaction
        con: Database connection dependency
        
    Returns:
        TransactionResponse: Transaction record
        
    Raises:
        HTTPException: If transaction is not found (404)
    """
    row = con.execute("SELECT * FROM transactions WHERE id = ?", [transaction_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return convert_to_response(row, [d[0] for d in con.description])

@app.put("/transactions/{transaction_id}", response_model=schemas.TransactionResponse)
def update_transaction(transaction_id: int, transaction_update: schemas.TransactionUpdate, con = Depends(get_db)):
    """
    Update an existing transaction record.
    
    Args:
        transaction_id: Unique identifier of the transaction to update
        transaction_update: Partial transaction data for update
        con: Database connection dependency
        
    Returns:
        TransactionResponse: Updated transaction record
        
    Raises:
        HTTPException: If no fields to update (400) or transaction not found (404)
    """
    update_data = {k: v for k, v in transaction_update.model_dump(exclude_unset=True).items()}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")
    set_clause = ", ".join([f"{k} = ?" for k in update_data.keys()])
    values = list(update_data.values()) + [transaction_id]
    con.execute(f"UPDATE transactions SET {set_clause} WHERE id = ?", values)
    row = con.execute("SELECT * FROM transactions WHERE id = ?", [transaction_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No transaction found after update")
    return convert_to_response(row, [d[0] for d in con.description])

@app.delete("/transactions/{transaction_id}", status_code=204)
def delete_transaction(transaction_id: int, con = Depends(get_db)):
    """
    Delete a transaction record.
    
    Args:
        transaction_id: Unique identifier of the transaction to delete
        con: Database connection dependency
        
    Returns:
        None
        
    Raises:
        HTTPException: If transaction is not found (404)
    """
    cur = con.execute("DELETE FROM transactions WHERE id = ?", [transaction_id])
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return None
    
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)
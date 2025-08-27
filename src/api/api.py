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
    with get_connection() as con:
        con.execute(CREATE_SEQUENCE_QUERY)
        con.execute(BASE_TABLE_QUERY)

# Dependency
def get_db():
    con = get_connection()
    try:
        yield con
    finally:
        con.close()

#################
### UTILITIES ###
#################

def convert_to_db_row(transaction: schemas.TransactionCreate):
    data = transaction.model_dump(exclude={"id", "created_at"})
    return list(data.values()), list(data.keys())

def convert_to_response(row, columns):
    return dict(zip(columns, row))

#################
### ENDPOINTS ###
#################

@app.get("/")
def read_root():
    return {"message": "Transaction API", "version": "1.0.0"}

@app.post("/transactions", response_model=schemas.TransactionResponse)
def create_transaction(transaction: schemas.TransactionCreate, con = Depends(get_db)):
    values, fields = convert_to_db_row(transaction)
    placeholders = ", ".join(["?"] * len(values))
    con.execute(f"INSERT INTO transactions ({', '.join(fields)}) VALUES ({placeholders})", values)
    last_id = con.execute("SELECT max(id) FROM transactions").fetchone()[0]
    row = con.execute("SELECT * FROM transactions WHERE id = ?", [last_id]).fetchone()
    return convert_to_response(row, [d[0] for d in con.description])

@app.get("/transactions", response_model=List[schemas.TransactionResponse])
def read_transactions(limit: int = 100, con = Depends(get_db)):
    rows = con.execute("SELECT * FROM transactions LIMIT ?", [limit]).fetchall()
    columns = [d[0] for d in con.description]
    return [convert_to_response(r, columns) for r in rows]

@app.get("/transactions/{transaction_id}", response_model=schemas.TransactionResponse)
def read_transaction(transaction_id: int, con = Depends(get_db)):
    row = con.execute("SELECT * FROM transactions WHERE id = ?", [transaction_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return convert_to_response(row, [d[0] for d in con.description])

@app.put("/transactions/{transaction_id}", response_model=schemas.TransactionResponse)
def update_transaction(transaction_id: int, transaction_update: schemas.TransactionUpdate, con = Depends(get_db)):
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
    cur = con.execute("DELETE FROM transactions WHERE id = ?", [transaction_id])
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return None
    
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)
"""
Pydantic Data Models for Transaction API

This module defines the data validation schemas using Pydantic models for:
- Request/response data validation
- API input/output serialization
- Data type enforcement and conversion

Models:
    - TransactionBase: Base transaction model with common fields
    - TransactionCreate: Model for creating new transactions
    - TransactionUpdate: Model for updating existing transactions
    - TransactionResponse: Model for API responses
"""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class TransactionBase(BaseModel):
    """
    Base transaction model containing all common transaction fields.
    
    This model defines the structure for financial transaction data including
    time, 28 feature variables (v1-v28), and amount.
    """
    time: int
    v1: float
    v2: float
    v3: float
    v4: float
    v5: float
    v6: float
    v7: float
    v8: float
    v9: float
    v10: float
    v11: float
    v12: float
    v13: float
    v14: float
    v15: float
    v16: float
    v17: float
    v18: float
    v19: float
    v20: float
    v21: float
    v22: float
    v23: float
    v24: float
    v25: float
    v26: float
    v27: float
    v28: float
    amount: float

class TransactionCreate(TransactionBase):
    """
    Model for creating new transactions.
    
    Inherits all fields from TransactionBase. Used for POST requests
    to create new transaction records.
    """
    pass

class TransactionUpdate(BaseModel):
    """
    Model for updating existing transactions.
    
    All fields are optional, allowing partial updates. Only provided
    fields will be updated in the database.
    """
    time: Optional[int] = None
    v1: Optional[float] = None
    v2: Optional[float] = None
    v3: Optional[float] = None
    v4: Optional[float] = None
    v5: Optional[float] = None
    v6: Optional[float] = None
    v7: Optional[float] = None
    v8: Optional[float] = None
    v9: Optional[float] = None
    v10: Optional[float] = None
    v11: Optional[float] = None
    v12: Optional[float] = None
    v13: Optional[float] = None
    v14: Optional[float] = None
    v15: Optional[float] = None
    v16: Optional[float] = None
    v17: Optional[float] = None
    v18: Optional[float] = None
    v19: Optional[float] = None
    v20: Optional[float] = None
    v21: Optional[float] = None
    v22: Optional[float] = None
    v23: Optional[float] = None
    v24: Optional[float] = None
    v25: Optional[float] = None
    v26: Optional[float] = None
    v27: Optional[float] = None
    v28: Optional[float] = None
    amount: Optional[float] = None

class TransactionResponse(TransactionBase):
    """
    Model for transaction API responses.
    
    Extends TransactionBase with system-generated fields:
    - id: Unique transaction identifier
    - created_at: Timestamp when the record was created
    """
    id: int
    created_at: datetime

    class Config:
        from_attributes = True
# 🚀 Transaction API

A FastAPI-based transaction API with DuckDB backend for fraud detection data.

Based on this [Kaggle challenge dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)

## ✨ Features

- 🎯 RESTful API for transaction management (CRUD operations)
- 🦆 DuckDB database backend
- 📊 Batch processing for Parquet data ingestion

## 🐳 Docker Setup

### 📋 Prerequisites

- Docker
- Docker Compose

### 🚀 Quick Start

1. **Build and run with Docker Compose:**

   Use this specific approach to ensure that the test suite runs before the API itself:

   ```bash
   docker-compose --profile test up --build test api
   ```

2. **Run the main.py script:**

   After building the docker file, with the api running go to the
   the root folder, create a virtual enviroment and install the requirements, you can do this with the following, on a cmd terminal:

   ```powershell
   python -m venv <env_name>
   ```

   ```powershell
   cd <env_name>/script
   activate
   ```
   ```powershell
   pip install -r requirements.txt
   ```
   Go back to the root folder, and run the script by doing:

      ```powershell
   python main.py
   ```

   This should populate the `transactions` database with two tables. One named `transactions` with the online data and another `transactions_batch`
   for batch ingestion.


## 🔌 API Endpoints

- `GET /` - API information
- `POST /transactions` - Create a new transaction
- `GET /transactions` - List all transactions
- `GET /transactions/{id}` - Get a specific transaction
- `PUT /transactions/{id}` - Update a transaction
- `DELETE /transactions/{id}` - Delete a transaction

## 📊 Data Structure

For this scenario, we'll work with the following assumptions:

The dataset should contain:
- Time series features (time, v1-v28)
- Amount

The following columns will be automatically created. For batch ingestion, the Parquet file should not have these columns:

- id
- created_at


## 📁 Project Structure

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .dockerignore
├── src/
│   ├── api/          # FastAPI application
│   ├── batch/        # Batch processing
│   ├── database/     # Database files
│   └── datasets/     # Data files
└── Demonstration.ipynb  # Desmonstration notebook
└── main.py  # Ingestion script
```

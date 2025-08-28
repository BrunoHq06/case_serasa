# 🚀 Transaction API

A FastAPI-based transaction API with a DuckDB backend for fraud detection data.

Based on this [Kaggle challenge dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud).

## ✨ Features

* 🎯 RESTful API for transaction management (CRUD operations)
* 🦆 DuckDB database backend
* 📊 Batch processing for Parquet data ingestion

## 🐳 Docker Setup

### 📋 Prerequisites

* Docker
* Docker Compose

### 🚀 Quick Start

1. **Build and run with Docker Compose:**

   Use the following command to ensure the test suite runs before the API starts:

   ```bash
   docker-compose --profile test up --build test api
   ```

   > If you are using the VS Code terminal to build the application and only 9 out of 10 tests run, please try building it again. I haven’t yet figured out why this happens, but the application still works correctly.

2. **Run the `main.py` script:**

   After building the Docker image and running the API, go to the root folder. Create a virtual environment and install the requirements using the following commands in a Command Prompt terminal:

   ```powershell
   python -m venv <env_name>
   ```

   ```powershell
   cd \<env\_name>/Scripts
   activate
   ```

   To install the dependencies, you can install everything from the `requirements.txt` file. However, the `main.py` script only uses the following:

   ```powershell
   pip install requests==2.31.0 pandas>=2.1.3 duckdb==1.3.2
   ```

   Go back to the root folder and run the script:

   ```powershell
   python main.py
   ```

   This will populate the `transactions` database for online transactions and another database for batch ingestions with the `transactions_batch` table.

## 🔌 API Endpoints

* `GET /` - API information
* `POST /transactions` - Create a new transaction
* `GET /transactions` - List all transactions
* `GET /transactions/{id}` - Get a specific transaction
* `PUT /transactions/{id}` - Update a transaction
* `DELETE /transactions/{id}` - Delete a transaction

## 📊 Data Structure

For this scenario, we'll work with the following assumptions:

The dataset should contain:

* Time series features (`time`, `v1-v28`)
* `amount`

The following columns will be automatically created. For batch ingestion, the Parquet file should not contain these columns:

* `id`
* `created_at`

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
├── Demonstration.ipynb  # Demonstration notebook
└── main.py              # Ingestion script
```

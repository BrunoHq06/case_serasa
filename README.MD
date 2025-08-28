# 🚀 Transaction API

A FastAPI-based transaction API with DuckDB backend for fraud detection data.

Based on this [Kaggle challenge dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)

## ✨ Features

- 🎯 RESTful API for transaction management (CRUD operations)
- 🦆 DuckDB database backend
- 📊 Batch processing for Parquet data ingestion
- 📓 Notebook demonstrating the features of the application

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
   After that, you can curl the endpoints or, just use the `demonstration notebook`

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

## 🛠️ Development

The Docker setup includes volume mounts for:
- `./src` - Source code (hot reload)
- `./database` - DuckDB database files
- `./datasets` - Data files and Parquet datasets

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
└── Demonstration.ipynb  # Testing notebook
```

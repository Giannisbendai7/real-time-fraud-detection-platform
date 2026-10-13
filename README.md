Real-Time Fraud Detection Platform

A real-time credit card fraud detection pipeline built with Python, Apache Kafka, FastAPI, PySpark, MLflow, and Docker.

Overview

This project demonstrates how transaction data can be streamed through Apache Kafka, processed by a consumer, and evaluated by a machine learning model exposed through a REST API.

Architecture

Transaction Dataset
        |
        v
Kafka Producer
        |
        v
Apache Kafka (transactions topic)
        |
        v
Kafka Consumer
        |
        v
FastAPI Prediction API
        |
        v
MLflow / PySpark Model
        |
        v
Fraud Probability and Prediction

Tech Stack

Python

Apache Kafka

FastAPI

PySpark

MLflow

Pandas

Docker

Features

Kafka producer for publishing transaction records.

Kafka consumer for receiving streamed transactions.

REST API for machine learning inference.

MLflow model loading.

Fraud probability and threshold-based classification.

Dockerized API and Kafka broker.

Health-check endpoint.

API Endpoints

Health Check

GET /health

Example response:

{
  "status": "ok",
  "model_loaded": true
}

Fraud Prediction

POST /predict

The endpoint accepts Time, Amount, and features V1 through V28.

Example response:

{
  "fraud_probability": 0.0127,
  "prediction": 0,
  "threshold": 0.2
}

A prediction of 1 indicates a potentially fraudulent transaction according to the configured threshold. A prediction of 0 indicates a non-fraudulent classification.

Local Setup

1. Install dependencies

Create and activate a virtual environment:

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

2. Start Kafka

With Docker Desktop running:

docker compose -f docker-compose.kafka.yml up -d

3. Build the API image

docker build -t fraud-detection-api .

4. Run the API

docker run --name fraud_api -p 8000:8000 fraud-detection-api

5. Check the API

Interactive API documentation:

http://localhost:8000/docs

Health check:

Invoke-RestMethod http://localhost:8000/health

6. Start the Kafka consumer

In a separate PowerShell window, navigate to the project directory and run:

.\.venv\Scripts\python.exe -m src.kafka_consumer

7. Publish transactions

In another PowerShell window, navigate to the project directory and run:

.\.venv\Scripts\python.exe -m src.kafka_producer

The producer publishes a limited number of sample transactions from the local dataset. The consumer forwards new messages to the prediction API and prints the resulting predictions.

Model Inference

The API loads a previously trained PySpark model logged with MLflow. It derives additional features from transaction amount, time, and numerical transaction features before generating a prediction.

The configured decision threshold is 0.20. The threshold and model performance should be evaluated against suitable validation data before production use.

Testing

A local end-to-end smoke test successfully processed five transactions through the Kafka producer, Kafka topic, Kafka consumer, FastAPI endpoint, and loaded machine learning model.

All five sample transactions were classified as non-fraudulent in this test. This verifies the basic integration and inference flow, but does not establish production-level fraud detection performance.

Project Structure

real-time-fraud-detection-platform/
├── src/
│   ├── api.py
│   ├── kafka_consumer.py
│   ├── kafka_producer.py
│   ├── load_to_postgres.py
│   ├── preprocess.py
│   └── spark_processing.py
├── Dockerfile
├── docker-compose.kafka.yml
├── docker-compose.yml
├── .dockerignore
├── .gitignore
├── requirements.txt
└── README.md

Disclaimer

This is an educational portfolio project. Predictions are probabilistic and should not be used as the sole basis for real financial decisions.
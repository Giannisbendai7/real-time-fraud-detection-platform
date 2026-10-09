from pathlib import Path

import mlflow
import mlflow.spark
from fastapi import FastAPI
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log1p, when
from pyspark.ml.feature import VectorAssembler

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MODEL_PATH = (
    PROJECT_ROOT
    / "mlruns"
    / "1"
    / "f36d4d33f41b468890ffe5e57cce24dd"
    / "artifacts"
    / "random_forest_model"
)

app = FastAPI(
    title="Real-Time Fraud Detection API",
    version="1.0.0",
)

spark = None
model = None


class Transaction(BaseModel):
    Time: float = Field(ge=0)
    Amount: float = Field(ge=0)
    V1: float
    V2: float
    V3: float
    V4: float
    V5: float
    V6: float
    V7: float
    V8: float
    V9: float
    V10: float
    V11: float
    V12: float
    V13: float
    V14: float
    V15: float
    V16: float
    V17: float
    V18: float
    V19: float
    V20: float
    V21: float
    V22: float
    V23: float
    V24: float
    V25: float
    V26: float
    V27: float
    V28: float


@app.on_event("startup")
def load_model():
    global spark, model

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("FraudDetectionAPI")
        .getOrCreate()
    )

    model = mlflow.spark.load_model(str(MODEL_PATH))


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None}

@app.post("/predict")
def predict(transaction: Transaction):
    data = transaction.model_dump()

    df = spark.createDataFrame([data])

    df = df.withColumn(
        "is_large_transaction",
        col("Amount") > 100
    )

    df = df.withColumn(
        "amount_log",
        log1p(col("Amount"))
    )

    df = df.withColumn(
        "hour",
        ((col("Time") / 3600) % 24).cast("int")
    )

    df = df.withColumn(
        "v_sum",
        sum(col(f"V{i}") for i in range(1, 29))
    )

    assembler = VectorAssembler(
        inputCols=[
            "Amount",
            "Time",
            "is_large_transaction",
            "amount_log",
            "hour",
            "v_sum"
        ],
        outputCol="features"
    )

    df = assembler.transform(df)

    prediction = model.transform(df).first()

    fraud_probability = float(prediction["probability"][1])

    threshold = 0.20

    return {
        "fraud_probability": fraud_probability,
        "prediction": int(fraud_probability >= threshold),
        "threshold": threshold
    }

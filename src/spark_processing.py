from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, log1p, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
import mlflow
import mlflow.spark


PARQUET_PATH = "data/processed/transactions.parquet"


def main():
    spark = (
        SparkSession.builder
        .appName("FraudDetectionSpark")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.parquet(PARQUET_PATH)

    print("Total rows:", df.count())
    print("Columns:", df.columns)

    df.select("Amount", "Class").show(10)

    df.groupBy("Class").count().show()

    df.groupBy("Class").avg("Amount").show()

    df = df.withColumn(
        "is_large_transaction",
        df["Amount"] > 100
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

    train_df, test_df = df.randomSplit(
        [0.8, 0.2],
        seed=42
    )

    print("Training rows:", train_df.count())
    print("Test rows:", test_df.count())

    train_df = train_df.withColumn(
        "class_weight",
        when(
            col("Class") == 1,
            100.0
        ).otherwise(1.0)
    )

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="Class",
        weightCol="class_weight",
        numTrees=100,
        maxDepth=8,
        seed=42
    )

    mlflow.set_experiment("fraud-detection")

    with mlflow.start_run():

        mlflow.log_param("model", "RandomForest")
        mlflow.log_param("numTrees", 100)
        mlflow.log_param("maxDepth", 8)
        mlflow.log_param("class_weight", 100.0)

        rf_model = rf.fit(train_df)

    predictions = rf_model.transform(test_df)

    get_fraud_probability = udf(
        lambda probability: float(probability[1]),
        DoubleType()
    )

    predictions = predictions.withColumn(
        "fraud_probability",
        get_fraud_probability(col("probability"))
    )

    thresholds = [0.20, 0.15, 0.10, 0.05]

    for threshold in thresholds:

        tuned = predictions.withColumn(
            "tuned_prediction",
            when(
                col("fraud_probability") >= threshold,
                1.0
            ).otherwise(0.0)
        )

        tp = tuned.filter(
            (col("Class") == 1) &
            (col("tuned_prediction") == 1)
        ).count()

        fp = tuned.filter(
            (col("Class") == 0) &
            (col("tuned_prediction") == 1)
        ).count()

        fn = tuned.filter(
            (col("Class") == 1) &
            (col("tuned_prediction") == 0)
        ).count()

        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0

        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        print(
            f"Threshold: {threshold} | "
            f"Precision: {precision:.4f} | "
            f"Recall: {recall:.4f} | "
            f"F1: {f1:.4f}"
        )


        mlflow.log_metric(f"precision_{threshold}", precision)
        mlflow.log_metric(f"recall_{threshold}", recall)
        mlflow.log_metric(f"f1_{threshold}", f1)

    mlflow.spark.log_model(
        rf_model,
        "random_forest_model"
    )

    spark.stop()


if __name__ == "__main__":
    main()
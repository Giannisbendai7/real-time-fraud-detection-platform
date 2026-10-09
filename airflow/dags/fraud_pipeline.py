from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def start_pipeline():
    print("Fraud detection pipeline started!")


with DAG(
    dag_id="fraud_detection_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )
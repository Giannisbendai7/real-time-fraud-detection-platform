import json
import os
import time

import pandas as pd
from kafka import KafkaProducer


CSV_PATH = "data/raw/creditcard.csv"
KAFKA_TOPIC = "transactions"
KAFKA_SERVER = "localhost:9092"
MAX_TRANSACTIONS = int(os.getenv("MAX_TRANSACTIONS", "5"))
DELAY_SECONDS = 0.1


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_transactions():
    if MAX_TRANSACTIONS < 1:
        raise ValueError("MAX_TRANSACTIONS must be at least 1")

    df = pd.read_csv(CSV_PATH, nrows=MAX_TRANSACTIONS)
    producer = create_producer()

    try:
        for _, row in df.iterrows():
            transaction = row.to_dict()
            producer.send(KAFKA_TOPIC, value=transaction)
            print(f"Sent transaction: {transaction}")
            time.sleep(DELAY_SECONDS)

        producer.flush()
        print(f"Successfully sent {len(df)} transactions to '{KAFKA_TOPIC}'")
    finally:
        producer.close()


if __name__ == "__main__":
    send_transactions()
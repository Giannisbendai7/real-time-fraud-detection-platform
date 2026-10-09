import json
import time

import pandas as pd
from kafka import KafkaProducer


CSV_PATH = "data/raw/creditcard.csv"
KAFKA_TOPIC = "transactions"
KAFKA_SERVER = "localhost:9092"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_transactions():
    df = pd.read_csv(CSV_PATH)

    producer = create_producer()

    for _, row in df.iterrows():
        transaction = row.to_dict()

        producer.send(KAFKA_TOPIC, value=transaction)

        print(f"Sent transaction: {transaction}")

        time.sleep(0.1)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    send_transactions()
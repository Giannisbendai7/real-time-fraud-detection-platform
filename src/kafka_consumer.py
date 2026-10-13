import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from kafka import KafkaConsumer


KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "transactions"
API_URL = "http://localhost:8000/predict"


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id="fraud-detection-consumer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            transaction = message.value

            try:
                request = Request(
                    API_URL,
                    data=json.dumps(transaction).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )

                with urlopen(request, timeout=30) as response:
                    result = json.loads(response.read().decode("utf-8"))

                print(
                    f"Prediction: {result['prediction']} | "
                    f"Fraud probability: "
                    f"{result['fraud_probability']:.4f}"
                )

            except (HTTPError, URLError, TimeoutError, ValueError) as exc:
                print(f"Could not process transaction: {exc}")

    except KeyboardInterrupt:
        print("\nStopping Kafka consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

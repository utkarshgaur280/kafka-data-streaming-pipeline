"""Generate sample e-commerce orders and publish them to Apache Kafka."""
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BROKER = "localhost:9092"
TOPIC = "orders"

SAMPLE_ORDERS = [
    {"order_id": 1001, "customer_id": "C101", "product": "Headphones", "amount": 1999.00, "status": "placed"},
    {"order_id": 1002, "customer_id": "C102", "product": "Keyboard", "amount": 2499.00, "status": "placed"},
    {"order_id": 1003, "customer_id": "C103", "product": "Mouse", "amount": 799.00, "status": "cancelled"},
    {"order_id": 1004, "customer_id": "C104", "product": "Monitor", "amount": 10999.00, "status": "placed"},
    {"order_id": 1005, "customer_id": "C105", "product": "USB-C Hub", "amount": 1499.00, "status": "placed"},
]

def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            retries=5,
        )
    except NoBrokersAvailable:
        print("Kafka broker not available. Start it first with: docker compose up -d")
        return

    for order in SAMPLE_ORDERS:
        event = dict(order)
        event["event_time_utc"] = datetime.now(timezone.utc).isoformat()
        producer.send(TOPIC, value=event)
        producer.flush()
        print("Published:", json.dumps(event))
        time.sleep(0.5)

    producer.close()
    print("\nFinished publishing sample orders.")

if __name__ == "__main__":
    main()

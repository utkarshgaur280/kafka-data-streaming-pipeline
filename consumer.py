"""Consume order events from Kafka, validate them, and append valid rows to CSV."""
import csv
import json
import os
import math
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BROKER = "localhost:9092"
TOPIC = "orders"
OUTPUT_FILE = "processed_orders.csv"
FIELDS = ["order_id", "customer_id", "product", "amount", "status", "event_time_utc", "processing_result"]
    
def valid_order(order):
    required = [
        "order_id", "customer_id", "product",
        "amount", "status", "event_time_utc"
    ]

    if not isinstance(order, dict):
        return False, "event must be a JSON object"

    if not all(field in order for field in required):
        return False, "missing required field"

    for field in ["customer_id", "product", "status", "event_time_utc"]:
        if not isinstance(order[field], str) or not order[field].strip():
            return False, f"{field} cannot be empty"

    try:
        order_id = int(order["order_id"])
        if order_id <= 0:
            return False, "order_id must be positive"
    except (TypeError, ValueError):
        return False, "order_id must be an integer"

    try:
        amount = float(order["amount"])
        if not math.isfinite(amount):
            return False, "amount must be finite"
        if amount < 0:
            return False, "amount cannot be negative"
    except (TypeError, ValueError):
        return False, "amount must be numeric"

    if order["status"] not in {
        "placed", "cancelled", "shipped", "delivered"
    }:
        return False, "unknown status"

    return True, "valid"


def main():
    new_file = not os.path.exists(OUTPUT_FILE)
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="order-processing-demo",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=15000,
        )
    except NoBrokersAvailable:
        print("Kafka broker not available. Start it first with: docker compose up -d")
        return

    count = 0
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        print("Listening for order events for up to 15 seconds...")
        for message in consumer:
            order = message.value
            is_valid, reason = valid_order(order)
            row = {field: order.get(field, "") for field in FIELDS if field != "processing_result"}
            row["processing_result"] = reason
            if is_valid:
                writer.writerow(row)
                output.flush()
                print(f"Processed order {order['order_id']} -> saved to {OUTPUT_FILE}")
            else:
                print("Rejected event:", reason, json.dumps(order))
            count += 1
    consumer.close()
    print(f"\nFinished. Events read: {count}. Output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

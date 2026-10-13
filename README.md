# Real-Time Data Streaming Pipeline Using Apache Kafka

## Project summary
This beginner-friendly academic project simulates an e-commerce order stream. A Python producer publishes JSON order events to an Apache Kafka topic named `orders`. A Python consumer reads the events, performs basic validation, and writes accepted records to `processed_orders.csv`.

## Architecture
Order generator (producer) → Kafka topic (`orders`) → consumer/validation → CSV output.

The editable draw.io diagram is `architecture.drawio`.

## Prerequisites
- Docker Desktop with Docker Compose
- Python 3.10 or newer
- Internet access for the first Docker image download and Python package installation

## Run on Windows
1. Extract this project folder.
2. Open PowerShell or Command Prompt in the folder.
3. Start Kafka:
   ```bash
   docker compose up -d
   ```
4. Create and activate a virtual environment (recommended):
   ```bash
   py -m venv .venv
   .venv\Scripts\activate
   ```
5. Install dependency:
   ```bash
   pip install -r requirements.txt
   ```
6. Start the consumer in Terminal 1:
   ```bash
   python consumer.py
   ```
   It waits up to 15 seconds for events. Keep this terminal running.
7. In Terminal 2, publish sample orders:
   ```bash
   python producer.py
   ```
8. Inspect `processed_orders.csv` in the project folder.
9. Stop Kafka when finished:
   ```bash
   docker compose down
   ```

If the consumer times out before you publish events, rerun `python consumer.py` first, then immediately run `python producer.py` in another terminal.

## What this demonstrates
- Event producer and consumer pattern
- Kafka topic as a buffer between systems
- JSON event serialization/deserialization
- Basic schema/field and value validation
- Writing processed records to a downstream file

## Limitations
This is a local proof-of-concept, not a production pipeline. It uses one Kafka broker, sample data, a CSV sink, and basic validation. It does not implement authentication, encryption, schema registry, dead-letter queues, monitoring, or high availability.

## Academic integrity
Describe this as a project implemented to demonstrate concepts studied during a data engineering internship. Do not claim it was deployed at Minfy or used with company/customer data unless that is accurate. Replace placeholders in the report with your actual details and include screenshots from your own successful run.

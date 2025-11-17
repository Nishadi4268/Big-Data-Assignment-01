# producer/producer.py
import time
import json
import uuid
import random
import io
from kafka import KafkaProducer
from fastavro import schemaless_writer, parse_schema
from pathlib import Path

# CONFIG
BOOTSTRAP_SERVERS = "localhost:9092"
ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders.DLQ"
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "avro" / "order.avsc"

# Load Avro schema
with open(SCHEMA_PATH, "r") as f:
    schema = parse_schema(json.load(f))

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]
MAX_RETRIES = 3 

def avro_serialize(record, schema):
    """
    Serialize a dict to Avro bytes using fastavro schemaless_writer.
    """
    out = io.BytesIO()
    schemaless_writer(out, schema, record)
    return out.getvalue()

def produce_orders(interval_seconds=1.0, total=None):
    """
    Produce orders indefinitely (or `total` messages).
    Includes retry logic and DLQ support.
    """
    count = 0
    try:
         while True:
            order = {
                "orderId": str(uuid.uuid4()),
                "product": random.choice(PRODUCTS),
                "price": round(random.uniform(5.0, 200.0), 2)
            }
            payload = avro_serialize(order, schema)

            # Retry logic with DLQ
            for attempt in range(MAX_RETRIES):
                try:
                    producer.send(ORDERS_TOPIC, value=payload)
                    producer.flush()
                    print(f"[Producer] Sent order #{count + 1}: {order}")
                    count += 1
                    break  # success
                except Exception as e:
                    print(f"[Producer] Temporary failure: {e}, retry {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(2 ** attempt)  # exponential backoff
            else:
                # All retries failed → send to DLQ
                try:
                    producer.send(DLQ_TOPIC, value=payload)
                    producer.flush()
                    print(f"[Producer] Message sent to DLQ: {order}")
                    count += 1
                except Exception as dlq_error:
                    print(f"[Producer] Failed to send to DLQ: {dlq_error}")

            # Stop if total messages sent
            if total and count >= total:
                break

            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Producer stopped by user")
    finally:
        producer.close()

if __name__ == "__main__":
    # Simple CLI behavior
    import argparse
    parser = argparse.ArgumentParser(description="Kafka Avro Producer for orders.")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between messages")
    parser.add_argument("--count", type=int, default=0, help="Number of messages to send (0 = infinite)")
    args = parser.parse_args()
    total = args.count if args.count > 0 else None
    produce_orders(interval_seconds=args.interval, total=total)

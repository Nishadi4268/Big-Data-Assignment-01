# consumer/consumer.py
import io
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from fastavro import schemaless_reader, parse_schema
from pathlib import Path

# CONFIG
BOOTSTRAP_SERVERS = "localhost:9092"
ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders_dlq"
ORDER_AVG_TOPIC = "order_avg"
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "avro" / "order.avsc"

# Load Avro schema
with open(SCHEMA_PATH, "r") as f:
    schema = parse_schema(json.load(f))

# Kafka client
consumer = KafkaConsumer(
    ORDERS_TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='my-consumer-group'
    # no consumer_timeout_ms — consumer will block until messages arrive
)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

# Running average state
total_sum = 0.0
total_count = 0

# Retry config
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1

def avro_deserialize(avro_bytes, schema):
    """Return dict read from avro bytes using fastavro."""
    out = io.BytesIO(avro_bytes)
    record = schemaless_reader(out, schema)
    return record

def publish_running_average(avg):
    """
    Publish running average as a JSON string to order_avg topic.
    """
    msg = json.dumps({"running_average": avg})
    producer.send(ORDER_AVG_TOPIC, value=msg.encode("utf-8"))
    producer.flush()

def send_to_dlq(original_bytes, error_str):
    """
    Send DLQ message that contains original message (base64 or hex) and error.
    We'll include the original Avro bytes encoded as hex to keep it transportable.
    """
    payload = {
        "error": error_str,
        "original_bytes_hex": original_bytes.hex()
    }
    producer.send(DLQ_TOPIC, value=json.dumps(payload).encode("utf-8"))
    producer.flush()
    print(f"[DLQ] Sent message to DLQ: {error_str}")

def process_order(record):
    """
    Business logic: update running average using 'price' field.
    Throws exceptions on invalid data to simulate failures.
    """
    global total_sum, total_count
    # Validate
    if "price" not in record:
        raise ValueError("price field missing")
    price = record["price"]
    # Simulate transient error: if price is negative or unrealistic, mark temporary
    if price is None:
        raise ValueError("price is None")
    if not isinstance(price, (float, int)):
        raise TypeError("price must be numeric")
    # update running average
    total_count += 1
    total_sum += float(price)
    running_average = total_sum / total_count
    print(f"[Consumer] Processed orderId={record.get('orderId')} price={price} -> running_avg={running_average:.4f}")
    publish_running_average(running_average)

def handle_message_with_retries(kafka_msg):
    """
    Attempts to process message with retry logic.
    If fails after MAX_RETRIES, send to DLQ.
    """
    avro_bytes = kafka_msg.value
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            record = avro_deserialize(avro_bytes, schema)
            # Simulate transient failure option: if product == "Item3" and attempts == 0 -> pretend transient error
            # (Remove or modify this if not desired.)
            process_order(record)
            return True
        except Exception as e:
            attempts += 1
            print(f"[Consumer] Error processing message (attempt {attempts}/{MAX_RETRIES}): {e}")
            # For transient errors we will retry; for some errors we might treat as permanent:
            # Here we treat TypeError, ValueError as permanent after retries.
            time.sleep(RETRY_DELAY_SECONDS)
    # if here, failed permanently
    send_to_dlq(avro_bytes, f"Failed after {MAX_RETRIES} attempts: {e}")
    return False

def consume_loop():
    print("[Consumer] Starting consumption...")
    try:
        for msg in consumer:
            print(f"[Consumer] Received message at partition {msg.partition} offset {msg.offset}")
            success = handle_message_with_retries(msg)
            # Commit offset only after processing (or after sending DLQ)
            if success:
                consumer.commit()
            else:
                # Still commit to skip the bad message. Alternative: keep it uncommitted and let it be reprocessed.
                consumer.commit()
    except KeyboardInterrupt:
        print("[Consumer] Keyboard interrupt. Stopping.")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    consume_loop()

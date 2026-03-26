import json, uuid, time, random
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_BOOTSTRAP      = "localhost:9092"
TOPIC                = "product.usage.events"

ACCOUNT_IDS   = [f"acc_{i:04d}" for i in range(1, 51)]
USER_IDS      = [f"usr_{i:04d}" for i in range(1, 201)]
EVENT_TYPES   = ["feature_used", "api_call", "export", "login", "settings_change"]
FEATURE_NAMES = ["dashboard", "reporting", "api_v2", "bulk_export", "webhooks"]


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def produce_events(count: int = 1000, delay_ms: float = 10):
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    with open("kafka/schemas/usage_event.avsc") as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(schema_registry, schema_str)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    for _ in range(count):
        event = {
            "event_id":        str(uuid.uuid4()),
            "account_id":      random.choice(ACCOUNT_IDS),
            "user_id":         random.choice(USER_IDS),
            "event_type":      random.choice(EVENT_TYPES),
            "feature_name":    random.choice(FEATURE_NAMES),
            "session_id":      str(uuid.uuid4()),
            "event_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "metadata":        json.dumps({"browser": "chrome", "os": "linux"}),
        }
        producer.produce(
            topic=TOPIC,
            key=event["account_id"],
            value=avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        time.sleep(delay_ms / 1000)

    producer.flush()
    print(f"[DONE] Produced {count} events to {TOPIC}")


if __name__ == "__main__":
    produce_events(count=5000, delay_ms=5)
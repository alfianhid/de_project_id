import json, logging
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
KAFKA_BOOTSTRAP      = "kafka:29092"
TOPIC                = "product.usage.events"
CONSUMER_GROUP       = "data-warehouse-consumer"
BQ_TABLE             = "your-project.raw.raw_usage_events"
BATCH_SIZE           = 500
COMMIT_INTERVAL_S    = 30


def consume_to_bigquery():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    with open("kafka/schemas/usage_event.avsc") as f:
        schema_str = f.read()
    avro_deserializer = AvroDeserializer(schema_registry, schema_str)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    bq_client   = bigquery.Client()
    buffer      = []
    last_commit = datetime.now(timezone.utc)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"Kafka error: {msg.error()}")
            else:
                event = avro_deserializer(
                    msg.value(), SerializationContext(TOPIC, MessageField.VALUE)
                )
                event["_kafka_offset"]    = msg.offset()
                event["_kafka_partition"] = msg.partition()
                event["_loaded_at"]       = datetime.now(timezone.utc).isoformat()
                buffer.append(event)

            elapsed = (datetime.now(timezone.utc) - last_commit).total_seconds()
            if len(buffer) >= BATCH_SIZE or (buffer and elapsed >= COMMIT_INTERVAL_S):
                errors = bq_client.insert_rows_json(BQ_TABLE, buffer)
                if errors:
                    raise RuntimeError(f"BigQuery insert errors: {errors}")
                consumer.commit(asynchronous=False)
                log.info(f"Flushed {len(buffer)} events to BigQuery")
                buffer.clear()
                last_commit = datetime.now(timezone.utc)

    except KeyboardInterrupt:
        log.info("Shutting down — flushing buffer...")
        if buffer:
            bq_client.insert_rows_json(BQ_TABLE, buffer)
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_to_bigquery()
#!/usr/bin/env python3
"""
Kafka to GCS Sink Consumer

This consumer reads from Kafka topics and writes batches to Google Cloud Storage (GCS)
instead of directly streaming to BigQuery. This is a cost-optimization strategy:

Cost Comparison (per 1M events):
- BigQuery Streaming Insert: ~$0.05
- GCS + Spark Batch: ~$0.001
- Savings: ~50x

The consumer writes Parquet files to GCS in a time-partitioned structure:
    gs://bucket/raw/events/YYYY/MM/DD/HH/events-{sequence}.parquet

Airflow-managed: This script is designed to run as a long-running task within
an Airflow DAG (dag_kafka_consumer.py), NOT as a standalone service.

Usage:
    # Run via Airflow DAG (recommended)
    # Airflow UI → DAGs → kafka_consumer_ingestion → Trigger

    # Or run manually for testing
    python kafka/consumers/gcs_sink_consumer.py

Environment Variables:
    GCS_BUCKET_NAME: GCS bucket name (required)
    GCP_PROJECT_ID: GCP project ID (optional, auto-detected)
"""

import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Configuration
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC_USAGE_EVENTS", "product.usage.events")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "data-warehouse-consumer")

# GCS Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "your-project-data-lake")
GCS_PREFIX = os.getenv("GCS_PATH_RAW_EVENTS", "raw/events")

# Batch Configuration
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "500"))
COMMIT_INTERVAL_S = int(os.getenv("KAFKA_COMMIT_INTERVAL_S", "30"))
MAX_FILE_SIZE_MB = int(os.getenv("GCS_MAX_FILE_SIZE_MB", "100"))


def get_gcs_path(timestamp: datetime) -> str:
    """Generate GCS path based on event timestamp."""
    return (
        f"{GCS_PREFIX}/"
        f"{timestamp.year:04d}/"
        f"{timestamp.month:02d}/"
        f"{timestamp.day:02d}/"
        f"{timestamp.hour:02d}"
    )


def upload_to_gcs(
    storage_client: storage.Client,
    bucket: storage.Bucket,
    df: pd.DataFrame,
    base_path: str,
    sequence: int,
) -> str:
    """
    Upload DataFrame to GCS as Parquet file.

    Returns:
        GCS URI of uploaded file
    """
    # Convert DataFrame to Parquet in memory
    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Generate filename
    timestamp = datetime.now(timezone.utc)
    filename = f"{base_path}/events-{sequence:05d}-{timestamp.strftime('%Y%m%d%H%M%S')}.parquet"

    # Upload to GCS
    blob = bucket.blob(filename)
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    # Set metadata
    blob.metadata = {
        "record_count": str(len(df)),
        "min_event_timestamp": str(df["event_timestamp"].min()),
        "max_event_timestamp": str(df["event_timestamp"].max()),
        "uploaded_at": timestamp.isoformat(),
    }
    blob.patch()

    return f"gs://{GCS_BUCKET_NAME}/{filename}"


def consume_to_gcs():
    """Main consumer loop - reads from Kafka and writes to GCS."""

    # Initialize Schema Registry
    log.info(f"Connecting to Schema Registry at {SCHEMA_REGISTRY_URL}")
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # Load Avro schema
    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schemas", "usage_event.avsc"
    )
    with open(schema_path) as f:
        schema_str = f.read()
    avro_deserializer = AvroDeserializer(schema_registry, schema_str)

    # Initialize Kafka Consumer
    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,  # 5 minutes
            "session.timeout.ms": 45000,  # 45 seconds
        }
    )
    consumer.subscribe([TOPIC])
    log.info(f"Subscribed to topic: {TOPIC}")

    # Initialize GCS Client
    log.info(f"Connecting to GCS bucket: {GCS_BUCKET_NAME}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # Verify bucket exists
    if not bucket.exists():
        raise RuntimeError(f"GCS bucket {GCS_BUCKET_NAME} does not exist!")

    # State tracking
    buffer = []
    last_commit = datetime.now(timezone.utc)
    file_sequence = 0
    total_events = 0

    log.info(
        f"Starting consumer: batch_size={BATCH_SIZE}, "
        f"commit_interval={COMMIT_INTERVAL_S}s"
    )

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message available
                pass

            elif msg.error():
                # Handle errors
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.info(
                        f"Reached end of partition: {msg.topic()}[{msg.partition()}]"
                    )
                else:
                    raise KafkaException(msg.error())

            else:
                # Process message
                try:
                    event = avro_deserializer(
                        msg.value(), SerializationContext(TOPIC, MessageField.VALUE)
                    )

                    # Add metadata
                    event["_kafka_offset"] = msg.offset()
                    event["_kafka_partition"] = msg.partition()
                    event["_kafka_timestamp"] = msg.timestamp()[1]
                    event["_loaded_at"] = datetime.now(timezone.utc).isoformat()

                    buffer.append(event)

                except Exception as e:
                    log.error(
                        f"Failed to deserialize message at offset {msg.offset()}: {e}"
                    )
                    # Continue processing other messages

            # Check if we should flush buffer
            elapsed = (datetime.now(timezone.utc) - last_commit).total_seconds()
            should_flush = len(buffer) >= BATCH_SIZE or (
                buffer and elapsed >= COMMIT_INTERVAL_S
            )

            if should_flush and buffer:
                try:
                    # Convert buffer to DataFrame
                    df = pd.DataFrame(buffer)

                    # Determine GCS path based on current time
                    # (alternatively, could use event timestamps)
                    gcs_path = get_gcs_path(datetime.now(timezone.utc))

                    # Upload to GCS
                    uri = upload_to_gcs(
                        storage_client, bucket, df, gcs_path, file_sequence
                    )

                    # Commit Kafka offsets (only after successful GCS upload)
                    consumer.commit(asynchronous=False)

                    log.info(
                        f"Flushed {len(buffer)} events to {uri} "
                        f"(total: {total_events + len(buffer)})"
                    )

                    # Update state
                    total_events += len(buffer)
                    file_sequence += 1
                    buffer.clear()
                    last_commit = datetime.now(timezone.utc)

                except Exception as e:
                    log.error(f"Failed to upload batch to GCS: {e}")
                    # Don't commit Kafka offsets - will retry on restart
                    raise

    except KeyboardInterrupt:
        log.info("Shutdown requested - flushing remaining buffer...")

        if buffer:
            try:
                df = pd.DataFrame(buffer)
                gcs_path = get_gcs_path(datetime.now(timezone.utc))
                uri = upload_to_gcs(storage_client, bucket, df, gcs_path, file_sequence)
                consumer.commit(asynchronous=False)
                log.info(f"Final flush: {len(buffer)} events to {uri}")
            except Exception as e:
                log.error(f"Failed final flush: {e}")

    except Exception as e:
        log.error(f"Consumer error: {e}")
        raise

    finally:
        log.info(f"Closing consumer. Total events processed: {total_events}")
        consumer.close()


if __name__ == "__main__":
    # Validate configuration
    if GCS_BUCKET_NAME == "your-project-data-lake":
        log.warning(
            "GCS_BUCKET_NAME not set! Using default. Please configure in .env file."
        )

    # Start consumer
    consume_to_gcs()

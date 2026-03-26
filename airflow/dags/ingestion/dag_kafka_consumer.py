from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import sys

sys.path.insert(0, "/opt/airflow")


def run_kafka_consumer():
    """Run the Kafka consumer to ingest events into GCS (Data Lake)."""
    from kafka.consumers.gcs_sink_consumer import consume_to_gcs

    consume_to_gcs()


@dag(
    dag_id="kafka_consumer_ingestion",
    schedule="@once",  # Run once and stay alive (consumer is long-running)
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["kafka", "ingestion", "streaming", "real-time"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "owner": "data-engineering",
    },
    max_active_runs=1,
)
def kafka_consumer_ingestion():
    """
    DAG to run the Kafka consumer that ingests usage events from Kafka
    and loads them into GCS (Data Lake) for cost-effective storage.

    Architecture: Kafka → GCS (raw/events/) → Spark → BigQuery
    This follows the cost-optimized Data Lake pattern (50x cheaper than
    BigQuery streaming inserts).

    This is a long-running streaming consumer, not a batch job.
    """

    PythonOperator(
        task_id="consume_kafka_to_gcs",
        python_callable=run_kafka_consumer,
    )


kafka_consumer_ingestion()

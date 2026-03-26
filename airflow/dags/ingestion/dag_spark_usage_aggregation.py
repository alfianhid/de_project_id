from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="spark_usage_aggregation",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["spark", "ingestion", "usage"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def spark_usage_aggregation():

    SparkSubmitOperator(
        task_id="submit_usage_aggregation_job",
        conn_id="spark_default",
        application="/opt/spark/jobs/batch_usage_aggregation.py",
        name="saas_usage_aggregation_{{ ds }}",
        application_args=["{{ ds }}"],
        conf={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
        executor_memory="3g",
        executor_cores=2,
        num_executors=2,
    )


spark_usage_aggregation()
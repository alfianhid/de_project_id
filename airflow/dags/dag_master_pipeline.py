from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="master_daily_pipeline",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["master", "orchestration"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def master_daily_pipeline():

    trigger_stripe = TriggerDagRunOperator(
        task_id="trigger_stripe_ingestion",
        trigger_dag_id="stripe_subscriptions_ingestion",
        wait_for_completion=True,
        poke_interval=60,
        failed_states=["failed"],
    )

    trigger_spark = TriggerDagRunOperator(
        task_id="trigger_spark_usage_aggregation",
        trigger_dag_id="spark_usage_aggregation",
        wait_for_completion=True,
        poke_interval=60,
        failed_states=["failed"],
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformation",
        trigger_dag_id="dbt_transformation_run",
        wait_for_completion=True,
        poke_interval=60,
        failed_states=["failed"],
    )

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality_checks",
        trigger_dag_id="data_quality_checks",
        wait_for_completion=True,
        poke_interval=30,
        failed_states=["failed"],
    )

    [trigger_stripe, trigger_spark] >> trigger_dbt >> trigger_quality


master_daily_pipeline()
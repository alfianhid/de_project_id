from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import json
from google.cloud import bigquery
from datetime import datetime, timezone

DBT = "dbt --no-use-colors --project-dir /opt/dbt --profiles-dir /opt/dbt"
TGT = "--target prod"


def upload_dbt_artifacts(**context):
    """
    Reads dbt run_results.json and manifest.json from target/,
    flattens key metrics, and inserts into monitoring.dbt_run_results in BQ.
    """
    run_results_path = "/opt/dbt/target/run_results.json"
    with open(run_results_path) as f:
        run_results = json.load(f)

    execution_date = context["ds"]
    rows = []

    for result in run_results.get("results", []):
        rows.append({
            "run_date":        execution_date,
            "pipeline_run_at": datetime.now(timezone.utc).isoformat(),
            "node_id":         result.get("unique_id"),
            "node_name":       result.get("unique_id", "").split(".")[-1],
            "status":          result.get("status"),
            "execution_time_s": round(result.get("execution_time", 0), 2),
            "rows_affected":   result.get("adapter_response", {}).get("rows_affected", 0),
            "message":         result.get("message", ""),
            "invocation_id":   run_results.get("metadata", {}).get("invocation_id"),
            "dbt_version":     run_results.get("metadata", {}).get("dbt_version"),
        })

    if not rows:
        print("[WARN] No dbt results found to upload.")
        return

    client   = bigquery.Client()
    table_id = "your-project.monitoring.dbt_run_results"
    errors   = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BQ insert errors for dbt artifacts: {errors}")

    print(f"[INFO] Uploaded {len(rows)} dbt result rows to {table_id}")


@dag(
    dag_id="dbt_transformation_run",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["transformation", "dbt"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "owner": "data-engineering",
    },
)
def dbt_transformation_run():

    deps         = BashOperator(task_id="dbt_deps",            bash_command=f"{DBT} deps")
    run_staging  = BashOperator(task_id="dbt_run_staging",     bash_command=f"{DBT} run {TGT} --select staging --fail-fast")
    test_staging = BashOperator(task_id="dbt_test_staging",    bash_command=f"{DBT} test {TGT} --select staging")
    run_int      = BashOperator(task_id="dbt_run_intermediate",bash_command=f"{DBT} run {TGT} --select intermediate")
    run_xform    = BashOperator(task_id="dbt_run_transform",   bash_command=f"{DBT} run {TGT} --select transform --fail-fast")
    test_xform   = BashOperator(task_id="dbt_test_transform",  bash_command=f"{DBT} test {TGT} --select transform")
    snapshots    = BashOperator(task_id="dbt_snapshots",       bash_command=f"{DBT} snapshot {TGT}")
    run_rpt      = BashOperator(task_id="dbt_run_reporting",   bash_command=f"{DBT} run {TGT} --select reporting")
    test_rpt     = BashOperator(task_id="dbt_test_reporting",  bash_command=f"{DBT} test {TGT} --select reporting")
    gen_docs     = BashOperator(task_id="dbt_generate_docs",   bash_command=f"{DBT} docs generate {TGT}",
                                trigger_rule=TriggerRule.ALL_SUCCESS)

    upload_artifacts = PythonOperator(
        task_id="upload_dbt_artifacts_to_bq",
        python_callable=upload_dbt_artifacts,
        trigger_rule="all_done",
    )

    deps >> run_staging >> test_staging >> run_int >> run_xform >> test_xform >> snapshots >> run_rpt >> test_rpt >> gen_docs >> upload_artifacts


dbt_transformation_run()
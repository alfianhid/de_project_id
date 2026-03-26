import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="airflow/dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, \
        f"DAG import errors:\n{dagbag.import_errors}"


def test_expected_dags_present(dagbag):
    for dag_id in [
        "master_daily_pipeline",
        "stripe_subscriptions_ingestion",
        "spark_usage_aggregation",
        "dbt_transformation_run",
        "data_quality_checks",
    ]:
        assert dag_id in dagbag.dags, f"Missing DAG: {dag_id}"


def test_no_dag_cycles(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.test_cycle() is False, f"Cycle found in DAG: {dag_id}"


def test_dbt_dag_layer_order(dagbag):
    dag   = dagbag.dags["dbt_transformation_run"]
    tasks = [t.task_id for t in dag.topological_sort()]
    assert tasks.index("dbt_run_staging") \
         < tasks.index("dbt_run_transform") \
         < tasks.index("dbt_run_reporting")
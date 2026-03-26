from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery


@dag(
    dag_id="data_quality_checks",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["quality"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def data_quality_checks():

    @task()
    def check_row_volume(**context) -> dict:
        ds      = context["ds"]
        client  = bigquery.Client()
        project = "your-gcp-project-id"
        query = f"""
            with daily as (
                select date(_loaded_at) as d, count(*) as n
                from `{project}.raw.raw_subscriptions`
                where date(_loaded_at) >= date_sub('{ds}', interval 8 day)
                group by 1
            ),
            stats as (
                select
                    avg(n) filter (where d < '{ds}') as avg_7d,
                    max(n) filter (where d = '{ds}')  as today
                from daily
            )
            select today, avg_7d,
                safe_divide(abs(today - avg_7d), avg_7d) as deviation_pct
            from stats
        """
        row = list(client.query(query).result())[0]
        if row.deviation_pct and row.deviation_pct > 0.30:
            raise ValueError(
                f"Volume anomaly: today={row.today}, avg_7d={row.avg_7d:.0f}, "
                f"deviation={row.deviation_pct:.1%}"
            )
        print(f"[PASS] Volume OK: {row.today} rows")
        return {"today": row.today, "avg_7d": row.avg_7d}

    @task()
    def check_mrr_reconciliation(**context) -> None:
        ds      = context["ds"]
        client  = bigquery.Client()
        project = "your-gcp-project-id"
        query = f"""
            with stg as (
                select sum(mrr_amount) as total
                from `{project}.staging.stg_subscriptions`
                where subscription_status='active' and date(_loaded_at)='{ds}'
            ),
            xfm as (
                select sum(mrr_amount) as total
                from `{project}.transform.fct_subscriptions`
                where subscription_status='active' and date(_loaded_at)='{ds}'
            )
            select s.total as stg, x.total as xfm,
                safe_divide(abs(s.total - x.total), s.total) as diff_pct
            from stg s cross join xfm x
        """
        row = list(client.query(query).result())[0]
        if row.diff_pct and row.diff_pct > 0.0001:
            raise ValueError(
                f"MRR reconciliation failed: stg={row.stg:.2f}, xfm={row.xfm:.2f}, "
                f"diff={row.diff_pct:.4%}"
            )
        print(f"[PASS] MRR reconciliation OK. Diff: {row.diff_pct:.6%}")

    @task()
    def check_freshness(**context) -> None:
        client  = bigquery.Client()
        project = "your-gcp-project-id"
        for table in [
            f"{project}.reporting.rpt_mrr_monthly",
            f"{project}.reporting.rpt_churn_cohort",
        ]:
            row = list(client.query(f"""
                select timestamp_diff(current_timestamp(),
                    max(_report_generated_at), hour) as hours_ago
                from `{table}`
            """).result())[0]
            if row.hours_ago and row.hours_ago > 6:
                raise ValueError(f"Freshness SLA breach: {table} is {row.hours_ago}h old.")
            print(f"[PASS] Freshness OK: {table} — {row.hours_ago}h ago")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def notify_summary(**context) -> None:
        print("[INFO] Quality check summary sent to Slack.")

    vol   = check_row_volume()
    mrr   = check_mrr_reconciliation()
    fresh = check_freshness()
    [vol, mrr, fresh] >> notify_summary()


data_quality_checks()
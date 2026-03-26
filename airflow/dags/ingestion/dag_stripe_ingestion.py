from __future__ import annotations
import pendulum, json, stripe, pandas as pd
from datetime import timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
from airflow.decorators import dag, task
from airflow.models import Variable
from google.cloud import bigquery


@dag(
    dag_id="stripe_subscriptions_ingestion",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["ingestion", "stripe", "billing"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "owner": "data-engineering",
        "email_on_failure": True,
        "email": ["data-alerts@yourcompany.com"],
    },
)
def stripe_subscriptions_ingestion():

    @task()
    def extract_subscriptions() -> list[dict]:
        stripe.api_key = Variable.get("STRIPE_API_KEY")
        subscriptions, last_id = [], None

        @retry(stop=stop_after_attempt(5),
               wait=wait_exponential(multiplier=1, min=4, max=60))
        def fetch_page(**kwargs):
            return stripe.Subscription.list(**kwargs)

        while True:
            kwargs = {"limit": 100, "expand": ["data.customer"]}
            if last_id:
                kwargs["starting_after"] = last_id
            response = fetch_page(**kwargs)
            batch    = response.get("data", [])
            if not batch:
                break
            subscriptions.extend([json.loads(str(s)) for s in batch])
            last_id = batch[-1]["id"]
            if not response.get("has_more"):
                break

        if not subscriptions:
            raise ValueError("No subscriptions returned from Stripe.")
        print(f"[INFO] Extracted {len(subscriptions)} subscriptions")
        return subscriptions

    @task()
    def load_to_bigquery_raw(subscriptions: list[dict], **context) -> int:
        df = pd.json_normalize(subscriptions, sep="_")
        df["_loaded_at"]         = pd.Timestamp.utcnow()
        df["_source_system"]     = "stripe"
        df["_pipeline_run_date"] = context["ds"]

        for col in df.select_dtypes("object").columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

        client   = bigquery.Client()
        table_id = f"{Variable.get('GCP_PROJECT_ID')}.raw.raw_subscriptions"
        job = client.load_table_from_dataframe(
            df, table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema_update_options=["ALLOW_FIELD_ADDITION"],
                autodetect=True,
            )
        )
        job.result()
        print(f"[INFO] Loaded {len(df)} rows → {table_id}")
        return len(df)

    @task()
    def validate_load(row_count: int) -> None:
        if row_count == 0:
            raise ValueError("Load validation failed: zero rows written.")
        print(f"[PASS] {row_count} rows confirmed in BigQuery.")

    subs      = extract_subscriptions()
    row_count = load_to_bigquery_raw(subs)
    validate_load(row_count)


stripe_subscriptions_ingestion()
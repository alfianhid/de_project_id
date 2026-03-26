import sys
from datetime import date, timedelta
from pyspark.sql import functions as F, Window
from spark_session_factory import create_spark_session

PROJECT_ID      = "your-gcp-project-id"
RAW_TABLE       = f"{PROJECT_ID}.raw.raw_usage_events"
STAGING_TABLE   = f"{PROJECT_ID}.staging.stg_usage_events_daily"
TEMP_GCS_BUCKET = "your-gcs-temp-bucket"


def run(processing_date: str):
    spark = create_spark_session("batch_usage_aggregation")
    print(f"[INFO] Processing: {processing_date}")

    raw_df = (
        spark.read.format("bigquery")
        .option("table", RAW_TABLE)
        .option("filter", f"DATE(_loaded_at) = '{processing_date}'")
        .load()
    )
    print(f"[INFO] Raw rows: {raw_df.count()}")

    deduped_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("event_id").orderBy(F.col("_loaded_at").desc())
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    cleaned_df = (
        deduped_df
        .withColumn("event_date",
                    F.to_date(F.from_unixtime(F.col("event_timestamp") / 1000)))
        .withColumn("event_type",   F.lower(F.trim("event_type")))
        .withColumn("feature_name", F.lower(F.trim("feature_name")))
        .filter(F.col("account_id").isNotNull())
        .filter(F.col("event_id").isNotNull())
    )

    agg_df = (
        cleaned_df
        .groupBy("event_date", "account_id", "event_type", "feature_name")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.min("event_timestamp").alias("first_event_ts"),
            F.max("event_timestamp").alias("last_event_ts"),
        )
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_source", F.lit("spark_batch"))
    )

    (
        agg_df.write
        .format("bigquery")
        .option("table", STAGING_TABLE)
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .option("partitionField", "event_date")
        .option("partitionType", "DAY")
        .option("clusteredFields", "account_id,event_type")
        .mode("overwrite")
        .save()
    )

    print(f"[DONE] Written {agg_df.count()} aggregated rows to {STAGING_TABLE}")
    spark.stop()


if __name__ == "__main__":
    processing_date = sys.argv[1] if len(sys.argv) > 1 \
        else str(date.today() - timedelta(days=1))
    run(processing_date)
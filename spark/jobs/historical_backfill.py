#!/usr/bin/env python3
"""
Historical Backfill Job for SaaS Data Platform

This Spark job performs historical data backfill for usage events.
It can process data for a specific date range, useful for:
- Initial data migration
- Re-processing corrupted data
- Backfilling missing historical data

Usage:
    # Backfill specific date range
    spark-submit historical_backfill.py --start-date 2024-01-01 --end-date 2024-01-31

    # Backfill with custom batch size
    spark-submit historical_backfill.py --start-date 2024-01-01 --end-date 2024-01-31 --batch-days 7

    # Dry run (validate without writing)
    spark-submit historical_backfill.py --start-date 2024-01-01 --end-date 2024-01-31 --dry-run
"""

import argparse
import sys
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import functions as F, Window
from spark_session_factory import create_spark_session


# Configuration
PROJECT_ID = "your-gcp-project-id"
RAW_TABLE = f"{PROJECT_ID}.raw.raw_usage_events"
STAGING_TABLE = f"{PROJECT_ID}.staging.stg_usage_events_daily"
TEMP_GCS_BUCKET = "your-gcs-temp-bucket"


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Historical backfill for usage events")
    parser.add_argument(
        "--start-date", type=str, required=True, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end-date", type=str, required=True, help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=1,
        help="Number of days to process in each batch (default: 1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the job without writing to BigQuery",
    )
    parser.add_argument(
        "--source-table",
        type=str,
        default=RAW_TABLE,
        help=f"Source BigQuery table (default: {RAW_TABLE})",
    )
    parser.add_argument(
        "--target-table",
        type=str,
        default=STAGING_TABLE,
        help=f"Target BigQuery table (default: {STAGING_TABLE})",
    )
    return parser.parse_args()


def validate_date(date_str: str) -> datetime:
    """Validate and parse date string."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def get_date_ranges(start_date: datetime, end_date: datetime, batch_days: int):
    """Generate date ranges for batch processing."""
    current = start_date
    while current <= end_date:
        batch_end = min(current + timedelta(days=batch_days - 1), end_date)
        yield current, batch_end
        current = batch_end + timedelta(days=1)


def process_batch(
    spark,
    batch_start: datetime,
    batch_end: datetime,
    source_table: str,
    dry_run: bool = False,
):
    """Process a single batch of dates."""
    start_str = batch_start.strftime("%Y-%m-%d")
    end_str = batch_end.strftime("%Y-%m-%d")

    print(f"[INFO] Processing batch: {start_str} to {end_str}")

    # Read raw data for the date range
    # Use partitioning to only read relevant data
    raw_df = (
        spark.read.format("bigquery")
        .option("table", source_table)
        .option(
            "filter",
            f"DATE(_loaded_at) >= '{start_str}' AND DATE(_loaded_at) <= '{end_str}'",
        )
        .load()
    )

    raw_count = raw_df.count()
    print(f"[INFO] Raw rows in batch: {raw_count}")

    if raw_count == 0:
        print(f"[WARNING] No data found for batch {start_str} to {end_str}")
        return 0

    # Deduplicate by event_id (keep latest)
    deduped_df = (
        raw_df.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("event_id").orderBy(F.col("_loaded_at").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Clean and transform data
    cleaned_df = (
        deduped_df.withColumn(
            "event_date", F.to_date(F.from_unixtime(F.col("event_timestamp") / 1000))
        )
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("feature_name", F.lower(F.trim("feature_name")))
        .filter(F.col("account_id").isNotNull())
        .filter(F.col("event_id").isNotNull())
    )

    # Aggregate daily metrics
    agg_df = (
        cleaned_df.groupBy("event_date", "account_id", "event_type", "feature_name")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.min("event_timestamp").alias("first_event_ts"),
            F.max("event_timestamp").alias("last_event_ts"),
        )
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_source", F.lit("spark_historical_backfill"))
        .withColumn("_batch_start", F.lit(start_str))
        .withColumn("_batch_end", F.lit(end_str))
    )

    result_count = agg_df.count()
    print(f"[INFO] Aggregated rows in batch: {result_count}")

    if not dry_run:
        # Write to BigQuery
        # Use append mode for historical backfill
        (
            agg_df.write.format("bigquery")
            .option("table", STAGING_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .option("partitionField", "event_date")
            .option("partitionType", "DAY")
            .option("clusteredFields", "account_id,event_type")
            .mode("append")
            .save()
        )
        print(f"[SUCCESS] Written {result_count} rows to {STAGING_TABLE}")
    else:
        print(f"[DRY RUN] Would write {result_count} rows to {STAGING_TABLE}")
        agg_df.show(5, truncate=False)

    return result_count


def run_backfill(
    start_date: str,
    end_date: str,
    batch_days: int = 1,
    dry_run: bool = False,
    source_table: str = RAW_TABLE,
    target_table: str = STAGING_TABLE,
):
    """Run the historical backfill job."""
    # Validate dates
    start_dt = validate_date(start_date)
    end_dt = validate_date(end_date)

    if start_dt > end_dt:
        raise ValueError(f"Start date {start_date} must be before end date {end_date}")

    print(f"[INFO] Starting historical backfill")
    print(f"[INFO] Date range: {start_date} to {end_date}")
    print(f"[INFO] Batch size: {batch_days} days")
    print(f"[INFO] Source: {source_table}")
    print(f"[INFO] Target: {target_table}")
    print(f"[INFO] Dry run: {dry_run}")

    # Create Spark session
    spark = create_spark_session("historical_backfill")

    try:
        total_rows = 0
        batch_num = 0

        # Process in batches
        for batch_start, batch_end in get_date_ranges(start_dt, end_dt, batch_days):
            batch_num += 1
            print(f"\n{'=' * 60}")
            print(
                f"[BATCH {batch_num}] Processing {batch_start.date()} to {batch_end.date()}"
            )
            print(f"{'=' * 60}")

            try:
                rows = process_batch(
                    spark, batch_start, batch_end, source_table, dry_run
                )
                total_rows += rows
            except Exception as e:
                print(f"[ERROR] Batch {batch_num} failed: {str(e)}")
                if not dry_run:
                    raise

        print(f"\n{'=' * 60}")
        print(f"[DONE] Historical backfill complete")
        print(f"[DONE] Total batches processed: {batch_num}")
        print(f"[DONE] Total rows written: {total_rows}")
        print(f"{'=' * 60}")

    finally:
        spark.stop()


def main():
    """Main entry point."""
    args = parse_args()

    try:
        run_backfill(
            start_date=args.start_date,
            end_date=args.end_date,
            batch_days=args.batch_days,
            dry_run=args.dry_run,
            source_table=args.source_table,
            target_table=args.target_table,
        )
        sys.exit(0)
    except Exception as e:
        print(f"[FATAL] Historical backfill failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

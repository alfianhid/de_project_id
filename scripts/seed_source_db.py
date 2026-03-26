#!/usr/bin/env python3
"""Seed postgres-source with CSV dummy data, handling intentional dirty records."""

import csv
import os
import psycopg2
from pathlib import Path

# Database connection - uses localhost since running outside Docker
DSN = (
    f"host=localhost "
    f"port=5433 dbname={os.getenv('SOURCE_DB_NAME', 'deproject_db')} "
    f"user={os.getenv('SOURCE_DB_USER', 'deprojectuser')} "
    f"password={os.getenv('SOURCE_DB_PASSWORD', 'deprojectpass')}"
)

CSV_DIR = Path("data/csv")

TABLE_MAP = {
    "raw_accounts.csv": {
        "table": "app.accounts",
        "columns": [
            "account_id",
            "account_name",
            "industry",
            "company_size_band",
            "region",
            "account_tier",
            "csm_user_id",
            "is_active",
            "created_at",
            "updated_at",
        ],
    },
    "raw_users.csv": {
        "table": "app.users",
        "columns": [
            "user_id",
            "account_id",
            "email",
            "first_name",
            "last_name",
            "role",
            "is_active",
            "created_at",
            "updated_at",
        ],
    },
    "raw_subscriptions.csv": {
        "table": "app.subscriptions",
        "columns": [
            "id",
            "customer",
            "plan_id",
            "status",
            "billing_interval",
            "currency",
            "amount",
            "created",
            "canceled_at",
            "ended_at",
            "trial_start",
            "trial_end",
            "_source_system",
            "_loaded_at",
        ],
    },
    "raw_usage_events.csv": {
        "table": "app.usage_events",
        "columns": [
            "event_id",
            "account_id",
            "user_id",
            "event_type",
            "feature_name",
            "session_id",
            "event_timestamp",
            "metadata",
            "_kafka_offset",
            "_kafka_partition",
            "_loaded_at",
        ],
    },
    "raw_invoices.csv": {
        "table": "app.invoices",
        "columns": [
            "invoice_id",
            "account_id",
            "subscription_id",
            "status",
            "currency",
            "total_cents",
            "due_date",
            "paid_at",
            "created_at",
            "_source_system",
            "_loaded_at",
        ],
    },
    "raw_invoice_line_items.csv": {
        "table": "app.invoice_line_items",
        "columns": [
            "line_item_id",
            "invoice_id",
            "description",
            "quantity",
            "unit_price_cents",
            "total_cents",
            "period_start",
            "period_end",
            "_source_system",
            "_loaded_at",
        ],
    },
}


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Truncate in FK-safe order
        cur.execute("""
            TRUNCATE TABLE app.invoice_line_items, app.invoices,
                app.usage_events, app.subscriptions,
                app.users, app.accounts
            CASCADE
        """)
        conn.commit()

        for filename, config in TABLE_MAP.items():
            filepath = CSV_DIR / filename
            table = config["table"]
            columns = config["columns"]

            with open(filepath, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = [[row.get(col) or None for col in columns] for row in reader]

            placeholders = ", ".join(["%s"] * len(columns))
            col_names = ", ".join(columns)
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

            cur.executemany(sql, rows)
            conn.commit()
            print(f"[OK] {table}: {cur.rowcount} rows inserted from {filename}")

        # Verify counts
        cur.execute("""
            SELECT schemaname, relname AS table_name, n_live_tup AS rows
            FROM pg_stat_user_tables
            WHERE schemaname = 'app'
            ORDER BY relname
        """)
        print("\n=== Final Row Counts ===")
        for row in cur.fetchall():
            print(f"  {row[1]}: {row[2]}")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Seeding failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

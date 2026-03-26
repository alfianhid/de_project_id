#!/bin/bash
set -euo pipefail

source .env

echo "=== Seeding postgres-source with CSV dummy data ==="

docker compose exec -T postgres-source psql \
    -U "$SOURCE_DB_USER" \
    -d "$SOURCE_DB_NAME" << 'EOF'

SET search_path TO app, public;

-- ── Accounts ────────────────────────────────────────────────────────────────
TRUNCATE TABLE app.invoice_line_items, app.invoices,
               app.usage_events, app.subscriptions,
               app.users, app.accounts CASCADE;

COPY app.accounts (
    account_id, account_name, industry, company_size_band,
    region, account_tier, csm_user_id, is_active, created_at, updated_at
)
FROM '/tmp/csv/raw_accounts.csv'
WITH (FORMAT csv, HEADER true, NULL '');

-- ── Users ───────────────────────────────────────────────────────────────────
COPY app.users (
    user_id, account_id, email, first_name, last_name,
    role, is_active, created_at, updated_at
)
FROM '/tmp/csv/raw_users.csv'
WITH (FORMAT csv, HEADER true, NULL '');

-- ── Subscriptions ───────────────────────────────────────────────────────────
COPY app.subscriptions (
    id, customer, plan_id, status, billing_interval,
    currency, amount, created, canceled_at, ended_at,
    trial_start, trial_end, _source_system, _loaded_at
)
FROM '/tmp/csv/raw_subscriptions.csv'
WITH (FORMAT csv, HEADER true, NULL '');

-- ── Usage Events ────────────────────────────────────────────────────────────
COPY app.usage_events (
    event_id, account_id, user_id, event_type, feature_name,
    session_id, event_timestamp, metadata,
    _kafka_offset, _kafka_partition, _loaded_at
)
FROM '/tmp/csv/raw_usage_events.csv'
WITH (FORMAT csv, HEADER true, NULL '');

-- ── Invoices ────────────────────────────────────────────────────────────────
COPY app.invoices (
    invoice_id, account_id, subscription_id, status, currency,
    total_cents, due_date, paid_at, created_at, _source_system, _loaded_at
)
FROM '/tmp/csv/raw_invoices.csv'
WITH (FORMAT csv, HEADER true, NULL '');

-- ── Invoice Line Items ──────────────────────────────────────────────────────
COPY app.invoice_line_items (
    line_item_id, invoice_id, description, quantity,
    unit_price_cents, total_cents, period_start, period_end,
    _source_system, _loaded_at
)
FROM '/tmp/csv/raw_invoice_line_items.csv'
WITH (FORMAT csv, HEADER true, NULL '');

-- ── Verify row counts ───────────────────────────────────────────────────────
SELECT
    'app.accounts'           AS table_name, COUNT(*) AS rows FROM app.accounts
UNION ALL SELECT 'app.users',              COUNT(*) FROM app.users
UNION ALL SELECT 'app.subscriptions',      COUNT(*) FROM app.subscriptions
UNION ALL SELECT 'app.usage_events',       COUNT(*) FROM app.usage_events
UNION ALL SELECT 'app.invoices',           COUNT(*) FROM app.invoices
UNION ALL SELECT 'app.invoice_line_items', COUNT(*) FROM app.invoice_line_items
ORDER BY table_name;

EOF

echo "=== Seeding complete ==="
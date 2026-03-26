-- 02_create_tables.sql
-- Defines all source tables that mirror the CSV dummy data structure.
-- The ingestion DAGs read from these tables and write to BigQuery raw layer.
-- NOTE: Tables have NO PRIMARY KEY or FOREIGN KEY constraints to allow raw dirty data
-- (duplicates, orphans, NULL values) to be loaded as-is for data quality testing.

SET search_path TO app, public;


-- ── Accounts ────────────────────────────────────────────────────────────────
-- Source: CRM system / application database
-- One row per tenant (company). Used by dbt snapshot for SCD Type 2.

CREATE TABLE IF NOT EXISTS app.accounts (
    account_id          VARCHAR(64),
    account_name        VARCHAR(255)    NOT NULL,
    industry            VARCHAR(100),
    company_size_band   VARCHAR(50),
    region              VARCHAR(100),
    account_tier        VARCHAR(50),
    csm_user_id         VARCHAR(64),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounts_tier    ON app.accounts (account_tier);
CREATE INDEX IF NOT EXISTS idx_accounts_region  ON app.accounts (region);
CREATE INDEX IF NOT EXISTS idx_accounts_updated ON app.accounts (updated_at);


-- ── Users ───────────────────────────────────────────────────────────────────
-- Source: application database
-- One row per user. FK to accounts (not enforced - allows orphan users).

CREATE TABLE IF NOT EXISTS app.users (
    user_id             VARCHAR(64),
    account_id          VARCHAR(64),
    email               VARCHAR(255)    NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    role                VARCHAR(50),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_account   ON app.users (account_id);
CREATE INDEX IF NOT EXISTS idx_users_email     ON app.users (email);


-- ── Subscriptions ───────────────────────────────────────────────────────────
-- Source: billing system (Stripe-format)
-- Amounts stored in cents (integer). Staging layer divides by 100.
-- Unix epoch timestamps match the Stripe raw payload format.

CREATE TABLE IF NOT EXISTS app.subscriptions (
    id                  VARCHAR(64),
    customer            VARCHAR(64),
    plan_id             VARCHAR(64)     NOT NULL,
    status              VARCHAR(50)     NOT NULL,
    billing_interval    VARCHAR(20),
    currency            CHAR(3),
    amount              INTEGER         NOT NULL,
    created             BIGINT,
    canceled_at         BIGINT          DEFAULT 0,
    ended_at            BIGINT          DEFAULT 0,
    trial_start         BIGINT          DEFAULT 0,
    trial_end           BIGINT          DEFAULT 0,
    _source_system      VARCHAR(50)     DEFAULT 'postgres_source',
    _loaded_at          TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_customer ON app.subscriptions (customer);
CREATE INDEX IF NOT EXISTS idx_subscriptions_status   ON app.subscriptions (status);


-- ── Usage Events ────────────────────────────────────────────────────────────
-- Source: product event stream (Kafka consumer writes here in addition to BQ)
-- High-volume table. event_timestamp in milliseconds.

CREATE TABLE IF NOT EXISTS app.usage_events (
    event_id            VARCHAR(128),
    account_id          VARCHAR(64),
    user_id             VARCHAR(64),
    event_type          VARCHAR(100),
    feature_name        VARCHAR(100),
    session_id          VARCHAR(128),
    event_timestamp     BIGINT,
    metadata            TEXT,
    _kafka_offset       INTEGER,
    _kafka_partition    INTEGER,
    _loaded_at          TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_usage_events_account   ON app.usage_events (account_id);
CREATE INDEX IF NOT EXISTS idx_usage_events_type      ON app.usage_events (event_type);
CREATE INDEX IF NOT EXISTS idx_usage_events_loaded    ON app.usage_events (_loaded_at);


-- ── Invoices ────────────────────────────────────────────────────────────────
-- Source: billing system (Stripe-format)

CREATE TABLE IF NOT EXISTS app.invoices (
    invoice_id          VARCHAR(64),
    account_id          VARCHAR(64),
    subscription_id     VARCHAR(64),
    status              VARCHAR(50),
    currency            CHAR(3),
    total_cents         INTEGER,
    due_date            DATE,
    paid_at             TIMESTAMP,
    created_at          TIMESTAMP       DEFAULT NOW(),
    _source_system      VARCHAR(50)     DEFAULT 'postgres_source',
    _loaded_at          TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_invoices_account      ON app.invoices (account_id);
CREATE INDEX IF NOT EXISTS idx_invoices_subscription ON app.invoices (subscription_id);
CREATE INDEX IF NOT EXISTS idx_invoices_status       ON app.invoices (status);


-- ── Invoice Line Items ──────────────────────────────────────────────────────
-- Source: billing system (Stripe-format)

CREATE TABLE IF NOT EXISTS app.invoice_line_items (
    line_item_id        VARCHAR(64),
    invoice_id          VARCHAR(64),
    description         TEXT,
    quantity            INTEGER         DEFAULT 1,
    unit_price_cents    INTEGER,
    total_cents         INTEGER,
    period_start        DATE,
    period_end          DATE,
    _source_system      VARCHAR(50)     DEFAULT 'postgres_source',
    _loaded_at          TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_line_items_invoice ON app.invoice_line_items (invoice_id);
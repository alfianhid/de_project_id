# Technical Implementation Guide
## SaaS Data Platform — Current Implementation Reference

**Last Updated:** Based on Current Codebase  
**Stack:** Apache Airflow 3.1.8 · dbt-core · Apache Spark 4.0.0 · Apache Kafka · BigQuery · Docker

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Docker Services Architecture](#3-docker-services-architecture)
4. [Airflow Orchestration](#4-airflow-orchestration)
5. [dbt Transformation Layer](#5-dbt-transformation-layer)
6. [Kafka Streaming](#6-kafka-streaming)
7. [Spark Batch Processing](#7-spark-batch-processing)
8. [Data Quality & Testing](#8-data-quality--testing)
9. [CI/CD Pipeline](#9-cicd-pipeline)
10. [Environment Configuration](#10-environment-configuration)

---

## 1. Architecture Overview

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW (Orchestrator)                    │
│                                                                     │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐ │
│  │ Kafka Consumer │  │   Spark Jobs   │  │    dbt Transformations │ │
│  │    DAG         │  │    DAGs        │  │        DAGs            │ │
│  └────────────────┘  └────────────────┘  └────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
         │                     │                     │
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA PLATFORM                               │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Source Systems │────▶│  Kafka Topics   │────▶│   GCS Bucket    │
│  (Postgres/CRM) │     │  (Streaming)    │     │  (Data Lake)    │
│                 │     │                 │     │                 │
│ - Subscriptions │     │ - usage.events  │     │ /raw/events/    │
│ - Accounts      │     │ - subscription  │     │ /raw/subscribe/ │
│ - Users         │     │   .events       │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                                               ▲
         │           ┌─────────────────┐                 │
         └──────────▶│   Apache Spark  │─────────────────┘
                     │  (Batch Jobs)   │   (Read from GCS)
                     └─────────────────┘
                              │
                              │ Write
                              ▼
                     ┌─────────────────┐
                     │   BIGQUERY      │
                     │ (Staging Layer) │
                     │                 │
                     │ - stg_events    │
                     │ - stg_subscribe │
                     └─────────────────┘
                              │
                              │ dbt Transform
                              ▼
                     ┌─────────────────┐
                     │   BIGQUERY      │
                     │ (Data Warehouse)│
                     │                 │
                     │ - Intermediate  │
                     │ - Transform     │
                     │ - Reporting     │
                     └─────────────────┘
```

### Architecture Principles

1. **Cost-Effective Data Lake (GCS)**: 
   - Raw data lands in GCS, not directly in BigQuery
   - 10-100x cheaper than BigQuery storage for raw data
   - Kafka streams to GCS in batches (controlled by Airflow)
   - Enables replay and reprocessing without cost penalty

2. **Data Warehouse (BigQuery)**:
   - Only curated, transformed data
   - Spark jobs move data from GCS to BigQuery staging
   - dbt transforms within BigQuery (staging → transform → reporting)
   - Optimized for analytics with partitioning and clustering

3. **Airflow as Central Orchestrator**:
   - **Kafka Consumer DAG**: Manages streaming to GCS
   - **Spark Job DAGs**: Schedules and monitors batch processing
   - **dbt DAGs**: Orchestrates transformation pipeline
   - **Quality DAGs**: Validates data across all layers
   - Single pane of glass for monitoring and alerting

4. **Layered Data Architecture**:
   - **Bronze (GCS)**: Raw data from Kafka and batch sources
   - **Silver (BigQuery Staging)**: Cleansed, deduplicated data
   - **Gold (BigQuery Reporting)**: Business-ready aggregates

### Data Flow

1. **Ingestion**:
   - Source systems produce events to Kafka topics
   - Airflow-managed Kafka consumer writes batches to GCS (Data Lake)
   - Batch sources (Postgres) extracted by Airflow DAGs

2. **Processing**:
   - Airflow triggers Spark jobs on schedule
   - Spark reads from GCS, applies transformations
   - Spark writes to BigQuery staging tables

3. **Transformation**:
   - Airflow orchestrates dbt pipeline
   - dbt transforms staging → intermediate → transform → reporting
   - All within BigQuery (no data movement)

4. **Quality**:
   - Airflow quality DAG runs post-transformation
   - Validates row counts, MRR reconciliation, freshness
   - Alerts on anomalies

---

## 2. Prerequisites

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 16 GB | 32 GB |
| Disk | 50 GB SSD | 100+ GB SSD |
| OS | Ubuntu 22.04 LTS | Ubuntu 22.04 LTS |
| Docker | 25.x+ | 26.x+ |
| Docker Compose | v2.24+ | v2.27+ |

### Required Tools

```bash
# Docker & Docker Compose
docker --version  # >= 25.x
docker compose version  # >= v2.24

# Google Cloud SDK (for BigQuery operations)
gcloud --version

# Make
make --version
```

### GCP Setup

#### Required GCP Services

1. **Google Cloud Storage (GCS)** - Data Lake
   - Create bucket: `your-project-data-lake`
   - Location: Same as BigQuery (asia-southeast2)
   - Storage class: Standard
   
2. **BigQuery** - Data Warehouse
   - Enable BigQuery API
   - Create datasets: raw, staging, transform, reporting
   
3. **IAM & Service Account**
   - Create service account: `data-platform@your-project.iam.gserviceaccount.com`
   - Grant roles:
     - `roles/storage.objectAdmin` (GCS read/write)
     - `roles/bigquery.dataEditor` (BigQuery tables)
     - `roles/bigquery.jobUser` (BigQuery queries)
   
4. **Download credentials**
   ```bash
   mkdir -p secrets
   # Download JSON key from GCP Console → IAM → Service Accounts → Keys
   # Save as: secrets/gcp-sa-key.json
   ```

#### Cost Optimization

- **GCS (Data Lake)**: ~$0.02/GB/month
- **BigQuery Storage**: ~$0.02/GB/month (active), $0.01/GB/month (long-term)
- **BigQuery Query**: $5/TB processed

**Best Practice**: Keep 90 days of raw data in GCS, only hot data in BigQuery staging

---

## 3. Docker Services Architecture

### 3.1 Service Inventory

The platform runs 14 services orchestrated by Docker Compose:

| Service | Image | Purpose | External Port |
|---------|-------|---------|---------------|
| **postgres** | postgres:16-alpine | Airflow metadata DB | - |
| **postgres-source** | postgres:16-alpine | Source application DB | 5433 |
| **redis** | redis:7.4-alpine | Celery broker | - |
| **airflow-webserver** | Custom (3.1.8) | Airflow UI/API | 8080 |
| **airflow-scheduler** | Custom (3.1.8) | DAG scheduling | - |
| **airflow-triggerer** | Custom (3.1.8) | Deferred operators | - |
| **airflow-worker** | Custom (3.1.8) | Task execution | - |
| **zookeeper** | confluentinc/cp-zookeeper:7.7.0 | Kafka coordination | - |
| **kafka** | confluentinc/cp-kafka:7.7.0 | Message broker | 9092 |
| **kafka-ui** | provectuslabs/kafka-ui:v0.7.2 | Kafka management | 8090 |
| **schema-registry** | confluentinc/cp-schema-registry:7.7.0 | Avro schemas | 8081 |
| **kafka-consumer** | Custom | BQ streaming consumer | - |
| **spark-master** | Custom (4.0.0) | Spark cluster master | 7077, 8082 |
| **spark-worker** | Custom (4.0.0) | Spark worker nodes | - |

### 3.2 Custom Images

**Airflow Image** (`docker/airflow/Dockerfile`):
- Based on `apache/airflow:3.1.8-python3.11`
- Uses UV package manager for faster installs
- Installs: providers for Google, Spark, Kafka, Celery, plus dbt

**Spark Image** (`docker/spark/Dockerfile`):
- Based on `apache/spark:4.0.0`
- Includes BigQuery connector and GCS connector
- UV package manager for Python dependencies

### 3.3 Network & Volumes

- **Network**: `data-platform-net` (bridge driver)
- **Volumes**:
  - `postgres-data`: Airflow metadata persistence
  - `postgres-source-data`: Source database persistence
  - `kafka-data`: Kafka log persistence
  - `zookeeper-data`: ZK data persistence

### 3.4 Quick Start

```bash
# 1. Bootstrap the entire environment
./scripts/bootstrap.sh

# 2. Or manually:
make build    # Build custom images
make up       # Start all services
make dbt-seed # Load seed data
make create-topics  # Create Kafka topics
```

---

## 4. Airflow Orchestration

### 4.1 DAG Architecture

All DAGs are defined in `airflow/dags/`:

```
airflow/dags/
├── dag_master_pipeline.py          # Master orchestrator
├── ingestion/
│   ├── dag_kafka_consumer.py       # Real-time streaming
│   ├── dag_stripe_ingestion.py     # Daily Stripe extract
│   └── dag_spark_usage_aggregation.py  # Spark batch job
├── transformation/
│   └── dag_dbt_run.py              # dbt pipeline
├── quality/
│   └── dag_data_quality.py         # Post-transform validation
└── utils/
    └── alerts.py                   # Slack notifications
```

### 4.2 Master Pipeline (dag_master_pipeline.py)

**Schedule**: Daily at 1:00 AM (Asia/Jakarta timezone)

**Execution Flow**:
```
[stripe_ingestion, spark_aggregation] (parallel)
              ↓
        dbt_transformation
              ↓
        data_quality_checks
```

**Key Features**:
- Uses `TriggerDagRunOperator` to coordinate child DAGs
- Waits for completion with 60s poke interval
- Retries once with 5-minute delay

### 4.3 Ingestion DAGs

#### Kafka Consumer (dag_kafka_consumer.py)
- **Type**: Long-running (@once)
- **Purpose**: Streams Kafka events to **GCS** (not BigQuery)
- **Consumer**: `gcs_sink_consumer.py`
- **Target**: `gs://bucket/raw/events/`
- **Why GCS?**: 50x cheaper than BigQuery streaming inserts

**Airflow Management**:
```python
# dag_kafka_consumer.py
kafka_consumer = BashOperator(
    task_id='kafka_to_gcs',
    bash_command='python kafka/consumers/gcs_sink_consumer.py',
    # This runs continuously as a long-running task
)
```

**Key Points**:
- Runs as a service-like task within Airflow
- Monitored via Airflow UI (task instance status)
- Can be restarted if it fails
- Consumer lag alerts via Airflow sensors

#### Stripe Ingestion (dag_stripe_ingestion.py)
- **Schedule**: Daily at 2:00 AM
- **Purpose**: Extracts subscription data from source DB to BigQuery
- **Source**: `postgres-source.app.subscriptions`
- **Target**: `raw.raw_subscriptions`

#### Spark Usage Aggregation (dag_spark_usage_aggregation.py)
- **Type**: Triggered (no schedule)
- **Purpose**: Submits Spark batch job for daily aggregations
- **Job**: `batch_usage_aggregation.py`
- **Input**: `raw.raw_usage_events`
- **Output**: `staging.stg_usage_events_daily`

### 4.4 Transformation DAG (dag_dbt_run.py)

**Schedule**: Daily at 4:00 AM

**Execution Stages**:
```
dbt_deps
  ↓
dbt_run_staging → dbt_test_staging
  ↓
dbt_run_intermediate
  ↓
dbt_run_transform → dbt_test_transform
  ↓
dbt_snapshots
  ↓
dbt_run_reporting → dbt_test_reporting
  ↓
dbt_generate_docs
  ↓
upload_dbt_artifacts
```

**Features**:
- Sequential execution with tests between stages
- Uploads manifest.json and catalog.json to GCS
- Full refresh on first run of the month

### 4.5 Data Quality DAG (dag_data_quality.py)

**Type**: Triggered (no schedule)

**Quality Checks**:
1. **Row Volume**: Detects 30%+ deviation from 7-day average
2. **MRR Reconciliation**: Validates staging vs transform MRR consistency
3. **Freshness**: Ensures reporting tables updated within 6 hours

**Alerting**: Sends Slack notifications on failure

---

## 5. dbt Transformation Layer

### 5.1 Airflow-dbt Integration

**Orchestration**: dbt transformations are **executed by Airflow**, not run manually.

```python
# In dag_dbt_run.py
from airflow.operators.bash import BashOperator

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='dbt run --select staging --target prod',
    cwd='/opt/dbt',
)

dbt_test_staging = BashOperator(
    task_id='dbt_test_staging',
    bash_command='dbt test --select staging --target prod',
    cwd='/opt/dbt',
)

# DAG flow: dbt_run_staging >> dbt_test_staging >> ...
```

**Benefits**:
- **Scheduling**: Transformations run on schedule after data ingestion completes
- **Dependency Management**: dbt waits for Spark jobs to finish loading staging data
- **Failure Handling**: Airflow retries and alerts on dbt failures
- **Monitoring**: Single UI for all pipeline stages
- **Full Refresh Control**: Airflow can trigger `--full-refresh` monthly

**Architecture**:
```
┌─────────────────┐
│     Airflow     │
│  (Orchestrator) │
└────────┬────────┘
         │ Triggers
         ▼
┌─────────────────┐
│      dbt        │
│   (Executes)    │
└────────┬────────┘
         │ Generates SQL
         ▼
┌─────────────────┐
│    BigQuery     │
│  (Data Warehouse)│
└─────────────────┘
```

### 5.2 Model Structure

```
dbt/models/
├── staging/           # Views - clean source data
├── intermediate/      # Ephemeral - business logic
├── transform/         # Incremental - core fact tables
└── reporting/         # Tables - business reports
```

### 5.2 Staging Layer

**stg_subscriptions** (`models/staging/stg_subscriptions.sql`):
- **Materialization**: View
- **Source**: `raw.raw_subscriptions`
- **Transformations**:
  - Normalizes MRR from cents to dollars
  - Standardizes status and billing interval values
  - Validates currency codes
- **Tests**:
  - Unique `subscription_id`
  - Not null checks on critical fields
  - Accepted values for status and billing_interval

### 5.3 Intermediate Layer

**int_account_metrics** (`models/intermediate/int_account_metrics.sql`):
- **Materialization**: Ephemeral
- **Purpose**: Account-level aggregations
- **Metrics**:
  - Total/active/cancelled/trialing subscription counts
  - Total MRR and ARR per account

**int_plan_metrics** (`models/intermediate/int_plan_metrics.sql`):
- **Materialization**: Ephemeral
- **Purpose**: Plan performance metrics
- **Metrics**:
  - Plan adoption rates
  - At-risk subscription count
  - Average MRR per plan

### 5.4 Transform Layer

**fct_subscriptions** (`models/transform/fct_subscriptions.sql`):
- **Materialization**: Incremental (merge strategy)
- **Surrogate Key**: `dbt_utils.generate_surrogate_key(['subscription_id'])`
- **Partitioning**: By `started_at` (monthly)
- **Clustering**: By `account_id`, `subscription_status`
- **Features**:
  - Handles SCD Type 2 via snapshots
  - Computes ARR from MRR (× 12)
  - Tracks subscription lifecycle timestamps

### 5.5 Reporting Layer

**rpt_subscription_monthly** (`models/reporting/rpt_subscription_monthly.sql`):
- **Materialization**: Table (partitioned by snapshot_month)
- **Purpose**: Monthly subscription snapshot for BI
- **Dimensions**:
  - Account attributes (name, industry, region, tier)
  - Plan details
  - Health status (healthy, at_risk, churned, new)
  - Value tier (high_value, mid_value, low_value)

**rpt_account_overview** (`models/reporting/rpt_account_overview.sql`):
- **Materialization**: Table
- **Purpose**: 360-degree account health view
- **Metrics**:
  - Subscription counts by status
  - Total MRR/ARR
  - Health segment (champion, healthy, at_risk, critical)
  - Product adoption level (full, partial, minimal)
  - Engagement status (highly_engaged, engaged, low_engagement)

### 5.6 Snapshots

**snap_accounts** (`snapshots/snap_accounts.sql`):
- **Strategy**: Timestamp-based SCD Type 2
- **Track Columns**: All account dimension columns
- **Updated At**: `updated_at`
- **Use Case**: Track account attribute changes over time

### 5.7 Tests

**Generic Tests** (defined in schema.yml files):
- `unique`: Primary key validation
- `not_null`: Required field validation
- `accepted_values`: Enum validation
- `relationships`: Foreign key validation

**Custom Tests**:
- `tests/assert_no_negative_mrr.sql`: Ensures active subscriptions have positive MRR

**Packages Used**:
- `dbt_utils`: Common utilities and tests
- `dbt_expectations`: Advanced data quality tests
- `audit_helper`: Data auditing utilities
- `dbt_date`: Date dimension helpers

---

## 6. Kafka Streaming

### 6.1 Topics

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `product.usage.events` | 6 | Application usage events |
| `billing.subscription.events` | 3 | Subscription lifecycle events |

### 6.2 Schema Registry

**Location**: `kafka/schemas/usage_event.avsc`

**UsageEvent Schema**:
```json
{
  "type": "record",
  "name": "UsageEvent",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "feature_name", "type": ["null", "string"]},
    {"name": "session_id", "type": ["null", "string"]},
    {"name": "event_timestamp", "type": "long"},
    {"name": "metadata", "type": ["null", "string"]}
  ]
}
```

### 6.3 Producer

**File**: `kafka/producers/usage_event_producer.py`

**Features**:
- Simulates realistic usage events
- Avro serialization with Schema Registry
- Random data generation for 50 accounts, 200 users
- Event types: feature_used, api_call, export, login, settings_change
- Features: dashboard, reporting, api_v2, bulk_export, webhooks

**Usage**:
```bash
# Run inside Airflow container
docker compose exec airflow-worker python kafka/producers/usage_event_producer.py
```

### 6.4 Consumer (GCS Sink)

**File**: `kafka/consumers/gcs_sink_consumer.py`

**Architecture Decision**: 
- ❌ **NOT** streaming directly to BigQuery (expensive streaming inserts)
- ✅ **BATCHING** to GCS first (cost-effective), then Spark loads to BigQuery

**Cost Comparison**:
| Method | Cost per 1M events | Best For |
|--------|-------------------|----------|
| BigQuery Streaming | ~$0.05 | Real-time analytics (< 1 min latency) |
| GCS + Spark Batch | ~$0.001 | Cost-conscious, 5-15 min latency acceptable |
| **Savings: ~50x** |

**Features**:
- Consumer group: `data-warehouse-consumer`
- Batch processing: 500 records or 30-second commit interval
- **Target**: GCS bucket path: `gs://your-bucket/raw/events/YYYY/MM/DD/HH/`
- File format: Parquet (compressed, columnar)
- Metadata enrichment: `_kafka_offset`, `_kafka_partition`, `_loaded_at`
- **Managed by Airflow**: Runs as long-running task in `dag_kafka_consumer.py`

**Airflow Integration**:
```python
# In dag_kafka_consumer.py
kafka_consumer_task = BashOperator(
    task_id='consume_kafka_to_gcs',
    bash_command='python kafka/consumers/gcs_sink_consumer.py',
    executor_config={
        # Ensure this runs on a dedicated worker or as a service
    }
)
```

**Deployment Options**:
1. **As Airflow Task**: Long-running task in `dag_kafka_consumer.py` (recommended)
2. **As Docker Service**: Separate `kafka-consumer` service (legacy mode)

**File Naming Convention**:
```
gs://your-bucket/raw/events/
  └── 2024/
      └── 01/
          └── 15/
              └── 14/
                  ├── events-00001.parquet
                  ├── events-00002.parquet
                  └── events-00003.parquet
```

**Monitoring**:
- Airflow UI: Track `dag_kafka_consumer.py` task status
- GCS Console: Verify file creation in bucket
- Consumer lag: Kafka UI at http://localhost:8090

---

## 7. Spark Batch Processing

### 7.1 Spark Architecture

- **Master**: `spark-master` service (port 7077, UI on 8082)
- **Workers**: 2 replicas (`spark-worker` service)
- **Worker Resources**: 4GB RAM, 2 cores each
- **Mode**: Standalone cluster

### 7.2 Airflow-Spark Integration

**Orchestration**: Spark jobs are **not** run manually - they are orchestrated by Airflow DAGs.

```python
# In dag_spark_usage_aggregation.py
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/opt/spark/jobs/batch_usage_aggregation.py',
    conn_id='spark_default',
    conf={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
    },
    application_args=['{{ ds }}'],  # Execution date from Airflow
)
```

**Benefits**:
- **Dependency management**: Spark job waits for upstream Kafka → GCS completion
- **Scheduling**: Runs on schedule (daily) or on-demand
- **Monitoring**: Success/failure tracked in Airflow UI
- **Retries**: Automatic retry on transient failures
- **Notifications**: Slack alerts on failure

### 7.3 Jobs

#### batch_usage_aggregation.py

**Purpose**: Daily aggregation of usage events from GCS → BigQuery

**Data Flow**:
```
GCS (Data Lake)              Spark Processing            BigQuery (Warehouse)
     │                              │                              │
     ▼                              ▼                              ▼
┌──────────┐              ┌──────────────────┐            ┌──────────────────┐
│ raw/     │─────────────▶│ 1. Read Parquet  │───────────▶│ staging.         │
│ events/  │              │ 2. Deduplicate   │            │ stg_usage_       │
│ *.parquet│              │ 3. Clean/Validate│            │ events_daily     │
└──────────┘              │ 4. Aggregate     │            └──────────────────┘
                          │ 5. Write to BQ   │
                          └──────────────────┘
```

**Parameters**:
- `processing_date`: Date to process (default: yesterday)
- Passed via Airflow: `application_args=['{{ ds }}']`

**Processing Steps**:
1. **Read from GCS**: Load Parquet files from `gs://bucket/raw/events/YYYY/MM/DD/`
2. **Deduplicate**: Keep latest record by `event_id` using window function
3. **Clean**: Normalize case, trim strings, validate nulls
4. **Aggregate**: Group by `event_date`, `account_id`, `event_type`, `feature_name`
5. **Compute metrics**: `event_count`, `unique_users`, `unique_sessions`, timestamps
6. **Write to BigQuery**: Load to `staging.stg_usage_events_daily`

**BigQuery Load Options**:
- **Method**: `direct` (Spark BigQuery connector)
- **Partitioned by**: `event_date` (DAY)
- **Clustered by**: `account_id`, `event_type`
- **Mode**: `overwrite` (daily full refresh of partition)
- **Cost**: ~$0.001 per 1M rows (vs $0.05 for streaming)

#### historical_backfill.py

**Purpose**: Historical data reprocessing from GCS

**Airflow Integration**:
```python
# Trigger via Airflow with parameters
backfill_job = SparkSubmitOperator(
    task_id='historical_backfill',
    application='/opt/spark/jobs/historical_backfill.py',
    application_args=[
        '--start-date', '{{ ds }}',
        '--end-date', '{{ macros.ds_add(ds, 7) }}',
        '--batch-days', '1'
    ],
)
```

**Parameters**:
- `--start-date`: Start date (YYYY-MM-DD)
- `--end-date`: End date (YYYY-MM-DD)
- `--batch-days`: Days to process per batch (default: 1)
- `--dry-run`: Validate without writing (useful for testing)
- `--source-path`: GCS path (default: `gs://bucket/raw/events/`)
- `--target-table`: BigQuery table (default: `staging.stg_usage_events_daily`)

**Use Cases**:
1. **Schema migration**: Backfill with new columns
2. **Data quality fix**: Reprocess corrupted data
3. **Historical load**: Initial data migration
4. **Replay**: Reprocess from GCS archive

**Usage Examples**:
```bash
# Via Airflow UI (trigger DAG with config):
{
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "batch_days": 7
}

# Or manually (for testing):
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/historical_backfill.py \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --batch-days 7
```

### 7.3 Session Factory

**File**: `spark/jobs/spark_session_factory.py`

**Configuration**:
- Master URL: `spark://spark-master:7077`
- BigQuery connector: `spark-bigquery-connector.jar`
- GCS connector: `gcs-connector.jar`
- Adaptive query execution: Enabled
- Shuffle partitions: 200
- Auth: Service account via `GOOGLE_APPLICATION_CREDENTIALS`

---

## 8. Data Quality & Testing

### 8.1 dbt Tests

**Staging Tests** (`models/staging/schema.yml`):
- `stg_subscriptions`: Unique subscription_id, not null checks, accepted values

**Transform Tests** (`models/transform/schema.yml`):
- `fct_subscriptions`: Unique surrogate key, not null checks, custom MRR validation

**Reporting Tests** (`models/reporting/schema.yml`):
- Row count validations
- Relationship tests to dimension tables

### 8.2 SQLFluff Linting

**Config** (`dbt/.sqlfluff`):
- Templater: dbt
- Dialect: bigquery
- Indent: 4 spaces
- Capitalization: lower for keywords and functions

**Usage**:
```bash
sqlfluff lint dbt/models --dialect bigquery --templater dbt
```

### 8.3 Data Quality DAG

Runs post-transformation checks:
- **Volume Anomaly Detection**: Flags 30%+ row count deviation
- **MRR Reconciliation**: Validates consistency across layers
- **Freshness Check**: Ensures data updated within SLA (6 hours)

---

## 9. CI/CD Pipeline

### 9.1 GitHub Actions Workflows

#### CI Airflow (.github/workflows/ci_airflow.yml)

**Trigger**: PR to main/develop with changes in `airflow/**`

**Steps**:
1. Checkout code
2. Setup Python 3.11
3. Install Airflow 3.0.0 + providers
4. Initialize Airflow DB (SQLite for CI)
5. Run pytest on DAGs
6. Generate coverage report

**Tests**:
- DAG import validation (no syntax errors)
- Expected DAGs present
- No cycles in DAGs
- dbt DAG layer order validation

#### CI dbt (.github/workflows/ci_dbt.yml)

**Trigger**: PR to main/develop with changes in `dbt/**`

**Steps**:
1. Checkout code
2. Setup Python 3.11
3. Install dbt + SQLFluff
4. Authenticate to GCP
5. Generate temporary profiles.yml
6. Run dbt deps
7. SQLFluff lint
8. dbt compile
9. dbt run staging
10. dbt test staging
11. Cleanup CI dataset

**Note**: Creates temporary BigQuery dataset `transform_ci_${run_id}` for testing

#### CD Deploy (.github/workflows/cd_deploy.yml)

**Trigger**: Push to main branch

**Environment**: production

**Steps**:
1. Authenticate to GCP
2. Install dbt
3. SSH to production host
4. Pull latest code
5. Rebuild Airflow images
6. Rolling restart of services
7. Refresh dbt packages
8. Slack notification (success/failure)

### 9.2 Required Secrets

Configure in GitHub Settings > Secrets and Variables > Actions:

| Secret | Purpose |
|--------|---------|
| `GCP_PROJECT_ID` | GCP project identifier |
| `GCP_SA_KEY_JSON` | Service account JSON key |
| `PROD_HOST` | Production server IP/hostname |
| `PROD_SSH_USER` | SSH username |
| `PROD_SSH_KEY` | SSH private key |
| `SLACK_WEBHOOK_URL` | Slack notifications (optional) |

---

## 10. Environment Configuration

### 10.1 Environment Variables (.env)

**Core Sections**:

```bash
# GCP Configuration
GCP_PROJECT_ID=your-project-id
GCP_REGION=asia-southeast2
BIGQUERY_DATASET_RAW=raw
BIGQUERY_DATASET_STAGING=staging
BIGQUERY_DATASET_TRANSFORM=transform
BIGQUERY_DATASET_REPORTING=reporting
GOOGLE_APPLICATION_CREDENTIALS=/opt/secrets/gcp-sa-key.json

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
AIRFLOW__CORE__SECRET_KEY=your-secret-key
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__DASHBOARD_USER_USERNAME=admin
AIRFLOW__CORE__DASHBOARD_USER_PASSWORD=admin

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC_USAGE_EVENTS=product.usage.events
KAFKA_TOPIC_SUBSCRIPTION_EVENTS=billing.subscription.events
KAFKA_CONSUMER_GROUP=data-warehouse-consumer

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077

# Source Database Configuration
SOURCE_DB_HOST=postgres-source
SOURCE_DB_PORT=5432
SOURCE_DB_USER=deprojectuser
SOURCE_DB_PASSWORD=deprojectpass
SOURCE_DB_NAME=deproject_db
SOURCE_DB_SCHEMA=app

# dbt Configuration
DBT_PROJECT_NAME=de_project_id
DBT_PROFILES_DIR=/opt/dbt
DBT_TARGET=prod
```

### 10.2 dbt Profiles (dbt/profiles.yml)

```yaml
de_project_id:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: transform_dev
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      location: asia-southeast2
      threads: 4
      timeout_seconds: 300
    
    prod:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: transform
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      location: asia-southeast2
      priority: batch
      threads: 8
      timeout_seconds: 600
```

### 10.3 Project Configuration (dbt/dbt_project.yml)

```yaml
name: de_project_id
version: "1.0.0"
config-version: 2
profile: de_project_id

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
analysis-paths: ["analysis"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  de_project_id:
    staging:
      +materialized: view
      +schema: staging
      +tags: ["staging"]
    intermediate:
      +materialized: ephemeral
      +tags: ["intermediate"]
    transform:
      +materialized: incremental
      +schema: transform
      +tags: ["transform"]
      +on_schema_change: append_new_columns
    reporting:
      +materialized: table
      +schema: reporting
      +tags: ["reporting"]

snapshots:
  de_project_id:
    +target_schema: transform
    +strategy: timestamp
    +updated_at: updated_at

seeds:
  de_project_id:
    +schema: raw
    +tags: ["seed"]
```

---

## Appendix A: Useful Commands

```bash
# Build and start
make build
make up

# dbt operations
make dbt-seed
make dbt-run
make dbt-test

# Kafka
make create-topics
make kafka-topics

# Utilities
make airflow-shell
make spark-shell
./scripts/health_check.sh

# Logs
make logs svc=airflow-webserver
make logs svc=spark-master

# Stop everything
make down
```

## Appendix B: Troubleshooting

**Issue**: Airflow webserver not accessible
- Check: `docker compose ps`
- Verify: `curl http://localhost:8080/api/v2/monitor/health`

**Issue**: dbt models failing
- Check logs: `make logs svc=airflow-worker`
- Verify BigQuery connection: `gcloud auth list`

**Issue**: Kafka consumer lag
- Check lag: `./scripts/health_check.sh`
- Restart consumer: `docker compose restart kafka-consumer`

**Issue**: Spark job OOM
- Increase worker memory in docker-compose.yml
- Reduce partition count in Spark job

---

*This guide reflects the current state of the codebase. For updates, check the git history and README.md.*

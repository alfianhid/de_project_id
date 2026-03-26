# SaaS Data Platform

A comprehensive data engineering solution for SaaS businesses, featuring real-time event streaming, batch processing, and automated data transformation pipelines.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         APACHE AIRFLOW                              │
│                    (Central Orchestrator)                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │   Kafka      │  │    Spark     │  │          dbt             │  │
│  │  Consumer    │  │    Jobs      │  │    Transformations       │  │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           DATA FLOW                                 │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Source Systems │────▶│  Kafka Topics   │────▶│   GCS Bucket    │
│  (Postgres/CRM) │     │  (Streaming)    │     │  (Data Lake)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                                               │
         │           ┌─────────────────┐                │
         └──────────▶│   Apache Spark  │◀───────────────┘
                     │  (Batch Jobs)   │  (Read from GCS)
                     └─────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │   BIGQUERY      │
                     │ (Staging Layer) │
                     └─────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │   BIGQUERY      │
                     │ (Data Warehouse)│
                     │ Transform/Report│
                     └─────────────────┘
```

### Architecture Principles

1. **Data Lake (GCS)**: Raw data lands in cost-effective object storage
2. **Data Warehouse (BigQuery)**: Curated, transformed data for analytics
3. **Airflow Orchestration**: Single control plane for all operations
4. **Layered Processing**: Bronze (raw) → Silver (staging) → Gold (reporting)

## Tech Stack

- **Orchestration:** Apache Airflow 3.1.8 (orchestrates all components)
- **Data Lake:** Google Cloud Storage (GCS) - cost-effective raw storage
- **Data Warehouse:** Google BigQuery - analytics and reporting
- **Streaming:** Apache Kafka with Confluent stack
- **Stream Processing:** Kafka → GCS (batch writes)
- **Batch Processing:** Apache Spark 4.0.0 (GCS → BigQuery)
- **Transformation:** dbt (data build tool) within BigQuery
- **Infrastructure:** Docker Compose

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Google Cloud Platform account with BigQuery enabled
- GCP Service Account key with BigQuery permissions

### 1. Clone and Setup

```bash
git clone https://github.com/YOUR_USERNAME/de_project_id.git
cd de_project_id

# Copy environment template
cp .env.example .env

# Edit .env with your GCP project details
vim .env
```

### 2. Place GCP Credentials

```bash
# Place your GCP service account key
mkdir -p secrets
cp /path/to/your/gcp-sa-key.json secrets/gcp-sa-key.json
```

### 3. Bootstrap the Environment

```bash
# Make bootstrap script executable and run
chmod +x scripts/bootstrap.sh
./scripts/bootstrap.sh
```

This will:
- Build Docker images
- Start all services
- Initialize BigQuery datasets
- Load seed data
- Create Kafka topics

### 4. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin/admin |
| Spark UI | http://localhost:8082 | - |
| Kafka UI | http://localhost:8090 | - |

### 5. Run dbt Models

```bash
# Load seed data
make dbt-seed

# Run all models
make dbt-run

# Run tests
make dbt-test
```

## Project Structure

```
.
├── airflow/              # Airflow DAGs and plugins
│   ├── dags/            # Pipeline definitions
│   └── tests/           # DAG unit tests
├── dbt/                 # dbt transformation models
│   ├── models/          # SQL transformations
│   ├── tests/           # Custom data tests
│   └── seeds/           # Static reference data
├── kafka/               # Kafka producers/consumers
│   ├── producers/       # Event producers
│   ├── consumers/       # BigQuery sink consumers
│   └── schemas/         # Avro schemas
├── spark/jobs/          # Spark batch jobs
├── scripts/             # Utility scripts
├── docker/              # Custom Docker images
└── .github/workflows/   # CI/CD pipelines
```

## Key Features

### Centralized Orchestration (Apache Airflow)
- **Single control plane**: Airflow orchestrates Kafka, Spark, and dbt
- **Dependency management**: DAGs define execution order across all systems
- **Monitoring**: Unified view of all pipeline components
- **Scheduling**: Cron-based or event-driven execution

### Cost-Effective Data Lake (GCS)
- **Raw data storage**: Kafka streams to GCS (not directly to BigQuery)
- **Cost optimization**: 10x cheaper than BigQuery for raw storage
- **Schema evolution**: Parquet/Avro formats with versioning
- **Airflow-managed**: Kafka consumer DAG controls batch writes to GCS

### Batch Processing (Apache Spark)
- **Spark jobs orchestrated by Airflow**: Scheduled via SparkSubmitOperator
- **GCS to BigQuery ETL**: Spark reads from Data Lake, writes to Warehouse
- **Historical backfill**: Reprocess any date range on-demand
- **Cost-efficient**: Batch processing vs. streaming inserts

### Data Warehouse (BigQuery)
- **Staging layer**: Spark loads cleansed data from GCS
- **Transformation**: dbt models run within BigQuery
- **Partitioning & clustering**: Optimized for query performance
- **Clear separation**: Storage costs in GCS, compute in BigQuery

### Data Quality
- **dbt tests**: Validate data at every layer
- **Great Expectations**: Advanced data validation
- **Airflow quality DAG**: Post-transformation validation
- **Automated CI/CD**: Quality gates in GitHub Actions

### Observability
- **Airflow UI**: Centralized monitoring of all components
- **Spark UI**: Job execution details
- **Kafka UI**: Consumer lag and topic metrics
- **BigQuery**: Query performance and cost monitoring

## Airflow Orchestration Flow

Airflow acts as the **central orchestrator** coordinating all pipeline components:

### Daily Pipeline Schedule

```
01:00 AM ┌──────────────────────────────────────────────────┐
         │  Master Pipeline (dag_master_pipeline.py)        │
         └────────────────┬─────────────────────────────────┘
                          │ TriggerDagRunOperator
                          ▼
         ┌──────────────────────────────────────────┐
         │ [Stripe Ingestion, Spark Aggregation]    │ (parallel)
         └────────────────┬─────────────────────────┘
                          │ Both must complete
                          ▼
         ┌──────────────────────────────────────────┐
         │  dbt Transformation (dag_dbt_run.py)     │
         │  Staging → Intermediate → Transform      │
         └────────────────┬─────────────────────────┘
                          │
                          ▼
         ┌──────────────────────────────────────────┐
         │  Data Quality Checks                     │
         └──────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Managed By Airflow | Purpose |
|-----------|-------------------|---------|
| **Kafka Consumer** | `dag_kafka_consumer.py` | Long-running task that streams Kafka → GCS |
| **Spark Jobs** | `dag_spark_usage_aggregation.py` | Batch processing GCS → BigQuery |
| **dbt Models** | `dag_dbt_run.py` | SQL transformations in BigQuery |
| **Quality Checks** | `dag_data_quality.py` | Post-transformation validation |

### Why Airflow Orchestration?

1. **Single Control Plane**: One UI to monitor Kafka, Spark, and dbt
2. **Dependency Management**: Spark waits for Kafka → GCS completion
3. **Scheduling**: All components run on coordinated schedule
4. **Failure Handling**: Automatic retries and alerting across all systems
5. **Observability**: Unified logs and metrics

## Development

### Useful Commands

```bash
# Build all services
make build

# Start the stack
make up

# View logs
make logs svc=airflow-webserver

# Open Airflow shell
make airflow-shell

# Open PySpark shell
make spark-shell

# List Kafka topics
make kafka-topics
```

### Running Tests

```bash
# Airflow DAG tests
pytest airflow/tests/ -v

# dbt tests
make dbt-test

# SQL linting
sqlfluff lint dbt/models --dialect bigquery
```

## CI/CD

This project uses GitHub Actions for continuous integration and deployment:

- **CI Airflow:** Tests DAGs on every PR
- **CI dbt:** Lints SQL and runs dbt tests
- **CD Deploy:** Automated deployment to production on merge to main

### Setting up GitHub Secrets

Before the CI/CD workflows can run properly, you need to configure the following secrets in your GitHub repository:

Go to **Settings > Secrets and Variables > Actions > New repository secret**

#### Required Secrets

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `GCP_PROJECT_ID` | Your Google Cloud project ID | `my-project-123456` |
| `GCP_SA_KEY_JSON` | Contents of your GCP service account JSON key | `{ "type": "service_account", ... }` |

#### Optional Secrets (for CD/Deployment)

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `PROD_HOST` | IP or hostname of production server | `203.0.113.10` or `prod.example.com` |
| `PROD_SSH_USER` | SSH username for production server | `ubuntu` |
| `PROD_SSH_KEY` | Private SSH key (the public key must be in `~/.ssh/authorized_keys` on the server) | `-----BEGIN OPENSSH PRIVATE KEY-----...` |
| `SLACK_WEBHOOK_URL` | Slack webhook for deployment notifications | `https://hooks.slack.com/services/...` |

**Note:** The workflow files contain placeholder/test values that will work for initial testing. Update these secrets before production deployment.

## Documentation

- [Technical Implementation Guide](./technical_implementation_guide.md)
- [Production Operations Guide](./production_ops_and_handover_guide.md)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For questions or support, please open an issue on GitHub.

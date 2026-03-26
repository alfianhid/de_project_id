# dbt Project: de_project_id

This dbt project transforms raw data from PostgreSQL into analytics-ready models in BigQuery.

## Project Structure

```
dbt/
├── dbt_project.yml          # Project configuration
├── profiles.yml              # Connection profiles (not in git)
├── packages.yml               # dbt packages dependencies
├── models/
│   ├── staging/              # Staging models (views)
│   │   ├── sources.yml       # Source definitions
│   │   ├── schema.yml        # Staging tests
│   │   └── stg_*.sql         # Staging model files
│   ├── intermediate/         # Intermediate models (ephemeral)
│   ├── transform/            # Transform models (incremental)
│   │   ├── schema.yml        # Transform tests
│   │   └── fct_*.sql         # Fact tables
│   └── reporting/            # Reporting models (tables)
├── snapshots/                # SCD Type 2 snapshots
├── tests/                    # Custom data tests
├── macros/                   # Reusable macros
└── seeds/                    # Static CSV data
```

## Environment Variables

Required environment variables:
- `GCP_PROJECT_ID`: Google Cloud project ID
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account JSON key
- `DBT_TARGET`: Target environment (dev or prod)

## Commands

```bash.Make Command
make dbt-run              # Run all models
make dbt-test             # Run all tests
```

```bash
# Run dbt commands directly in container
make airflow-shell
dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod
dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod
```

## Model Layers

### Staging (stg_)
- Materialized as views
- Cleans and deduplicates raw data
- Renames columns to business-friendly names
- One-to-one mapping with source tables

### Intermediate (int_)
- Materialized as ephemeral
- Business logic transformations
- Join staging models

### Transform (fct_, dim_)
- Materialized as incremental tables
- Core business entities
- Partitioned for performance

### Reporting (rpt_)
- Materialized as tables
- End-user facing models
- Aggregated metrics

## Packages

- dbt_utils: Utility functions and tests
- dbt_expectations: Advanced data quality tests
- audit_helper: Data comparison utilities

## Testing

Run tests:
```bash
dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt
```

## Documentation

Generate docs:
```bash
dbt docs generate --project-dir /opt/dbt --profiles-dir /opt/dbt
dbt docs serve --project-dir /opt/dbt --profiles-dir /opt/dbt
```
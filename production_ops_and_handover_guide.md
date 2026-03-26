# Production Operations & Handover Guide
## SaaS Data Platform — Operations Runbook & PIC Reference

**Last Updated:** Based on Current Codebase  
**Audience:** Current PIC, Incoming Engineers, On-Call Team  
**Severity Levels:** P1 (Critical), P2 (High), P3 (Medium), P4 (Low)

---

## Table of Contents

1. [Document Purpose](#1-document-purpose)
2. [Severity Levels & SLAs](#2-severity-levels--slas)
3. [System Health Baseline](#3-system-health-baseline)
4. [Routine Operations](#4-routine-operations)
5. [Incident Response Runbooks](#5-incident-response-runbooks)
6. [Data Quality Issues](#6-data-quality-issues)
7. [Performance & Cost Optimization](#7-performance--cost-optimization)
8. [PIC Handover Checklist](#8-pic-handover-checklist)
9. [Architecture Decision Log](#9-architecture-decision-log)
10. [Emergency Contacts & Escalation](#10-emergency-contacts--escalation)

---

## 1. Document Purpose

This guide serves three primary functions:

1. **Operational Runbook**: Step-by-step procedures for diagnosing and resolving production issues
2. **Cost Management**: Guidance for keeping BigQuery costs predictable as data grows
3. **Knowledge Transfer**: Complete handover documentation for incoming engineers

### How to Use This Guide

| Situation | Go To |
|-----------|-------|
| Pipeline is failing | §5 (Incident Response) |
| Data looks wrong in dashboards | §6 (Data Quality) |
| BigQuery costs spiked | §7 (Performance) |
| Taking over as PIC | §8 (Handover Checklist) |
| Why was X built this way? | §9 (Architecture Decisions) |
| Who to call for help? | §10 (Contacts) |

---

## 2. Severity Levels & SLAs

| Level | Label | Definition | Response Time | Example |
|-------|-------|------------|---------------|---------|
| **P1** | Critical | Business reporting broken, data stale > 4h, revenue impact | Immediate | MRR dashboard showing $0 for all accounts |
| **P2** | High | Pipeline delayed beyond SLA, quality checks failing | < 2 hours | dbt tests failing in production |
| **P3** | Medium | Non-critical test failures, performance degraded | < 8 hours | Warnings on freshness checks |
| **P4** | Low | Documentation gaps, cosmetic issues | Next sprint | Typos in documentation |

### SLA Definitions

- **Ingestion**: Raw data landed within 1 hour of source
- **Transformation**: dbt models refreshed within 4 hours of ingestion
- **Reporting**: Dashboard data < 6 hours old
- **Streaming**: Kafka consumer lag < 5 minutes

---

## 3. System Health Baseline

### 3.1 Quick Health Check

Run this command first when investigating any issue:

```bash
./scripts/health_check.sh
```

**Expected Output**:
```
=== Data Platform Health Check — 2024-01-15T08:30:00Z ===

── Docker Services ──────────────────────────────────
NAME                STATUS
postgres-airflow    Up 3 days
postgres-source     Up 3 days
redis               Up 3 days
airflow-webserver   Up 3 days (healthy)
airflow-scheduler   Up 3 days (healthy)
...

── Kafka Consumer Lag ───────────────────────────────
GROUP                          TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
 data-warehouse-consumer       product.usage.events  0      1234567         1234568         1
...

── Airflow Health ───────────────────────────────────
{
    "status": "healthy",
    "metadatabase": {
        "status": "healthy"
    },
    "scheduler": {
        "status": "healthy"
    }
}

── Spark Cluster Status ─────────────────────────────
  Workers: 2
  Active apps: 0

── Data Lake (GCS) ──────────────────────────────────
Recent files in gs://your-bucket/raw/events/2024/01/15/:
  events-00001.parquet (2.3 MB, 08:15)
  events-00002.parquet (2.1 MB, 08:30)

── Data Warehouse (BigQuery) ────────────────────────
Last update to staging.stg_usage_events_daily: 2024-01-15 08:35:00 UTC
Row count: 1,234,567

=== Health Check Complete ===
```

### 3.2 Health Check Indicators

| Component | Healthy | Warning | Critical |
|-----------|---------|---------|----------|
| **Airflow Webserver** | HTTP 200 on /api/v2/monitor/health | Slow response (>5s) | HTTP error or timeout |
| **Airflow Scheduler** | HTTP 200 on /health | Heartbeat delayed | HTTP error |
| **Kafka Consumer Lag** | < 1000 messages | 1000-10000 messages | > 10000 messages or stalled |
| **GCS Data Lake** | Files < 15 min old | Files 15-60 min old | No new files > 1 hour |
| **BigQuery Staging** | Updated < 1 hour ago | Updated 1-4 hours ago | Not updated > 4 hours |
| **Spark Workers** | 2 connected | 1 connected | 0 connected |
| **Disk Usage** | < 70% | 70-85% | > 85% |
| **Memory Usage** | < 80% | 80-90% | > 90% |

### 3.3 Dashboard URLs

| Service | URL | Purpose |
|---------|-----|---------|
| Airflow UI | http://localhost:8080 | DAG monitoring, manual triggers |
| Spark UI | http://localhost:8082 | Job status, worker health |
| Kafka UI | http://localhost:8090 | Topic inspection, consumer lag |

---

## 4. Routine Operations

### 4.1 Daily Health Check (Morning)

**Time**: 09:00 local time  
**Duration**: 5 minutes  
**Owner**: On-call engineer

```bash
# 1. Run health check
./scripts/health_check.sh

# 2. Check Airflow DAG status
curl -s http://localhost:8080/api/v2/dags | jq '.dags[] | {id: .dag_id, paused: .paused}'

# 3. Check last successful run dates
curl -s http://localhost:8080/api/v2/dags/master_daily_pipeline/dagRuns?limit=1 | jq '.dag_runs[0].execution_date'

# 4. Check GCS Data Lake (recent files)
CURRENT_DATE=$(date +%Y/%m/%d)
gsutil ls -l gs://your-bucket/raw/events/$CURRENT_DATE/ | tail -5

# 5. Check BigQuery freshness
bq query --use_legacy_sql=false \
  "SELECT MAX(_loaded_at) as latest_record FROM \`project.raw.raw_usage_events\`"

# 6. Check BigQuery slot usage (if using reservations)
bq show --format=prettyjson --reservation reservation-name
```

**Checklist**:
- [ ] All Docker services running
- [ ] No critical alerts in last 24h
- [ ] Master pipeline completed successfully last night
- [ ] Consumer lag acceptable (< 1000)
- [ ] GCS Data Lake: New files created in last hour
- [ ] BigQuery Staging: Updated within SLA (< 1 hour)
- [ ] No P1/P2 incidents open

### 4.2 Weekly Maintenance (Monday)

**Time**: 10:00 local time  
**Duration**: 30 minutes  
**Owner**: PIC

```bash
# 1. Review failed tasks in past week
docker compose logs --since="7 days ago" airflow-scheduler | grep -i error

# 2. Check disk usage
df -h
docker system df

# 3. Review BigQuery costs
# Go to GCP Console > BigQuery > Monitoring

# 4. Clean up old logs if needed (>30 days)
find airflow/logs -name "*.log" -mtime +30 -delete

# 5. Check for Airflow zombie tasks
docker compose exec airflow-worker airflow tasks list zombie_tasks
```

**Checklist**:
- [ ] Review all failed tasks from past week
- [ ] Document any recurring failures
- [ ] Verify disk space healthy
- [ ] Review cost trends
- [ ] Check for stuck tasks
- [ ] Update runbook if new issues found

### 4.3 Monthly Maintenance

**Time**: First Monday of month  
**Duration**: 2 hours  
**Owner**: PIC + Team

```bash
# 1. Full system restart (during maintenance window)
make down
make up

# 2. Verify all services healthy
./scripts/health_check.sh

# 3. Update dependencies
docker compose exec airflow-worker pip list --outdated
docker compose exec spark-master pip list --outdated

# 4. Review and rotate logs
tar -czf logs-backup-$(date +%Y%m).tar.gz airflow/logs/
rm -rf airflow/logs/*

# 5. Review access permissions
gcloud projects get-iam-policy $GCP_PROJECT_ID
```

**Checklist**:
- [ ] All services restarted successfully
- [ ] Dependencies reviewed for security updates
- [ ] Logs archived and cleaned
- [ ] IAM permissions reviewed
- [ ] Documentation updated

### 4.4 Quarterly Review

**Activities**:
- Capacity planning review (data growth projections)
- Cost optimization analysis
- Security audit (keys, permissions)
- Disaster recovery test
- Architecture review (pain points, improvements)

---

## 5. Incident Response Runbooks

### 5.1 Airflow DAG Failure (P2)

**Symptoms**: Alert received, DAG shows failed state in UI

**Investigation**:
```bash
# 1. Get failed task details
docker compose logs --tail=100 airflow-scheduler | grep -A 10 "FAILED"

# 2. Check specific task logs
docker compose logs --tail=200 airflow-worker | grep "task_id=<failed_task>"

# 3. View task instance in UI
# http://localhost:8080/dags/<dag_id>/grid
```

**Common Causes & Fixes**:

| Cause | Fix |
|-------|-----|
| BigQuery API quota exceeded | Wait 10 minutes, retry; consider increasing quota |
| Network timeout | Check connectivity, retry task |
| dbt compilation error | Check SQL syntax, fix model, redeploy |
| Out of memory | Increase worker memory or reduce parallelism |
| Dependency not met | Check upstream DAG status, may need manual trigger |

**Resolution**:
```bash
# Clear the failed task and retry
docker compose exec airflow-worker airflow tasks clear <dag_id> -t <task_id> -s <start_date> -e <end_date>

# Or use UI: Click on failed task → Clear → Recursive
```

### 5.2 Airflow Scheduler Unresponsive (P1)

**Symptoms**: DAGs not triggering, UI accessible but stale data

**Investigation**:
```bash
# 1. Check scheduler process
docker compose ps airflow-scheduler

# 2. Check scheduler logs
docker compose logs --tail=50 airflow-scheduler

# 3. Check scheduler health endpoint
curl -s http://localhost:8974/health
```

**Resolution**:
```bash
# Restart scheduler only (no impact on running tasks)
docker compose restart airflow-scheduler

# Wait 2 minutes and verify
curl -s http://localhost:8974/health
```

### 5.3 Kafka Consumer Lag (P2)

**Symptoms**: Data freshness alert, consumer lag growing, GCS files not being created

**Investigation**:
```bash
# 1. Check current lag
docker compose exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe --group data-warehouse-consumer

# 2. Check Airflow DAG status (preferred - Airflow-managed consumer)
# Airflow UI → DAGs → kafka_consumer_ingestion
# Check if task is running or failed

# 3. Check consumer logs
docker compose logs --tail=100 kafka-consumer

# 4. Check GCS for recent files
# GCP Console → Storage → your-bucket → raw/events/
# Verify files are being created: YYYY/MM/DD/HH/events-*.parquet

# 5. Check GCS write errors
# Consumer logs will show GCS API errors if present
```

**Common Causes**:
- GCS API rate limits or quota exceeded
- Consumer container crashed
- Schema mismatch causing deserialization errors
- Network issues between consumer and GCS
- Airflow task marked as failed (if using Airflow-managed consumer)

**Resolution**:
```bash
# Option 1: Restart via Airflow (if Airflow-managed)
# Airflow UI → DAGs → kafka_consumer_ingestion → Clear → Recursive

# Option 2: Restart Docker service (if service-based)
docker compose restart kafka-consumer

# If data loss is acceptable, skip to latest offset:
docker compose exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group data-warehouse-consumer \
    --reset-offsets --to-latest --execute \
    --topic product.usage.events

# Monitor lag recovery
watch -n 5 'docker compose exec -T kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group data-warehouse-consumer'

# Verify GCS files are being created
gsutil ls -l gs://your-bucket/raw/events/$(date +%Y/%m/%d/)
```

### 5.4 Spark Job Failure (P2)

**Symptoms**: Spark job failed in Airflow, error in logs

**Investigation**:
```bash
# 1. Check Spark UI for failed application
open http://localhost:8082

# 2. Check worker logs
docker compose logs --tail=100 spark-worker

# 3. Check specific job logs in Spark UI
# Navigate to Completed/Failed Applications → Click application
```

**Common Causes**:
- Out of memory (OOM)
- BigQuery connection timeout
- Invalid date parameter
- GCS temp bucket permissions

**Resolution**:
```bash
# For OOM errors:
# 1. Increase worker memory in docker-compose.yml
# 2. Restart workers:
docker compose up -d --force-recreate spark-worker

# For transient errors:
# Clear and retry the Airflow task (see 5.1)

# For invalid parameters:
# Check the Spark job parameters in Airflow UI
```

### 5.5 dbt Run Failure (P2)

**Symptoms**: dbt tests failing or models not compiling

**Investigation**:
```bash
# 1. Run dbt in debug mode
docker compose exec airflow-worker \
    dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod --debug

# 2. Check specific model compilation
docker compose exec airflow-worker \
    dbt compile --project-dir /opt/dbt --profiles-dir /opt/dbt --select <model_name>

# 3. Check test failures
docker compose exec airflow-worker \
    dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt --select <model_name> --store-failures
```

**Common Issues**:
- Schema drift in source data
- Missing columns
- Referential integrity violations
- Type mismatches

**Resolution**:
```bash
# Fix model in dbt/models/ and redeploy
git add dbt/models/path/to/model.sql
git commit -m "Fix: <description>"
git push origin main

# Full refresh if schema changed
docker compose exec airflow-worker \
    dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt --full-refresh --select <model_name>
```

### 5.6 BigQuery API Quota Exceeded (P2)

**Symptoms**: 429 errors in logs, "Quota exceeded" messages

**Investigation**:
```bash
# Check BigQuery quota usage
# GCP Console > IAM & Admin > Quotas > BigQuery API
```

**Resolution**:
```bash
# Immediate fix: Wait for quota reset (usually 10-60 seconds)
# Then retry failed tasks

# Long-term fix: Request quota increase
# GCP Console > IAM & Admin > Quotas > Edit > Increase

# Alternative: Reduce parallelism in dbt
docker compose exec airflow-worker \
    dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt --threads 2
```

### 5.7 GCP Service Account Key Expiry (P1)

**Symptoms**: All GCP operations failing, auth errors

**Investigation**:
```bash
# Check if key exists
ls -la secrets/gcp-sa-key.json

# Verify key is valid
cat secrets/gcp-sa-key.json | jq '.client_email'

# Test authentication
gcloud auth activate-service-account --key-file=secrets/gcp-sa-key.json
gcloud projects list
```

**Resolution**:
```bash
# 1. Generate new key in GCP Console
# IAM & Admin > Service Accounts > <account> > Keys > Add Key > Create New Key

# 2. Download and replace
cp /path/to/new-key.json secrets/gcp-sa-key.json

# 3. Restart services to pick up new key
docker compose restart airflow-webserver airflow-scheduler airflow-worker spark-master spark-worker

# 4. Verify
gcloud auth list
```

---

## 6. Data Quality Issues

### 6.1 MRR Discrepancy (P1)

**Symptoms**: Business reports MRR doesn't match dashboard

**Investigation**:
```sql
-- 1. Check current MRR by status
SELECT 
    subscription_status,
    COUNT(*) as subscription_count,
    ROUND(SUM(mrr_amount), 2) as total_mrr
FROM `project.transform.fct_subscriptions`
WHERE dbt_valid_to IS NULL  -- Current records only
GROUP BY subscription_status;

-- 2. Compare with staging
SELECT 
    subscription_status,
    SUM(mrr_amount) as staging_mrr
FROM `project.staging.stg_subscriptions`
GROUP BY subscription_status;

-- 3. Check for negative MRR
SELECT *
FROM `project.transform.fct_subscriptions`
WHERE mrr_amount < 0
  AND dbt_valid_to IS NULL;
```

**Common Causes**:
- Currency not normalized (cents vs dollars)
- Cancelled subscriptions still counted
- Duplicates in source data
- Snapshot logic error

**Resolution**:
```bash
# 1. Identify root cause from queries above

# 2. If source data issue:
#    - Fix in source system or ETL
#    - Run historical backfill if needed

# 3. If dbt model issue:
#    - Fix model logic
#    - Full refresh: dbt run --full-refresh --select fct_subscriptions
#    - Recreate snapshots: dbt snapshot

# 4. Verify fix
#    Re-run MRR queries and compare
```

### 6.2 Duplicate Records (P2)

**Symptoms**: dbt unique test failing, row counts inflated

**Investigation**:
```sql
-- Find duplicates in staging
SELECT subscription_id, COUNT(*) as cnt
FROM `project.staging.stg_subscriptions`
GROUP BY subscription_id
HAVING COUNT(*) > 1;

-- Check source data
SELECT subscription_id, COUNT(*) as cnt
FROM `project.raw.raw_subscriptions`
GROUP BY subscription_id
HAVING COUNT(*) > 1;
```

**Resolution**:
```bash
# If source has duplicates, add deduplication in staging model
# Edit: dbt/models/staging/stg_subscriptions.sql

# Add window function:
# QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY _loaded_at DESC) = 1

# Then:
dbt run --select stg_subscriptions
dbt test --select stg_subscriptions
```

### 6.3 Schema Drift (P2)

**Symptoms**: dbt compilation errors, column not found

**Investigation**:
```bash
# Compare source schema with dbt expectations
docker compose exec airflow-worker \
    bq show --schema project:raw.raw_subscriptions

# Check against sources.yml
cat dbt/models/staging/sources.yml
```

**Resolution**:
```bash
# 1. Update sources.yml with new columns

# 2. Update staging model to handle new columns

# 3. Run dbt compile to verify
dbt compile

# 4. Deploy
dbt run --select staging
```

### 6.4 Data Freshness Alert (P2)

**Symptoms**: Airflow DAG `check_freshness` task failing

**Investigation**:
```sql
-- Check latest data timestamp
SELECT MAX(_loaded_at) as latest_data
FROM `project.raw.raw_subscriptions`;

-- Check Airflow DAG run times
# Airflow UI > Browse > DAG Runs
```

**Resolution**:
```bash
# 1. Check if ingestion DAG is running
# Airflow UI > DAGs > stripe_subscriptions_ingestion

# 2. If stuck, clear and retry
docker compose exec airflow-worker airflow tasks clear stripe_subscriptions_ingestion -s <date>

# 3. Check source data availability
# Connect to postgres-source and verify new records
```

---

## 7. Performance & Cost Optimization

### 7.1 Identifying Expensive Queries

```sql
-- Top 10 most expensive queries in last 7 days
SELECT
  job_id,
  query,
  user_email,
  total_bytes_processed / 1024 / 1024 / 1024 as gb_processed,
  total_slot_ms / 1000 / 60 as slot_minutes,
  creation_time
FROM `region-asia-southeast2`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
ORDER BY total_bytes_processed DESC
LIMIT 10;
```

### 7.2 Cost Reduction Strategies

| Strategy | Implementation | Expected Savings |
|----------|---------------|------------------|
| **Partition Pruning** | Always filter on partition columns | 50-90% |
| **Clustering** | Keep clustered columns in WHERE clause | 20-50% |
| **Materialized Views** | For frequently queried aggregations | 30-70% |
| **Query Caching** | Re-run identical queries within 24h | 100% |
| **Slot Reservations** | For predictable workloads | 20-40% |

### 7.3 BigQuery Cost Controls

```bash
# Set maximum bytes billed per query
dbt run --vars '{"max_bytes_billed": 10000000000}'  # 10 GB

# Use dry run to estimate costs
dbt compile --dry-run

# Monitor daily costs
# GCP Console > Billing > Reports
```

### 7.4 Optimizing dbt Models

**Incremental Models**:
```sql
-- Good: Filters on partition column
{{ config(
    materialized='incremental',
    partition_by={'field': 'event_date', 'data_type': 'date'}
) }}

SELECT *
FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}
```

**Avoid Full Scans**:
```sql
-- Bad: Full table scan
SELECT * FROM `project.transform.fct_subscriptions`;

-- Good: Partition pruning
SELECT *
FROM `project.transform.fct_subscriptions`
WHERE DATE(started_at) >= '2024-01-01';
```

---

## 8. PIC Handover Checklist

### 8.1 Pre-Handover (Outgoing PIC)

**Week -2**:
- [ ] Update all documentation (this guide, README, ADRs)
- [ ] Document any ongoing issues or workarounds
- [ ] Prepare architecture overview presentation
- [ ] List all external dependencies and contacts

**Week -1**:
- [ ] Pair on incident response (simulate P2 failure)
- [ ] Review cost trends and optimization opportunities
- [ ] Transfer ownership of:
  - GCP project
  - GitHub repository
  - Slack integrations
  - Monitoring alerts
- [ ] Schedule follow-up meetings for first month

### 8.2 Handover Meeting Agenda

**Day 1** (4 hours):
1. Architecture walkthrough (1h)
2. Live system demonstration (1h)
3. Common issues and runbooks (1h)
4. Cost management walkthrough (1h)

**Day 2-3** (Shadowing):
- Morning health checks
- Incident response simulation
- Deployment walkthrough
- dbt model review

**Day 4-5** (Hands-on):
- Incoming PIC runs health checks
- Practice incident response
- Review and update documentation
- Q&A sessions

### 8.3 Access Transfer Checklist

**GCP Access**:
- [ ] Grant Owner/Editor role to new PIC
- [ ] Transfer service account ownership
- [ ] Share billing account access
- [ ] Document any special IAM configurations

**GitHub**:
- [ ] Transfer repository ownership or add as admin
- [ ] Update CODEOWNERS file
- [ ] Verify GitHub Secrets access
- [ ] Update branch protection rules if needed

**Infrastructure**:
- [ ] Share Docker host SSH keys
- [ ] Document any cron jobs outside Docker
- [ ] Transfer SSL certificates
- [ ] Share VPN access if applicable

### 8.4 First Month Check-ins

**Week 1**: Daily 15-min standups
**Week 2-3**: Every other day
**Week 4**: Weekly

Topics:
- Any surprises or undocumented behavior
- Questions about architecture decisions
- Ideas for improvements
- Confidence level (1-10)

---

## 9. Architecture Decision Log

### ADR-001: Docker Compose for Local Development

**Status**: Accepted  
**Context**: Need consistent local environment for team  
**Decision**: Use Docker Compose for all services  
**Consequences**:
- (+) Reproducible environments
- (+) Easy onboarding
- (-) Higher resource requirements
- (-) Complexity in networking

### ADR-002: Airflow 3.x with CeleryExecutor

**Status**: Accepted  
**Context**: Need reliable task execution with horizontal scaling  
**Decision**: Use Airflow 3.1.8 with CeleryExecutor backed by Redis  
**Consequences**:
- (+) Distributed task execution
- (+) Better fault tolerance
- (-) More complex setup than LocalExecutor
- (-) Requires Redis and Celery monitoring

### ADR-003: dbt Incremental Models with Merge Strategy

**Status**: Accepted  
**Context**: Large fact tables, need efficient updates  
**Decision**: Use incremental materialization with merge strategy  
**Consequences**:
- (+) Faster daily runs
- (+) Lower BigQuery costs
- (-) More complex model logic
- (-) Requires careful unique key management

### ADR-004: Kafka for Real-time Events

**Status**: Accepted  
**Context**: Need real-time usage event ingestion  
**Decision**: Use Kafka with Schema Registry, custom Python consumer  
**Consequences**:
- (+) Decoupled producers/consumers
- (+) Schema evolution support
- (+) Replay capability
- (-) Operational complexity
- (-) Requires monitoring consumer lag

### ADR-005: Spark for Batch Aggregation

**Status**: Accepted  
**Context**: Need scalable batch processing for usage events  
**Decision**: Use Spark 4.0.0 standalone cluster  
**Consequences**:
- (+) Scalable processing
- (+) Rich transformation APIs
- (-) Resource intensive
- (-) Complex debugging

### ADR-006: BigQuery as Data Warehouse (with GCS Data Lake)

**Status**: Accepted  
**Context**: Need cost-effective data storage + powerful analytics  
**Decision**: 
- **GCS (Data Lake)**: Store raw data cost-effectively
- **BigQuery (Warehouse)**: Curated data for analytics only
- **Spark**: Bridge between GCS and BigQuery

**Architecture**:
```
Kafka → GCS (Raw) → Spark → BigQuery (Staging) → dbt → BigQuery (Reporting)
        (Data Lake)         (Data Warehouse)
```

**Consequences**:
- (+) **50x cost savings**: GCS storage vs BigQuery streaming inserts
- (+) Replay capability from GCS without re-extracting from source
- (+) BigQuery optimized for queries only, not storage
- (+) Clear separation of concerns
- (-) Added complexity (Spark jobs required)
- (-) Slight latency increase (batch vs streaming)

**Cost Comparison** (per 1TB/month):
| Component | Storage | Query/Processing | Total |
|-----------|---------|------------------|-------|
| BigQuery Only (streaming) | $20 | $50+ | $70+ |
| GCS + BigQuery (batch) | $2 | $5 | $7 |
| **Savings** | | | **90%** |

---

## 10. Emergency Contacts & Escalation

### 10.1 Team Contacts

| Role | Name | Slack | Phone | Primary Hours |
|------|------|-------|-------|---------------|
| **Primary PIC** | TBD | @pic | - | Business hours |
| **Secondary PIC** | TBD | @backup-pic | - | Business hours |
| **Engineering Manager** | TBD | @em | - | Business hours |
| **On-Call Engineer** | Rotating | @oncall | - | 24/7 |

### 10.2 Vendor Support

| Vendor | Support Channel | Escalation Path |
|--------|----------------|-----------------|
| **Google Cloud** | GCP Console > Support | P1: Phone hotline |
| **GitHub** | support.github.com | Enterprise support |
| **Confluent (Kafka)** | confluent.io/support | Commercial support |

### 10.3 Escalation Matrix

**Level 1: On-Call Engineer**
- Triage incident
- Attempt resolution using runbooks
- Time limit: 30 minutes (P1), 2 hours (P2)

**Level 2: Primary PIC**
- Complex issues beyond runbooks
- Architectural decisions needed
- Vendor escalation required
- Time limit: 2 hours (P1), 4 hours (P2)

**Level 3: Engineering Manager**
- Business impact decisions
- Resource allocation
- External communications
- Vendor escalation
- Time limit: 4 hours (P1)

**Level 4: Executive**
- Revenue-impacting decisions
- Public communications
- Legal/compliance issues

### 10.4 Communication Templates

**Slack Alert (P1)**:
```
🚨 P1 INCIDENT — Data Platform

Impact: [MRR dashboard down / All pipelines stopped / etc.]
Started: [timestamp]
Current Status: [investigating / identified / resolved]

Actions Taken:
- [action 1]
- [action 2]

Next Update: [+30 minutes]

@channel @oncall @pic
```

**Incident Resolution Summary**:
```
✅ RESOLVED — [Brief description]

Duration: [X minutes]
Impact: [Description]
Root Cause: [Technical explanation]
Resolution: [What fixed it]
Prevention: [Action items to prevent recurrence]

Full post-mortem: [link]
```

---

## Appendix: Quick Reference Commands

```bash
# Health check
./scripts/health_check.sh

# View service logs
make logs svc=airflow-webserver
make logs svc=spark-master
make logs svc=kafka-consumer

# Restart services
docker compose restart <service-name>

# dbt operations
docker compose exec airflow-worker dbt run --select <model>
docker compose exec airflow-worker dbt test --select <model>
docker compose exec airflow-worker dbt snapshot

# Clear Airflow tasks
docker compose exec airflow-worker airflow tasks clear <dag_id> -t <task>

# Kafka consumer lag
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group data-warehouse-consumer

# BigQuery query
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM project.raw.raw_subscriptions'
```

---

*This runbook is a living document. Update it as the system evolves and new issues are encountered.*

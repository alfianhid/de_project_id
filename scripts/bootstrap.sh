#!/bin/bash
#
# SaaS Data Platform - Bootstrap Script
# 
# This script initializes the entire data platform environment.
# Run this after cloning the repo and setting up your .env file.
#
# Usage: ./scripts/bootstrap.sh
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# =============================================================================
# Pre-flight Checks
# =============================================================================

log_info "Starting SaaS Data Platform bootstrap..."
echo ""

# Check if .env exists
if [ ! -f ".env" ]; then
    log_error ".env file not found!"
    log_info "Please copy .env.example to .env and configure your settings:"
    log_info "  cp .env.example .env"
    log_info "  vim .env"
    exit 1
fi

# Check if GCP credentials exist
if [ ! -f "secrets/gcp-sa-key.json" ]; then
    log_error "GCP service account key not found!"
    log_info "Please place your GCP service account key at:"
    log_info "  secrets/gcp-sa-key.json"
    exit 1
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    log_error "Docker is not running or not installed!"
    exit 1
fi

# Check if gcloud CLI is installed
if ! command -v gcloud &> /dev/null; then
    log_warning "gcloud CLI not found. BigQuery dataset creation will be skipped."
    log_info "To install gcloud: https://cloud.google.com/sdk/docs/install"
    SKIP_BQ_INIT=true
else
    SKIP_BQ_INIT=false
fi

# =============================================================================
# Step 1: Build Docker Images
# =============================================================================

echo ""
log_info "Step 1/6: Building Docker images..."
make build
log_success "Docker images built successfully"

# =============================================================================
# Step 2: Start Services
# =============================================================================

echo ""
log_info "Step 2/6: Starting services..."
make up
log_success "Services started"

# Wait for services to be healthy
echo ""
log_info "Waiting for services to be healthy (this may take 2-3 minutes)..."
sleep 30

# Check Airflow health
for i in {1..30}; do
    if curl -s http://localhost:8080/api/v2/monitor/health > /dev/null 2>&1; then
        log_success "Airflow is healthy"
        break
    fi
    if [ $i -eq 30 ]; then
        log_warning "Airflow health check timed out, but continuing..."
    else
        echo -n "."
        sleep 5
    fi
done

# =============================================================================
# Step 3: Initialize BigQuery Datasets
# =============================================================================

echo ""
if [ "$SKIP_BQ_INIT" = false ]; then
    log_info "Step 3/6: Initializing BigQuery datasets..."
    if ./scripts/init_bigquery_datasets.sh; then
        log_success "BigQuery datasets initialized"
    else
        log_warning "BigQuery dataset initialization had issues, but continuing..."
    fi
else
    log_warning "Step 3/6: Skipping BigQuery initialization (gcloud not available)"
fi

# =============================================================================
# Step 4: Load Seed Data
# =============================================================================

echo ""
log_info "Step 4/6: Loading dbt seed data..."
sleep 10  # Wait for Airflow worker to be ready

# Retry logic for dbt seed
for i in {1..3}; do
    if docker compose exec -T airflow-worker \
        dbt seed --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod; then
        log_success "Seed data loaded"
        break
    else
        if [ $i -eq 3 ]; then
            log_warning "dbt seed failed after 3 attempts, but continuing..."
        else
            log_warning "dbt seed attempt $i failed, retrying in 10 seconds..."
            sleep 10
        fi
    fi
done

# =============================================================================
# Step 5: Create Kafka Topics
# =============================================================================

echo ""
log_info "Step 5/6: Creating Kafka topics..."
sleep 5

# Wait for Kafka to be ready
for i in {1..30}; do
    if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        break
    fi
    if [ $i -eq 30 ]; then
        log_warning "Kafka not ready, skipping topic creation..."
        break
    fi
    echo -n "."
    sleep 2
done

if [ $i -lt 30 ]; then
    if make create-topics; then
        log_success "Kafka topics created"
    else
        log_warning "Kafka topic creation had issues, but continuing..."
    fi
fi

# =============================================================================
# Step 6: Verify Setup
# =============================================================================

echo ""
log_info "Step 6/6: Running health checks..."
./scripts/health_check.sh

# =============================================================================
# Bootstrap Complete
# =============================================================================

echo ""
echo "═══════════════════════════════════════════════════════════════"
log_success "Bootstrap complete! 🎉"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Your SaaS Data Platform is now running:"
echo ""
echo "  Airflow UI:  http://localhost:8080"
echo "  Spark UI:    http://localhost:8082"
echo "  Kafka UI:    http://localhost:8090"
echo ""
echo "Default credentials:"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "Next steps:"
echo "  1. Access Airflow UI and enable the example DAGs"
echo "  2. Run 'make dbt-run' to build all dbt models"
echo "  3. Check the documentation in the README.md"
echo ""
echo "Useful commands:"
echo "  make logs svc=airflow-webserver  - View Airflow logs"
echo "  make dbt-run                     - Run all dbt models"
echo "  make dbt-test                    - Run dbt tests"
echo "  make down                        - Stop all services"
echo ""

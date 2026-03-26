# ==================== Docker Build Optimization ====================
# Build with BuildKit for faster builds:
#   DOCKER_BUILDKIT=1 docker compose build
# 
# Build specific service:
#   make build-airflow  # Build only Airflow
#   make build-spark    # Build only Spark
# 
# Fresh build (no cache):
#   make build-no-cache
# 
# Prune build cache periodically:
#   make prune-cache
# 
# Build with parallel jobs (faster):
#   DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 docker compose build --parallel
# 
# ====================================================================

.PHONY: help build build-no-cache build-airflow build-spark up down logs ps init-bq dbt-seed dbt-run dbt-test create-topics prune-cache prune-cache

help:
	@grep -E'^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	    | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all custom Docker images
	DOCKER_BUILDKIT=1 docker compose build

build-no-cache: ## Build images without cache (fresh build)
	DOCKER_BUILDKIT=1 docker compose build --no-cache

build-airflow: ## Build only Airflow image
	DOCKER_BUILDKIT=1 docker compose build airflow-webserver

build-spark: ## Build only Spark image
	DOCKER_BUILDKIT=1 docker compose build spark-master

prune-cache: ## Prune Docker build cache to free space
	docker builder prune -f

up: ## Start the full stack
	docker compose up -d --force-recreate --remove-orphans --always-recreate-deps
	@echo "Airflow:  http://localhost:8080"
	@echo "Spark UI: http://localhost:8082"
	@echo "Kafka UI: http://localhost:8090"

down: ## Stop all services
	docker compose down

logs: ## Tail logs. Usage: make logs svc=airflow-worker
	docker compose logs -f $(svc)

ps: ## Show running containers
	docker compose ps

init-bq: ## Initialize BigQuery datasets
	./scripts/init_bigquery_datasets.sh

dbt-seed: ## Load seed data into BigQuery
	docker compose exec airflow-worker \dbt seed --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod

dbt-run: ## Run all dbt models (prod)
	docker compose exec airflow-worker \dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod

dbt-test: ## Run dbt tests
	docker compose exec airflow-worker \dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod

create-topics: ## Create required Kafka topics
	docker compose exec kafka bash -c "\
	    kafka-topics --bootstrap-server localhost:9092 \
	        --create --if-not-exists \
	        --topic product.usage.events \
	        --partitions 6 --replication-factor 1 && \
	    kafka-topics --bootstrap-server localhost:9092 \
	        --create --if-not-exists \
	        --topic billing.subscription.events \
	        --partitions 3 --replication-factor 1"

airflow-shell: ## Open shell inside Airflow worker
	docker compose exec airflow-worker bash

spark-shell: ## Open PySpark shell
	docker compose exec spark-master \
	    /usr/local/bin/pyspark --master spark://spark-master:7077

kafka-topics: ## List Kafka topics
	docker compose exec kafka \
	    kafka-topics --bootstrap-server localhost:9092 --list

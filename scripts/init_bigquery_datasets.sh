#!/bin/bash
set -euo pipefail
source .env

for dataset in raw staging transform reporting; do
    echo "Creating: ${GCP_PROJECT_ID}.${dataset}"
    bq --location=$GCP_REGION mk \
        --dataset \
        "${GCP_PROJECT_ID}:${dataset}" 2>/dev/null || echo "  Already exists, skipping."
done

# Set retention: 90 days on raw, unlimited on transform/reporting
bq update --default_partition_expiration=7776000 "${GCP_PROJECT_ID}:raw"
bq update --default_partition_expiration=0 "${GCP_PROJECT_ID}:transform"
bq update --default_partition_expiration=0 "${GCP_PROJECT_ID}:reporting"

echo "BigQuery datasets initialized."

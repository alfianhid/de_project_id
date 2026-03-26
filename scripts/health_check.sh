#!/bin/bash

set -euo pipefail
echo "=== Data Platform Health Check — $(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="

echo ""
echo "── Docker Services ──────────────────────────────────"
docker compose ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "── Kafka Consumer Lag ───────────────────────────────"
result=$(docker compose exec -T kafka \
    kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group data-warehouse-consumer 2>&1)
if echo "$result" | grep -q "does not exist"; then
    echo "  Consumer group 'data-warehouse-consumer' not found (may not have started yet)"
else
    echo "$result"
fi

echo ""
echo "── Airflow Health ───────────────────────────────────"
curl -s http://localhost:8080/api/v2/monitor/health | python3 -m json.tool

echo ""
echo "── Spark Cluster Status ─────────────────────────────"
curl -s http://localhost:8082/json/ | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'  Workers: {len(d[\"workers\"])}')
print(f'  Active apps: {len(d[\"activeapps\"])}')
"

echo ""
echo "=== Health Check Complete ==="
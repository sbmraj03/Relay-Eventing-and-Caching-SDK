#!/usr/bin/env bash
# lag-check.sh — runbook: inspect consumer group lag and throughput.
#
# Usage:
#   ./scripts/lag-check.sh
#   GROUP=my-group TOPIC=my-topic ./scripts/lag-check.sh
#
# Output:
#   1. Kafka consumer group lag per partition (via kafka-consumer-groups CLI).
#   2. Prometheus message throughput counters (requires stack running).

set -euo pipefail

GROUP="${GROUP:-notification-consumer}"
TOPIC="${TOPIC:-orders}"
PROMETHEUS="${PROMETHEUS:-http://localhost:9093}"

hr() { printf '%0.s─' {1..60}; echo; }

# ── 1. Kafka lag via CLI ──────────────────────────────────────────────────────
echo ""
echo "Consumer Group Lag — ${GROUP}"
hr

if docker compose ps kafka 2>/dev/null | grep -q "Up"; then
  docker compose exec -T kafka \
    kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group "${GROUP}" \
    --describe 2>&1 || echo "(No committed offsets yet — consumer may not have started)"
else
  echo "⚠  Kafka container is not running. Start the stack with: docker compose up -d"
fi

# ── 2. Prometheus counters ────────────────────────────────────────────────────
echo ""
echo "Prometheus Metrics (last 5m)"
hr

prom() {
  local label="$1" expr="$2"
  local val
  val=$(curl -sf "${PROMETHEUS}/api/v1/query" \
    --data-urlencode "query=${expr}" \
    | python3 -c "
import json, sys
d = json.load(sys.stdin)
r = d.get('data', {}).get('result', [])
if not r:
    print('no data')
else:
    for item in r:
        labels = ','.join(f\"{k}={v}\" for k,v in item['metric'].items() if k != '__name__')
        print(f\"  {labels}: {item['value'][1]}\")
" 2>/dev/null) || val="  (Prometheus unreachable — is the stack running?)"
  echo "${label}:"
  echo "${val}"
  echo ""
}

prom "Messages processed (total, by group+status)" \
     "sum by (group, status) (relay_consumer_messages_total)"

prom "Message rate last 5m (msg/s)" \
     "sum by (group, status) (rate(relay_consumer_messages_total[5m]))"

prom "DLQ routing total (by group)" \
     "sum by (group) (relay_consumer_dlq_total)"

prom "Cache hit ratio (5m window)" \
     "sum(rate(relay_cache_operations_total{op=\"hit\"}[5m])) / (sum(rate(relay_cache_operations_total{op=\"hit\"}[5m])) + sum(rate(relay_cache_operations_total{op=\"miss\"}[5m])))"

# ── 3. Quick health summary ───────────────────────────────────────────────────
echo ""
echo "Container Health"
hr
docker compose ps 2>/dev/null || echo "(docker compose not available)"

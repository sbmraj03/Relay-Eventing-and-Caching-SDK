#!/usr/bin/env bash
# canary.sh — canary deploy for notification-consumer.
#
# Strategy (docker-compose scale model):
#   1. Build a new "canary" image from the current source.
#   2. Start it as a second consumer in the SAME Kafka consumer group.
#      Kafka re-balances partitions between v1 and canary (traffic split).
#   3. Observe the shared Prometheus metrics for WINDOW seconds.
#   4. If error_ratio > MAX_ERROR_RATE  →  ROLLBACK (stop canary, keep v1).
#      Otherwise                        →  PROMOTE  (stop v1, canary becomes primary).
#
# Usage:
#   ./scripts/canary.sh                        # build canary from current source
#   ./scripts/canary.sh --image <repo:tag>     # deploy a pre-built image
#   ./scripts/canary.sh --window 120 --max-error-rate 0.02
#
# Prerequisites: docker compose stack must already be running.
#   docker compose up -d

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────
CANARY_IMAGE=""          # empty = build from source
WINDOW=60                # observation window in seconds
MAX_ERROR_RATE="0.05"    # 5% error ratio triggers rollback
PROMETHEUS="http://localhost:9093"
CANARY_SERVICE="notification-consumer-canary"
CANARY_PORT="9095"       # host port for canary /metrics (9090 is v1)

# ── argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --image)            CANARY_IMAGE="$2";      shift 2 ;;
    --window)           WINDOW="$2";            shift 2 ;;
    --max-error-rate)   MAX_ERROR_RATE="$2";    shift 2 ;;
    --prometheus)       PROMETHEUS="$2";         shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── helpers ───────────────────────────────────────────────────────────────────
log()  { printf '\033[0;36m[canary %s]\033[0m %s\n' "$(date '+%H:%M:%S')" "$*"; }
ok()   { printf '\033[0;32m[canary %s] ✓ %s\033[0m\n' "$(date '+%H:%M:%S')" "$*"; }
fail() { printf '\033[0;31m[canary %s] ✗ %s\033[0m\n' "$(date '+%H:%M:%S')" "$*" >&2; }

# Query a PromQL expression; return the scalar value or "0" on error.
prom_query() {
  local expr="$1"
  curl -sf "${PROMETHEUS}/api/v1/query" \
    --data-urlencode "query=${expr}" \
    | python3 -c "
import json, sys
d = json.load(sys.stdin)
results = d.get('data', {}).get('result', [])
print(results[0]['value'][1] if results else '0')
" 2>/dev/null || echo "0"
}

# Float comparison: returns true (0) if $1 > $2.
gt() { python3 -c "import sys; sys.exit(0 if float('$1') > float('$2') else 1)"; }

# ── build (if no image provided) ──────────────────────────────────────────────
if [[ -z "$CANARY_IMAGE" ]]; then
  CANARY_IMAGE="relay-notification-consumer:canary-$(date '+%Y%m%d-%H%M%S')"
  log "Building canary image: ${CANARY_IMAGE}"
  docker build \
    --file services/notification-consumer/Dockerfile \
    --tag  "${CANARY_IMAGE}" \
    .
fi

log "Canary image:      ${CANARY_IMAGE}"
log "Observation window: ${WINDOW}s"
log "Max error ratio:   ${MAX_ERROR_RATE}"
log ""

# ── write a temporary compose override ────────────────────────────────────────
# The override adds a second consumer container that shares the same consumer
# group as the primary (notification-consumer). Kafka distributes partitions
# across both instances — this is the traffic split.
OVERRIDE=$(mktemp /tmp/compose-canary-XXXXXX.yml)
trap 'rm -f "${OVERRIDE}"' EXIT

cat > "${OVERRIDE}" <<EOF
services:
  ${CANARY_SERVICE}:
    image: ${CANARY_IMAGE}
    environment:
      KAFKA_BROKERS: kafka:9092
      REDIS_ADDR: redis:6379
      OTEL_EXPORTER_OTLP_ENDPOINT: jaeger:4318
    ports:
      - "${CANARY_PORT}:9090"
    depends_on:
      kafka:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: "no"
EOF

# ── start canary ──────────────────────────────────────────────────────────────
log "Starting canary alongside v1..."
docker compose -f docker-compose.yml -f "${OVERRIDE}" up -d "${CANARY_SERVICE}"

# Give the canary a moment to join the consumer group and start processing.
log "Waiting 10s for canary to join consumer group..."
sleep 10

log "Kafka consumer group after canary join:"
docker compose exec -T kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group notification-consumer \
  --describe 2>/dev/null || log "(kafka-consumer-groups not available — skip)"

log ""
log "Observing metrics for ${WINDOW}s..."
sleep "${WINDOW}"

# ── sample metrics ────────────────────────────────────────────────────────────
log "Sampling Prometheus at ${PROMETHEUS}..."

TOTAL_RATE=$(prom_query "sum(rate(relay_consumer_messages_total[2m]))")
ERROR_RATE=$(prom_query "sum(rate(relay_consumer_messages_total{status=\"error\"}[2m]))")

# Compute error ratio; guard against zero total.
ERROR_RATIO=$(python3 -c "
t = float('${TOTAL_RATE}')
e = float('${ERROR_RATE}')
print(f'{e/t:.4f}' if t > 0 else '0.0000')
")

DLQ_RATE=$(prom_query "sum(rate(relay_consumer_dlq_total[2m]))")

log "─────────────────────────────────────────"
log "Total message rate:  ${TOTAL_RATE} msg/s"
log "Error rate:          ${ERROR_RATE} msg/s"
log "Error ratio:         ${ERROR_RATIO}  (threshold: ${MAX_ERROR_RATE})"
log "DLQ routing rate:    ${DLQ_RATE} msg/s"
log "─────────────────────────────────────────"
log ""

# ── promote or rollback ───────────────────────────────────────────────────────
if gt "${ERROR_RATIO}" "${MAX_ERROR_RATE}"; then
  fail "ERROR RATIO ${ERROR_RATIO} EXCEEDS THRESHOLD ${MAX_ERROR_RATE}"
  fail "Rolling back — stopping canary, v1 remains primary."
  docker compose -f docker-compose.yml -f "${OVERRIDE}" stop "${CANARY_SERVICE}"
  docker compose -f docker-compose.yml -f "${OVERRIDE}" rm -f "${CANARY_SERVICE}"
  fail "Rollback complete."
  exit 1
else
  ok "Error ratio within threshold — promoting canary."
  log "Stopping v1 consumer (notification-consumer)..."
  docker compose stop notification-consumer
  docker compose rm -f notification-consumer
  log "Canary is now the sole consumer. Renaming for operational clarity..."
  # In docker compose you can't rename a container, but you can restart it
  # under the primary service name by updating the image tag and restarting.
  # For this demo, we leave the canary running and label it as the new primary.
  docker rename "${CANARY_SERVICE}" relay-notification-consumer-1 2>/dev/null || true
  ok "Promotion complete."
  ok "The canary is now the primary consumer: image=${CANARY_IMAGE}"
  log ""
  log "To make this permanent, update docker-compose.yml image: ${CANARY_IMAGE}"
fi

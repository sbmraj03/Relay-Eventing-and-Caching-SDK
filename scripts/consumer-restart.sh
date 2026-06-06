#!/usr/bin/env bash
# consumer-restart.sh — runbook: gracefully restart the notification-consumer.
#
# When to use:
#   - Consumer is stuck / not making progress on consumer group lag.
#   - Memory leak or goroutine leak suspected (pre-fix mitigation).
#   - Config change that requires a restart.
#
# This script does a graceful stop (SIGTERM → drain → stop) rather than
# docker compose restart, which sends SIGKILL immediately. The consumer's
# Run() method returns cleanly on context cancellation, committing any
# in-flight offsets before exiting.
#
# Usage:
#   ./scripts/consumer-restart.sh
#   SERVICE=notification-consumer DRAIN_TIMEOUT=30 ./scripts/consumer-restart.sh

set -euo pipefail

SERVICE="${SERVICE:-notification-consumer}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-15}"   # seconds to wait for graceful shutdown
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-60}"

hr() { printf '%0.s─' {1..60}; echo; }

echo ""
echo "Consumer Restart: ${SERVICE}"
hr

# ── pre-restart state ─────────────────────────────────────────────────────────
echo "Before restart:"
docker compose ps "${SERVICE}" 2>/dev/null || true
echo ""

# ── show current lag (for comparison after restart) ──────────────────────────
echo "Consumer group lag before restart:"
docker compose exec -T kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group notification-consumer \
  --describe 2>/dev/null || echo "  (could not fetch lag)"
echo ""

# ── graceful stop ─────────────────────────────────────────────────────────────
echo "Sending SIGTERM to ${SERVICE} (drain timeout: ${DRAIN_TIMEOUT}s)..."
# docker compose stop sends SIGTERM then waits up to --timeout before SIGKILL.
docker compose stop --timeout "${DRAIN_TIMEOUT}" "${SERVICE}"
echo "✓ ${SERVICE} stopped"
echo ""

# ── start ────────────────────────────────────────────────────────────────────
echo "Starting ${SERVICE}..."
docker compose start "${SERVICE}"

# ── wait for healthy ──────────────────────────────────────────────────────────
echo "Waiting for ${SERVICE} to be healthy (timeout: ${STARTUP_TIMEOUT}s)..."
elapsed=0
while (( elapsed < STARTUP_TIMEOUT )); do
  status=$(docker compose ps "${SERVICE}" --format json 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0].get('State',''))" 2>/dev/null \
    || docker compose ps "${SERVICE}" 2>/dev/null | tail -1 | awk '{print $NF}')

  if echo "$status" | grep -q "running\|Up"; then
    echo "✓ ${SERVICE} is running (elapsed: ${elapsed}s)"
    break
  fi
  sleep 2
  (( elapsed += 2 ))
done

if (( elapsed >= STARTUP_TIMEOUT )); then
  echo "✗ ${SERVICE} did not come up within ${STARTUP_TIMEOUT}s" >&2
  docker compose logs --tail=20 "${SERVICE}"
  exit 1
fi

# ── post-restart check ────────────────────────────────────────────────────────
echo ""
echo "Last 10 log lines:"
hr
docker compose logs --tail=10 "${SERVICE}"
hr
echo ""
echo "Post-restart consumer group lag:"
docker compose exec -T kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group notification-consumer \
  --describe 2>/dev/null || echo "  (could not fetch lag)"

echo ""
echo "✓ Restart complete."

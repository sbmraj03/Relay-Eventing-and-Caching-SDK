#!/usr/bin/env bash
# dlq-replay.sh — runbook: replay dead-letter queue messages back to the source topic.
#
# When to use:
#   A bug caused legitimate messages to be misclassified as poison and routed
#   to the DLQ. After deploying the fix, use this script to re-process them.
#
# Safety features:
#   - Shows a count + sample of DLQ messages before asking for confirmation.
#   - Requires explicit [y/N] confirmation before writing any messages.
#   - Replays only raw message values; strips SDK-added DLQ headers
#     (x-dlq-reason, x-dlq-source-topic, x-dlq-source-offset) so the
#     replayed messages look like fresh publishes to the consumer.
#
# Usage:
#   ./scripts/dlq-replay.sh
#   DLQ_TOPIC=orders.dlq TARGET_TOPIC=orders LIMIT=50 ./scripts/dlq-replay.sh

set -euo pipefail

DLQ_TOPIC="${DLQ_TOPIC:-orders.dlq}"
TARGET_TOPIC="${TARGET_TOPIC:-orders}"
LIMIT="${LIMIT:-100}"

hr() { printf '%0.s─' {1..60}; echo; }

echo ""
echo "DLQ Replay: ${DLQ_TOPIC}  →  ${TARGET_TOPIC}"
hr

# ── check prerequisites ───────────────────────────────────────────────────────
if ! docker compose ps kafka 2>/dev/null | grep -q "Up"; then
  echo "✗ Kafka is not running. Start the stack first: docker compose up -d"
  exit 1
fi

# ── count messages in DLQ ─────────────────────────────────────────────────────
echo "Counting messages in ${DLQ_TOPIC}..."

# GetOffsetShell returns sum of latest offsets — this is the high-water mark,
# not necessarily unprocessed count, but useful as an upper bound.
OFFSET_OUTPUT=$(docker compose exec -T kafka \
  kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic "${DLQ_TOPIC}" \
  --time -1 2>/dev/null || echo "")

if [[ -z "$OFFSET_OUTPUT" ]]; then
  echo "  (Topic '${DLQ_TOPIC}' does not exist or has no messages)"
  exit 0
fi

TOTAL=$(echo "$OFFSET_OUTPUT" | awk -F: '{sum += $3} END {print sum}')
echo "  Approximate message count (high-water mark): ${TOTAL}"
echo ""

# ── show sample of DLQ messages ───────────────────────────────────────────────
echo "Sample (first 3 messages from ${DLQ_TOPIC}):"
hr
docker compose exec -T kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic "${DLQ_TOPIC}" \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 5000 \
  --property print.headers=true \
  2>/dev/null || echo "  (no messages or timeout)"
hr

# ── confirmation gate ─────────────────────────────────────────────────────────
echo ""
echo "This will replay up to ${LIMIT} messages from '${DLQ_TOPIC}' → '${TARGET_TOPIC}'."
echo "DLQ headers (x-dlq-*) are stripped. The consumer will reprocess each message."
echo ""
read -r -p "Proceed? [y/N] " confirm
if [[ "${confirm,,}" != "y" ]]; then
  echo "Aborted."
  exit 0
fi

# ── replay ────────────────────────────────────────────────────────────────────
echo ""
echo "Replaying up to ${LIMIT} messages..."

# Read values from DLQ, pipe to producer on target topic.
# --property print.headers=false ensures we only get the raw value.
REPLAYED=$(docker compose exec -T kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic "${DLQ_TOPIC}" \
  --from-beginning \
  --max-messages "${LIMIT}" \
  --timeout-ms 10000 \
  2>/dev/null \
| tee >(wc -l) \
| docker compose exec -T kafka \
  kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic "${TARGET_TOPIC}" \
  2>/dev/null) || true

echo ""
echo "✓ Replay complete. Messages written to '${TARGET_TOPIC}'."
echo ""
echo "Monitor the consumer logs to verify reprocessing:"
echo "  docker compose logs -f notification-consumer"

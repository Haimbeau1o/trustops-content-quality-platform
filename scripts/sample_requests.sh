#!/usr/bin/env bash

set -euo pipefail

GATEWAY_BASE_URL="${GATEWAY_BASE_URL:-http://127.0.0.1:8080}"
COPILOT_BASE_URL="${COPILOT_BASE_URL:-http://127.0.0.1:8090}"
GATEWAY_API_KEY="${GATEWAY_API_KEY:-content-dev-key}"

echo "[1/5] healthz"
curl -sS "${GATEWAY_BASE_URL}/healthz" | jq .

echo "[2/5] ingest event"
curl -sS -X POST "${GATEWAY_BASE_URL}/api/v1/content/events/ingest" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ${GATEWAY_API_KEY}" \
  -d '{
    "event_id": "evt-sample-001",
    "content_id": "content-sample-001",
    "content_type": "video",
    "risk_signals": ["spam", "unsafe"],
    "evidence": [{"type":"rule_hit","detail":"rule:unsafe_keyword"}]
  }' | jq .

echo "[3/5] query ingested case"
curl -sS "${GATEWAY_BASE_URL}/api/v1/content/cases/case-evt-sample-001" \
  -H "X-API-Key: ${GATEWAY_API_KEY}" | jq .

echo "[4/5] ops metrics"
curl -sS "${GATEWAY_BASE_URL}/api/v1/ops/metrics" \
  -H "X-API-Key: ${GATEWAY_API_KEY}" | jq .

echo "[5/5] copilot summary"
curl -sS -X POST "${COPILOT_BASE_URL}/copilot/content/summary" \
  -H "Content-Type: application/json" \
  -d '{
    "case_id": "case-evt-sample-001",
    "title": "Potential spam upload with unsafe keyword hit",
    "content_id": "content-sample-001",
    "risk_signals": ["spam", "unsafe"],
    "evidence": [{"type":"rule_hit","detail":"rule:unsafe_keyword"}]
  }' | jq .

#!/usr/bin/env bash
set -euo pipefail

echo "== verify: ask2 legacy body =="
LEGACY=$(mktemp)
APEX=$(mktemp)
trap 'rm -f "$LEGACY" "$APEX"' EXIT

curl --connect-timeout 5 --max-time 15 -sS http://localhost:8080/ask2 \
  -H 'Content-Type: application/json' \
  -d '{"question":"Is Microsoft in the TECH100 Index?","top_k":6}' \
  >"$LEGACY" || true

echo "== verify: ask2 apex body =="
curl --connect-timeout 5 --max-time 15 -sS http://localhost:8080/ask2 \
  -H 'Content-Type: application/json' \
  -d '{"q":"Is Microsoft in the TECH100 Index?","top_k":6,"refine":"off"}' \
  >"$APEX" || true

for name in LEGACY APEX; do
  FILE=${!name}
  echo "== payload: $name =="
  jq '.' "$FILE" 2>/dev/null || cat "$FILE"
  ROUTE=$(jq -r '.meta.routing // "unknown"' "$FILE" 2>/dev/null || echo unknown)
  CTX_COUNT=$(jq -r '.contexts | length' "$FILE" 2>/dev/null || echo 0)
  ANSWER=$(jq -r '.answer // ""' "$FILE" 2>/dev/null || echo "")
  echo "routing=$ROUTE contexts=$CTX_COUNT"
  if [ "$CTX_COUNT" -eq 0 ]; then
    echo "❌ FAIL (no contexts in $name)"
    exit 1
  fi
  if grep -qi 'Sources:' <<<"$ANSWER"; then
    echo "❌ FAIL (inline Sources in $name)"
    exit 1
  fi
  if grep -qi 'sources:' <<<"$ANSWER"; then
    echo "❌ FAIL (inline sources in $name)"
    exit 1
  fi
  echo "--"
  jq -r '.meta.routing // "unknown"' "$FILE" 2>/dev/null || true
  echo "--"
  jq -r '.meta.contexts_note // empty' "$FILE" 2>/dev/null || true
  echo "== end payload: $name =="
done

echo "✅ PASS"

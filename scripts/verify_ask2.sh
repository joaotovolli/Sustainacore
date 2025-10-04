#!/usr/bin/env bash
set -euo pipefail

SERVICE="${SERVICE:-sustainacore-ai}"
LEG="$(mktemp)"
APX="$(mktemp)"
trap 'rm -f "$LEG" "$APX"' EXIT

curl --max-time 15 --connect-timeout 5 -sS http://localhost:8080/ask2 \
  -H 'Content-Type: application/json' \
  -d '{"question":"Is Microsoft in the TECH100 Index?","top_k":6}' > "$LEG" || true

curl --max-time 15 --connect-timeout 5 -sS http://localhost:8080/ask2 \
  -H 'Content-Type: application/json' \
  -d '{"q":"Is Microsoft in the TECH100 Index?","top_k":6,"refine":"off"}' > "$APX" || true

jq '.answer,.contexts,.meta' "$LEG" 2>/dev/null || cat "$LEG"
jq '.answer,.contexts,.meta' "$APX" 2>/dev/null || cat "$APX"

INLINE_LEG=$(jq -r '.answer // ""' "$LEG" 2>/dev/null | grep -c 'Sources:' || true)
INLINE_APX=$(jq -r '.answer // ""' "$APX" 2>/dev/null | grep -c 'Sources:' || true)
HAS_CTX_LEG=$(jq -e 'has("contexts")' "$LEG" >/dev/null 2>&1 && echo 1 || echo 0)
HAS_CTX_APX=$(jq -e 'has("contexts")' "$APX" >/dev/null 2>&1 && echo 1 || echo 0)
CTX_LEN_LEG=$(jq '(.contexts // []) | length' "$LEG" 2>/dev/null || echo 0)
CTX_LEN_APX=$(jq '(.contexts // []) | length' "$APX" 2>/dev/null || echo 0)
ROUTE_LEG=$(jq -r '.meta.routing // "unknown"' "$LEG" 2>/dev/null || echo unknown)
ROUTE_APX=$(jq -r '.meta.routing // "unknown"' "$APX" 2>/dev/null || echo unknown)
GEM_LOGS=$(journalctl -u "$SERVICE" --since '2 minutes ago' --no-pager | egrep -c 'gemini=ok|gemini=fail' || true)

echo "inline_legacy=$INLINE_LEG inline_apex=$INLINE_APX ctx_legacy=$HAS_CTX_LEG ctx_apex=$HAS_CTX_APX ctx_len_legacy=$CTX_LEN_LEG ctx_len_apex=$CTX_LEN_APX route_legacy=$ROUTE_LEG route_apex=$ROUTE_APX gemini_logs=$GEM_LOGS"

if [ "$HAS_CTX_LEG" -eq 1 ] && [ "$HAS_CTX_APX" -eq 1 ] \
   && [ "$CTX_LEN_LEG" -gt 0 ] && [ "$CTX_LEN_APX" -gt 0 ] \
   && ! { [ "$INLINE_LEG" -gt 0 ] && [ "$INLINE_APX" -gt 0 ]; }; then
  echo "✅ PASS"
  exit 0
fi

echo "❌ FAIL"
exit 1

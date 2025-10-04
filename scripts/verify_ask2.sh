#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-localhost}"
PORT="${PORT:-8080}"

legacy_payload=$(mktemp)
apex_payload=$(mktemp)
trap 'rm -f "$legacy_payload" "$apex_payload"' EXIT

echo "== Legacy body =="
curl -sS "http://${HOST}:${PORT}/ask2" \
  -H 'Content-Type: application/json' \
  -d '{"question":"Is Microsoft in the TECH100 Index?","top_k":6}' | tee "$legacy_payload"
legacy_answer=$(jq -r '.answer // ""' "$legacy_payload")
legacy_contexts=$(jq -c '.contexts' "$legacy_payload" 2>/dev/null || echo 'null')
legacy_meta=$(jq -c '.meta' "$legacy_payload" 2>/dev/null || echo 'null')

echo "== APEX body =="
curl -sS "http://${HOST}:${PORT}/ask2" \
  -H 'Content-Type: application/json' \
  -d '{"q":"Is Microsoft in the TECH100 Index?","top_k":6,"refine":"off"}' | tee "$apex_payload"
apex_answer=$(jq -r '.answer // ""' "$apex_payload")
apex_contexts=$(jq -c '.contexts' "$apex_payload" 2>/dev/null || echo 'null')
apex_meta=$(jq -c '.meta' "$apex_payload" 2>/dev/null || echo 'null')

for ctx in "$legacy_contexts" "$apex_contexts"; do
  if [[ "$ctx" == "null" || "$ctx" == "" ]]; then
    echo "Contexts missing in response" >&2
    exit 1
  fi
  if [[ "$ctx" == "[]" ]]; then
    echo "Contexts empty in response" >&2
    exit 1
  fi
done

legacy_has_sources=$(grep -c 'Sources:' <<<"$legacy_answer" || true)
apex_has_sources=$(grep -c 'Sources:' <<<"$apex_answer" || true)

if [[ $legacy_has_sources -gt 0 && $apex_has_sources -gt 0 ]]; then
  echo "Both responses contain inline Sources:, expected pipeline to strip them" >&2
  exit 1
fi

echo "== Check logs (needs journald on host) =="
if ! journalctl -u sustainacore-ai --since '2 minutes ago' --no-pager | egrep 'gemini=ok|gemini=fail'; then
  legacy_route=$(jq -r '.meta.routing // ""' "$legacy_payload")
  apex_route=$(jq -r '.meta.routing // ""' "$apex_payload")
  if [[ -z "$legacy_route" && -z "$apex_route" ]]; then
    echo "No journald entries and meta.routing missing" >&2
    exit 1
  fi
fi

echo "Verification complete"

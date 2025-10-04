#!/usr/bin/env bash
set -euo pipefail
HOST="${HOST:-localhost}"
PORT="${PORT:-8080}"

echo "== Legacy body =="
curl -sS "http://${HOST}:${PORT}/ask2" -H 'Content-Type: application/json' \
  -d '{"question":"Is Microsoft in the TECH100 Index?","top_k":6}' | jq '.answer,.contexts,.meta'

echo "== APEX body =="
curl -sS "http://${HOST}:${PORT}/ask2" -H 'Content-Type: application/json' \
  -d '{"q":"Is Microsoft in the TECH100 Index?","top_k":6,"refine":"off"}' | jq '.answer,.contexts,.meta'

echo "== Check logs (needs journald on host) =="
journalctl -u sustainacore-ai --since '2 minutes ago' --no-pager | egrep 'gemini=ok|gemini=fail' || true


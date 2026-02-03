#!/usr/bin/env bash
set -euo pipefail

URL="${1:-}"
TIMEOUT_SECONDS="${2:-60}"
INTERVAL_SECONDS="${3:-2}"

if [[ -z "$URL" ]]; then
  echo "Usage: $0 <url> [timeout_seconds] [interval_seconds]" >&2
  exit 1
fi

start_ts=$(date +%s)
while true; do
  if curl -fsS --max-time 5 "$URL" >/dev/null 2>&1; then
    echo "OK: $URL"
    exit 0
  fi
  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))
  if [[ $elapsed -ge $TIMEOUT_SECONDS ]]; then
    echo "Timeout waiting for $URL after ${elapsed}s" >&2
    exit 2
  fi
  sleep "$INTERVAL_SECONDS"
done

#!/usr/bin/env bash
set -euo pipefail

AUTH_BASE_URL="${AUTH_BASE_URL:-}"
AUTH_EMAIL="${AUTH_EMAIL:-}"
AUTH_TIMEOUT_SEC="${AUTH_TIMEOUT_SEC:-5}"
HEALTH_PATH="${AUTH_HEALTH_PATH:-/healthz}"
REQUEST_PATH="${AUTH_REQUEST_PATH:-/api/auth/request-code}"

if [[ -z "${AUTH_BASE_URL}" ]]; then
  echo "AUTH_BASE_URL is required" >&2
  exit 2
fi

if [[ -z "${AUTH_EMAIL}" ]]; then
  echo "AUTH_EMAIL is required" >&2
  exit 2
fi

health_url="${AUTH_BASE_URL%/}${HEALTH_PATH}"
request_url="${AUTH_BASE_URL%/}${REQUEST_PATH}"

health_out=$(curl -sS -o /dev/null -w "%{http_code} %{time_total}" \
  --connect-timeout 2 --max-time "${AUTH_TIMEOUT_SEC}" "${health_url}" || true)

health_code=$(awk '{print $1}' <<< "${health_out}")
health_time=$(awk '{print $2}' <<< "${health_out}")

if [[ "${health_code}" != "200" ]]; then
  echo "healthz FAIL status=${health_code} time_s=${health_time}" >&2
  exit 3
fi

echo "healthz OK time_s=${health_time}"

request_out=$(curl -sS -o /dev/null -w "%{http_code} %{time_total}" \
  --connect-timeout 2 --max-time "${AUTH_TIMEOUT_SEC}" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"${AUTH_EMAIL}\"}" \
  "${request_url}" || true)

request_code=$(awk '{print $1}' <<< "${request_out}")
request_time=$(awk '{print $2}' <<< "${request_out}")

if [[ "${request_code}" != "200" ]]; then
  echo "request_code FAIL status=${request_code} time_s=${request_time}" >&2
  exit 4
fi

echo "request_code OK time_s=${request_time}"

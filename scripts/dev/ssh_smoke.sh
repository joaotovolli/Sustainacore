#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  VM1_HOST=... VM1_USER=... VM2_HOST=... VM2_USER=... \
  [SSH_KEY_PATH=...] scripts/dev/ssh_smoke.sh

Required env vars: VM1_HOST VM1_USER VM2_HOST VM2_USER
Optional: SSH_KEY_PATH (path to private key outside the repo)
USAGE
}

require_env() {
  local missing=()
  for var in "$@"; do
    if [ -z "${!var:-}" ]; then
      missing+=("$var")
    fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "Missing env vars: ${missing[*]}" >&2
    usage
    exit 1
  fi
}

require_env VM1_HOST VM1_USER VM2_HOST VM2_USER

SSH_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout=5
  -o StrictHostKeyChecking=accept-new
)
if [ -n "${SSH_KEY_PATH:-}" ]; then
  SSH_OPTS=(-i "$SSH_KEY_PATH" "${SSH_OPTS[@]}")
fi

check_host() {
  local label="$1"
  local user="$2"
  local host="$3"
  echo "Checking ${label}..."
  ssh "${SSH_OPTS[@]}" "${user}@${host}" "uname -a && echo OK"
}

check_host "VM2" "$VM2_USER" "$VM2_HOST"
check_host "VM1" "$VM1_USER" "$VM1_HOST"

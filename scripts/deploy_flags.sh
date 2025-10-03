#!/usr/bin/env bash
set -euo pipefail

SERVICE_DIR="/etc/systemd/system/sustainacore-ai.service.d"
CONF_PATH="${SERVICE_DIR}/15-persona.conf"

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 VAR=VALUE [VAR=VALUE ...]" >&2
  echo "Example: $0 PERSONA_V1=1 REQUEST_NORMALIZE=1" >&2
  exit 1
fi

mkdir -p "${SERVICE_DIR}"

echo "Writing persona flag overrides to ${CONF_PATH}" >&2
{
  echo "[Service]"
  for pair in "$@"; do
    printf 'Environment="%s"\n' "$pair"
  done
} > "${CONF_PATH}"

echo "Reloading sustainacore-ai.service" >&2
systemctl daemon-reload
systemctl restart sustainacore-ai.service

echo "Effective flags:" >&2
for pair in "$@"; do
  echo " - ${pair}" >&2
  echo "${pair}"
done

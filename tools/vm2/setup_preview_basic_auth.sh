#!/usr/bin/env bash
set -euo pipefail

USER_NAME="${BASIC_AUTH_USER:-}"
USER_PASS="${BASIC_AUTH_PASS:-}"
AUTH_FILE="/etc/nginx/.htpasswd_preview_sustainacore"

if [[ -z "${USER_NAME}" || -z "${USER_PASS}" ]]; then
  echo "BASIC_AUTH_USER and BASIC_AUTH_PASS must be set."
  exit 1
fi

if ! command -v htpasswd >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y apache2-utils
fi

sudo install -m 640 -o root -g www-data /dev/null "${AUTH_FILE}"
sudo htpasswd -b "${AUTH_FILE}" "${USER_NAME}" "${USER_PASS}" >/dev/null

if sudo grep -q "^${USER_NAME}:" "${AUTH_FILE}"; then
  echo "OK"
else
  echo "Failed to create htpasswd entry."
  exit 1
fi

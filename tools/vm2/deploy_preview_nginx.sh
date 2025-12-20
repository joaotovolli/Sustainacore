#!/usr/bin/env bash
set -euo pipefail

SRC_CONF="/opt/code/Sustainacore/infra/nginx/preview.sustainacore.conf"
DEST_CONF="/etc/nginx/conf.d/preview.sustainacore.conf"

if [[ ! -f "${SRC_CONF}" ]]; then
  echo "Missing ${SRC_CONF}"
  exit 1
fi

sudo install -m 644 "${SRC_CONF}" "${DEST_CONF}"
sudo nginx -t
sudo systemctl reload nginx
echo "OK"

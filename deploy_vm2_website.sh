#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [ -f "${SCRIPT_DIR}/.env.vm2" ]; then
  echo "[VM2] Loading environment from .env.vm2..."
  set -a
  . "${SCRIPT_DIR}/.env.vm2"
  set +a
else
  echo "[VM2] No .env.vm2 file found; proceeding with existing environment..."
fi

PYTHON_BIN="${SCRIPT_DIR}/website_django/venv/bin/python"
if [ ! -x "${PYTHON_BIN}" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
  else
    PYTHON_BIN="python"
  fi
fi

echo "[VM2] Applying Django migrations and static collection..."
cd "${SCRIPT_DIR}/website_django"
"${PYTHON_BIN}" manage.py migrate --noinput
"${PYTHON_BIN}" manage.py collectstatic --noinput

echo "[VM2] Restarting gunicorn and reloading nginx..."
sudo systemctl restart gunicorn.service
sudo systemctl reload nginx

echo "[VM2] Deploy completed successfully."

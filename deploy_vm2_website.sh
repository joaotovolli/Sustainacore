#!/usr/bin/env bash
set -euo pipefail

echo "[VM2] Applying Django migrations and static collection..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Prefer VM2 virtualenv Python, but fall back gracefully.
CANDIDATES=(
  "/opt/sustainacore/website_django/venv/bin/python"
  "/opt/code/Sustainacore/website_django/venv/bin/python"
  "${SCRIPT_DIR}/website_django/venv/bin/python"
)

PYTHON_BIN=""
for candidate in "${CANDIDATES[@]}"; do
  if [ -x "$candidate" ]; then
    PYTHON_BIN="$candidate"
    break
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  echo "[VM2] WARNING: No virtualenv Python found, falling back to 'python' on PATH." >&2
# Use the VM2 virtualenv Python if available; fallback to `python` if not.
PYTHON_BIN="./website_django/venv/bin/python"
if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN="python"
fi

# Run Django management commands from the website_django project
cd "${SCRIPT_DIR}/website_django"
${PYTHON_BIN} manage.py migrate --noinput
${PYTHON_BIN} manage.py collectstatic --noinput

echo "[VM2] Restarting gunicorn and reloading nginx..."
sudo systemctl restart gunicorn.service
sudo systemctl reload nginx

echo "[VM2] Deploy completed successfully."

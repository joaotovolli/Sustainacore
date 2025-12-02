#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/opt/code/Sustainacore"
APP_DIR="${PROJECT_ROOT}/website_django"
ENV_FILE="${PROJECT_ROOT}/.env.vm2"
VENV_DIR="${APP_DIR}/venv"

echo "[VM2] Starting Django deploy..."
cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  echo "[VM2] Loading environment from .env.vm2..."
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "[VM2] .env.vm2 not found; continuing with existing environment..."
fi

if [ -d "${VENV_DIR}" ]; then
  echo "[VM2] Activating virtualenv at ${VENV_DIR}..."
  . "${VENV_DIR}/bin/activate"
else
  echo "[VM2] Virtualenv not found; using system Python..."
fi

cd "${APP_DIR}"

PYTHON_BIN=""
if command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "[VM2] No Python interpreter found." >&2
  exit 1
fi

echo "[VM2] Running Django system checks..."
"${PYTHON_BIN}" manage.py check

echo "[VM2] Running Django migrations..."
"${PYTHON_BIN}" manage.py migrate --noinput

echo "[VM2] Collecting static files..."
"${PYTHON_BIN}" manage.py collectstatic --noinput

cd "${PROJECT_ROOT}"

echo "[VM2] Restarting gunicorn.service..."
sudo systemctl restart gunicorn.service

echo "[VM2] Reloading nginx..."
sudo systemctl reload nginx

echo "[VM2] Deploy completed successfully."

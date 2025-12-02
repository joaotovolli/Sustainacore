#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/opt/code/Sustainacore"
ENV_FILE="${PROJECT_ROOT}/.env.vm2"
VENV_DIR="${PROJECT_ROOT}/website_django/venv"
VENV_ACTIVATE="${VENV_DIR}/bin/activate"
VENV_PYTHON="${VENV_DIR}/bin/python"

cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  echo "[VM2] Loading environment from .env.vm2..."
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "[VM2] No .env.vm2 file found; proceeding with existing environment..."
fi

if [ -f "${VENV_ACTIVATE}" ]; then
  echo "[VM2] Activating virtual environment..."
  . "${VENV_ACTIVATE}"
else
  echo "[VM2] Virtual environment not found; continuing without activation."
fi

if [ -x "${VENV_PYTHON}" ]; then
  PYTHON_BIN="${VENV_PYTHON}"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "[VM2] No suitable Python interpreter found." >&2
  exit 1
fi

echo "[VM2] Running Django checks and migrations..."
cd "${PROJECT_ROOT}/website_django"
"${PYTHON_BIN}" manage.py check
"${PYTHON_BIN}" manage.py migrate --noinput
"${PYTHON_BIN}" manage.py collectstatic --noinput

echo "[VM2] Restarting gunicorn and reloading nginx..."
sudo systemctl restart gunicorn.service
sudo systemctl reload nginx

echo "[VM2] Deploy completed successfully."

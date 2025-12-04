#!/usr/bin/env bash
set -euo pipefail

echo "[VM2] Starting SustainaCore Django deploy..."

REPO_ROOT="/opt/code/Sustainacore"
VENV_PATH="$REPO_ROOT/website_django/venv"

# 1) Go to repo root
cd "$REPO_ROOT"

# 2) Load optional local environment file
if [ -f ".env.vm2" ]; then
  echo "[VM2] Loading environment from .env.vm2..."
  set -a
  # shellcheck disable=SC1091
  . ".env.vm2"
  set +a
else
  echo "[VM2] No .env.vm2 file found, continuing without it."
fi

# 3) Activate virtualenv if it exists
if [ -d "$VENV_PATH" ]; then
  echo "[VM2] Activating virtualenv at $VENV_PATH"
  # shellcheck disable=SC1091
  . "$VENV_PATH/bin/activate"
else
  echo "[VM2] Virtualenv $VENV_PATH not found, using system Python."
fi

# 4) Move into Django project
cd "$REPO_ROOT/website_django"

# 5) Choose Python interpreter
PYTHON_BIN="python"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "[VM2] No suitable Python interpreter found (tried python and python3)." >&2
  exit 1
fi

echo "[VM2] Using Python interpreter: $PYTHON_BIN"

# 6) Run Django commands
echo "[VM2] Running Django checks..."
"$PYTHON_BIN" manage.py check

echo "[VM2] Applying migrations..."
"$PYTHON_BIN" manage.py migrate --noinput

echo "[VM2] Collecting static files..."
"$PYTHON_BIN" manage.py collectstatic --noinput

# 7) Return to repo root and restart services
cd "$REPO_ROOT"

echo "[VM2] Restarting gunicorn.service..."
sudo systemctl restart gunicorn.service

echo "[VM2] Reloading nginx..."
sudo systemctl reload nginx

echo "[VM2] Deploy completed successfully."

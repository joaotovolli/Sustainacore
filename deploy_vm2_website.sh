#!/usr/bin/env bash
set -euo pipefail

echo "[VM2] Starting SustainaCore Django deploy..."

REPO_ROOT="/opt/code/Sustainacore"
VENV_PATH="$REPO_ROOT/website_django/venv"
NGINX_CONF_SRC="$REPO_ROOT/infra/nginx/sustainacore.conf"
NGINX_CONF_DEST="/etc/nginx/sites-available/sustainacore"

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

# 7) Ensure Nginx config is up to date and enabled
if [ -f "$NGINX_CONF_SRC" ]; then
  echo "[VM2] Updating nginx config..."
  sudo cp "$NGINX_CONF_SRC" "$NGINX_CONF_DEST"
  sudo ln -sf "$NGINX_CONF_DEST" /etc/nginx/sites-enabled/sustainacore
else
  echo "[VM2] Nginx config not found at $NGINX_CONF_SRC; skipping copy."
fi

echo "[VM2] Restarting gunicorn.service..."
sudo systemctl restart gunicorn.service

echo "[VM2] Validating nginx config..."
sudo nginx -t

echo "[VM2] Reloading nginx..."
sudo systemctl reload nginx

echo "[VM2] Verifying static asset serving..."
STATIC_CHECK_OUTPUT=$(curl -k -I --fail https://127.0.0.1/static/css/main.css -H "Host: sustainacore.org")
echo "$STATIC_CHECK_OUTPUT"
if ! grep -qi "^HTTP/.* 200" <<< "$STATIC_CHECK_OUTPUT"; then
  echo "[VM2] Expected HTTP 200 for static asset check." >&2
  exit 1
fi

if ! grep -qi "Content-Type: text/css" <<< "$STATIC_CHECK_OUTPUT"; then
  echo "[VM2] Expected Content-Type text/css for static asset check." >&2
  exit 1
fi

echo "[VM2] Pinging Google for sitemap..."
if ! (cd "$REPO_ROOT/website_django" && "$PYTHON_BIN" manage.py ping_google --sitemap=https://www.sustainacore.org/sitemap.xml); then
  echo "[VM2] Warning: Google sitemap ping failed (non-fatal)." >&2
fi

echo "[VM2] Deploy completed successfully."

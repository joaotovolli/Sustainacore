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

# 4.1) Ensure Python dependencies are installed
if [ -f "$REPO_ROOT/website_django/requirements.txt" ]; then
  echo "[VM2] Installing Django dependencies..."
  if [ -d "$VENV_PATH" ]; then
    "$VENV_PATH/bin/python" -m pip install -U pip wheel
    "$VENV_PATH/bin/pip" install -r "$REPO_ROOT/website_django/requirements.txt"
  else
    python3 -m pip install -U pip wheel
    python3 -m pip install -r "$REPO_ROOT/website_django/requirements.txt"
  fi
else
  echo "[VM2] requirements.txt not found; skipping pip install."
fi

# 5) Run Django commands in the service environment
echo "[VM2] Running Django checks..."
"$REPO_ROOT/scripts/vm2_manage.sh" check

echo "[VM2] Applying migrations..."
"$REPO_ROOT/scripts/vm2_manage.sh" migrate --noinput

echo "[VM2] Collecting static files..."
"$REPO_ROOT/scripts/vm2_manage.sh" collectstatic --noinput

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
if ! "$REPO_ROOT/scripts/vm2_manage.sh" ping_google --sitemap=https://www.sustainacore.org/sitemap.xml; then
  echo "[VM2] Warning: Google sitemap ping failed (non-fatal)." >&2
fi

echo "[VM2] Deploy completed successfully."

#!/usr/bin/env bash
set -euo pipefail

echo "[VM2] Starting SustainaCore Django deploy..."

REPO_DIR="/opt/code/Sustainacore"
VENV_DIR="${HOME}/.venvs/sustainacore_vm2"
NGINX_CONF_SRC="$REPO_DIR/infra/nginx/sustainacore.conf"
NGINX_CONF_DEST="/etc/nginx/sites-available/sustainacore"


# 1) Go to repo root
cd "$REPO_DIR"

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

# 2.1) Ensure STATIC_VERSION is set for the gunicorn environment
ENV_FILE="/etc/sysconfig/sustainacore-django.env"
STATIC_VERSION="$(git rev-parse --short HEAD 2>/dev/null || date +%s)"
if [ -f "$ENV_FILE" ]; then
  if grep -q "^STATIC_VERSION=" "$ENV_FILE"; then
    sudo sed -i "s/^STATIC_VERSION=.*/STATIC_VERSION=$STATIC_VERSION/" "$ENV_FILE"
  else
    echo "STATIC_VERSION=$STATIC_VERSION" | sudo tee -a "$ENV_FILE" >/dev/null
  fi
else
  echo "STATIC_VERSION=$STATIC_VERSION" | sudo tee "$ENV_FILE" >/dev/null
fi

# 3) Create or repair the user-owned venv
mkdir -p "${HOME}/.venvs"
echo "[VM2] Checking venv health..."
if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "[VM2] venv missing; creating..."
  python3 -m venv "$VENV_DIR"
fi

if ! "$VENV_DIR/bin/python" -m pip --version >/dev/null 2>&1; then
  echo "[VM2] pip is broken; recreating venv..."
  rm -rf "$VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

echo "[VM2] Bootstrapping pip..."
"$VENV_DIR/bin/python" -m ensurepip --upgrade >/dev/null 2>&1 || true
"$VENV_DIR/bin/python" -m pip install -U pip setuptools wheel

# 4) Move into Django project
cd "$REPO_DIR/website_django"

# 4.1) Ensure Python dependencies are installed
if [ -f "$REPO_DIR/website_django/requirements.txt" ]; then
  echo "[VM2] Installing Django dependencies..."
  "$VENV_DIR/bin/python" -m pip install -r "$REPO_DIR/website_django/requirements.txt"
else
  echo "[VM2] requirements.txt not found; skipping pip install."
fi

# 4.2) Validate Django configuration with the refreshed venv
DJANGO_SECRET_KEY=dev-secret "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" check

# 5) Run Django commands in the service environment
echo "[VM2] Running Django checks..."
DJANGO_SECRET_KEY=dev-secret "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" check

echo "[VM2] Applying migrations..."
DJANGO_SECRET_KEY=dev-secret "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" migrate --noinput

echo "[VM2] Collecting static files..."
DJANGO_SECRET_KEY=dev-secret "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" collectstatic --noinput

# 7) Return to repo root and restart services
cd "$REPO_DIR"

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
if ! DJANGO_SECRET_KEY=dev-secret "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" ping_google --sitemap=https://www.sustainacore.org/sitemap.xml; then
  echo "[VM2] Warning: Google sitemap ping failed (non-fatal)." >&2
fi

echo "[VM2] Deploy completed successfully."

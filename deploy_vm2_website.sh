#!/usr/bin/env bash
set -euo pipefail

echo "[VM2] Starting SustainaCore Django deploy..."

REPO_DIR="/opt/code/Sustainacore"
VENV_DIR="${HOME}/.venvs/sustainacore_vm2"
NGINX_CONF_SRC="$REPO_DIR/infra/nginx/sustainacore.conf"
NGINX_CONF_DEST="/etc/nginx/sites-available/sustainacore"

# 0) Ensure sudo is available non-interactively
if ! sudo -n true >/dev/null 2>&1; then
  echo "[VM2] sudo -n is required for deploy. Configure passwordless sudo for deploy tasks." >&2
  exit 1
fi


# 1) Go to repo root
cd "$REPO_DIR"

# 2) Validate env files used by gunicorn (do not parse or print secrets)
ENV_FILES=(
  "/etc/sustainacore.env"
  "/etc/sustainacore/db.env"
  "/etc/sysconfig/sustainacore-django.env"
)

for env_file in "${ENV_FILES[@]}"; do
  if [ ! -f "$env_file" ]; then
    echo "[VM2] Missing env file: $env_file" >&2
    echo "[VM2] See docs/vm2-website-deploy.md#env-files" >&2
    exit 1
  fi
  if ! sudo -n test -r "$env_file" >/dev/null 2>&1; then
    echo "[VM2] Cannot access env file via sudo -n: $env_file" >&2
    echo "[VM2] See docs/vm2-website-deploy.md#env-files" >&2
    exit 1
  fi
done

# 2.1) Define environment flags for systemd-run/gunicorn
STATIC_VERSION="$(git rev-parse --short HEAD 2>/dev/null || date +%s)"

# 2.2) Ensure gunicorn loads all env files consistently
GUNICORN_DROPIN_DIR="/etc/systemd/system/gunicorn.service.d"
GUNICORN_DROPIN_FILE="${GUNICORN_DROPIN_DIR}/sustainacore-env.conf"
sudo -n mkdir -p "$GUNICORN_DROPIN_DIR"
sudo -n tee "$GUNICORN_DROPIN_FILE" >/dev/null <<EOF
[Service]
EnvironmentFile=/etc/sustainacore.env
EnvironmentFile=/etc/sustainacore/db.env
EnvironmentFile=/etc/sysconfig/sustainacore-django.env
Environment=SUSTAINACORE_ENV=production
Environment=STATIC_VERSION=${STATIC_VERSION}
EOF
sudo -n systemctl daemon-reload

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

run_manage() {
  local cmd="$1"
  shift || true
  local unit="vm2-deploy-${cmd}-$(date +%s)"
  if ! command -v systemd-run >/dev/null 2>&1; then
    echo "[VM2] systemd-run not available; cannot run manage.py safely." >&2
    exit 1
  fi
  sudo -n systemd-run \
    --quiet \
    --collect \
    --wait \
    --pipe \
    --unit "${unit}" \
    --property WorkingDirectory="$REPO_DIR/website_django" \
    --property EnvironmentFile=/etc/sustainacore.env \
    --property EnvironmentFile=/etc/sustainacore/db.env \
    --property EnvironmentFile=/etc/sysconfig/sustainacore-django.env \
    --property Environment=SUSTAINACORE_ENV=production \
    --property Environment=STATIC_VERSION="${STATIC_VERSION}" \
    "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" "$cmd" "$@"
}

# 5) Run Django commands in the service environment
echo "[VM2] Running Django checks..."
run_manage check

echo "[VM2] Applying migrations..."
run_manage migrate --noinput

echo "[VM2] Verifying Oracle telemetry..."
if ! run_manage diagnose_db --fail-on-sqlite --verify-insert --timeout 60; then
  echo "[VM2] Oracle telemetry verification failed. Check DB env variables and privileges." >&2
  exit 1
fi

echo "[VM2] Collecting static files..."
run_manage collectstatic --noinput

# 7) Return to repo root and restart services
cd "$REPO_DIR"

# 7) Ensure Nginx config is up to date and enabled
if [ -f "$NGINX_CONF_SRC" ]; then
  echo "[VM2] Updating nginx config..."
  sudo -n cp "$NGINX_CONF_SRC" "$NGINX_CONF_DEST"
  sudo -n ln -sf "$NGINX_CONF_DEST" /etc/nginx/sites-enabled/sustainacore
else
  echo "[VM2] Nginx config not found at $NGINX_CONF_SRC; skipping copy."
fi

echo "[VM2] Restarting gunicorn.service..."
sudo -n systemctl restart gunicorn.service

echo "[VM2] Validating nginx config..."
sudo -n nginx -t

echo "[VM2] Reloading nginx..."
sudo -n systemctl reload nginx

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

echo "[VM2] Verifying telemetry inserts..."
telemetry_before="$(run_manage shell -c "from telemetry.models import WebEvent; print(WebEvent.objects.using('default').count())")"
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/ >/dev/null
telemetry_after="$(run_manage shell -c "from telemetry.models import WebEvent; print(WebEvent.objects.using('default').count())")"
echo "[VM2] telemetry_count_before ${telemetry_before}"
echo "[VM2] telemetry_count_after ${telemetry_after}"
if [ "${telemetry_after}" -le "${telemetry_before}" ]; then
  echo "[VM2] Telemetry count did not increase after request." >&2
  exit 1
fi

echo "[VM2] Pinging Google for sitemap..."
if ! run_manage ping_google --sitemap=https://www.sustainacore.org/sitemap.xml; then
  echo "[VM2] Warning: Google sitemap ping failed (non-fatal)." >&2
fi

echo "[VM2] Deploy completed successfully."

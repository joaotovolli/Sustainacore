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

# 2.1) Helper to load env files safely without printing secrets
ENV_FILES=(
  "/etc/sustainacore.env"
  "/etc/sustainacore/db.env"
  "/etc/sysconfig/sustainacore-django.env"
)

load_env_file() {
  local env_file="$1"
  if [ -r "$env_file" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$env_file"
    set +a
    return 0
  fi
  if sudo -n true >/dev/null 2>&1; then
    if sudo -n test -r "$env_file" >/dev/null 2>&1; then
      set -a
      # shellcheck disable=SC1090
      . <(sudo -n cat "$env_file")
      set +a
      return 0
    fi
  fi
  return 1
}

load_env_files_or_fail() {
  local missing=()
  for env_file in "${ENV_FILES[@]}"; do
    if [ -f "$env_file" ]; then
      if load_env_file "$env_file"; then
        echo "[VM2] Loaded environment from $env_file"
      else
        missing+=("$env_file")
      fi
    fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "[VM2] Cannot read env files: ${missing[*]}" >&2
    echo "[VM2] Fix permissions or allow sudo -n cat. See docs/vm2-website-deploy.md#env-files" >&2
    exit 1
  fi
}

# 2.2) Load systemd environment files used by gunicorn (do not print secrets)
load_env_files_or_fail

# 2.3) Ensure STATIC_VERSION is set for the gunicorn environment
ENV_FILE="/etc/sustainacore.env"
STATIC_VERSION="$(git rev-parse --short HEAD 2>/dev/null || date +%s)"
if [ -f "$ENV_FILE" ]; then
  if grep -q "^STATIC_VERSION=" "$ENV_FILE"; then
    sudo -n sed -i "s/^STATIC_VERSION=.*/STATIC_VERSION=$STATIC_VERSION/" "$ENV_FILE"
  else
    echo "STATIC_VERSION=$STATIC_VERSION" | sudo -n tee -a "$ENV_FILE" >/dev/null
  fi
else
  echo "STATIC_VERSION=$STATIC_VERSION" | sudo -n tee "$ENV_FILE" >/dev/null
fi

# 2.4) Ensure production environment flag is present (banner should never show on prod host)
if [ -f "$ENV_FILE" ]; then
  if grep -q "^SUSTAINACORE_ENV=" "$ENV_FILE"; then
    sudo -n sed -i "s/^SUSTAINACORE_ENV=.*/SUSTAINACORE_ENV=production/" "$ENV_FILE"
  else
    echo "SUSTAINACORE_ENV=production" | sudo -n tee -a "$ENV_FILE" >/dev/null
  fi
else
  echo "SUSTAINACORE_ENV=production" | sudo -n tee "$ENV_FILE" >/dev/null
fi

# 2.5) Ensure gunicorn loads all env files consistently
GUNICORN_DROPIN_DIR="/etc/systemd/system/gunicorn.service.d"
GUNICORN_DROPIN_FILE="${GUNICORN_DROPIN_DIR}/sustainacore-env.conf"
sudo -n mkdir -p "$GUNICORN_DROPIN_DIR"
sudo -n tee "$GUNICORN_DROPIN_FILE" >/dev/null <<'EOF'
[Service]
EnvironmentFile=/etc/sustainacore.env
EnvironmentFile=/etc/sustainacore/db.env
EnvironmentFile=/etc/sysconfig/sustainacore-django.env
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
  if sudo -n true >/dev/null 2>&1 && command -v systemd-run >/dev/null 2>&1; then
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
      "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" "$cmd" "$@"
    return $?
  fi

  load_env_files_or_fail
  DJANGO_SECRET_KEY=dev-secret "$VENV_DIR/bin/python" "$REPO_DIR/website_django/manage.py" "$cmd" "$@"
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

echo "[VM2] Pinging Google for sitemap..."
if ! run_manage ping_google --sitemap=https://www.sustainacore.org/sitemap.xml; then
  echo "[VM2] Warning: Google sitemap ping failed (non-fatal)." >&2
fi

echo "[VM2] Deploy completed successfully."

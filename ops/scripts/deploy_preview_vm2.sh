#!/usr/bin/env bash
set -euo pipefail

: "${PREVIEW_SHA:?Set PREVIEW_SHA}"

REPO_DIR="/opt/code/Sustainacore"
PREVIEW_DIR="/opt/code/Sustainacore_preview"
VENV_DIR="${HOME}/.venvs/sustainacore_vm2"
GUNICORN_PORT="8001"
UNIT_FILE="/etc/systemd/system/gunicorn-preview.service"
NGINX_CONF_DEST="/etc/nginx/conf.d/preview.sustainacore.conf"

if ! sudo -n true >/dev/null 2>&1; then
  echo "[preview] sudo -n is required for preview deploy." >&2
  exit 1
fi

cd "$REPO_DIR"

git fetch origin

if [ -d "$PREVIEW_DIR/.git" ] || [ -f "$PREVIEW_DIR/.git" ]; then
  echo "[preview] Updating existing preview worktree..."
  git -C "$PREVIEW_DIR" fetch origin
  git -C "$PREVIEW_DIR" reset --hard "$PREVIEW_SHA"
else
  echo "[preview] Creating preview worktree..."
  git worktree add "$PREVIEW_DIR" "$PREVIEW_SHA"
fi

STATIC_VERSION="$(git -C "$PREVIEW_DIR" rev-parse --short HEAD)"

mkdir -p "${HOME}/.venvs"
if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "[preview] Creating preview venv..."
  python3 -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/python" -m pip install -U pip setuptools wheel

if [ -f "$PREVIEW_DIR/website_django/requirements.txt" ]; then
  echo "[preview] Installing dependencies..."
  "$VENV_DIR/bin/python" -m pip install -r "$PREVIEW_DIR/website_django/requirements.txt"
fi

cat <<UNIT | sudo -n tee "$UNIT_FILE" >/dev/null
[Unit]
Description=Gunicorn (preview) for SustainaCore Django
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=$PREVIEW_DIR/website_django
EnvironmentFile=/etc/sustainacore.env
EnvironmentFile=/etc/sustainacore/db.env
EnvironmentFile=/etc/sysconfig/sustainacore-django.env
Environment=SUSTAINACORE_ENV=preview
Environment=STATIC_VERSION=$STATIC_VERSION
ExecStart=$VENV_DIR/bin/gunicorn --bind 127.0.0.1:$GUNICORN_PORT --workers 2 --threads 2 core.wsgi:application
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

sudo -n systemctl daemon-reload
sudo -n systemctl enable gunicorn-preview.service
sudo -n systemctl restart gunicorn-preview.service

echo "[preview] Installing preview nginx config..."
sudo -n tee "$NGINX_CONF_DEST" >/dev/null <<NGINX
# Nginx configuration for preview.sustainacore.org (VM2)

log_format sustainacore_preview_timing '\$remote_addr - \$remote_user [\$time_local] '
    '"\$request" \$status \$body_bytes_sent '
    '"\$http_referer" "\$http_user_agent" '
    'rt=\$request_time urt=\$upstream_response_time '
    'uct=\$upstream_connect_time uht=\$upstream_header_time us=\$upstream_status';

server {
    listen 80;
    server_name preview.sustainacore.org 127.0.0.1 localhost;
    access_log /var/log/nginx/preview_sustainacore_access.log sustainacore_preview_timing;

    auth_basic "Preview";
    auth_basic_user_file /etc/nginx/.htpasswd_preview_sustainacore;

    add_header X-Robots-Tag "noindex, nofollow, noarchive, nosnippet" always;

    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_min_length 1024;
    gzip_http_version 1.1;
    gzip_types
        text/plain
        text/css
        application/json
        application/javascript
        text/xml
        application/xml
        application/xml+rss
        text/javascript
        image/svg+xml;

    location = /robots.txt {
        add_header Content-Type text/plain;
        return 200 "User-agent: *\\nDisallow: /\\n";
    }

    location / {
        include proxy_params;
        proxy_pass http://127.0.0.1:${GUNICORN_PORT};
    }

    location /static/ {
        alias ${PREVIEW_DIR}/website_django/staticfiles/;
        add_header Cache-Control "public, max-age=86400";
    }
}

server {
    listen 443 ssl http2;
    server_name preview.sustainacore.org 127.0.0.1 localhost;
    access_log /var/log/nginx/preview_sustainacore_access.log sustainacore_preview_timing;

    ssl_certificate /etc/letsencrypt/live/sustainacore.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/sustainacore.org/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    auth_basic "Preview";
    auth_basic_user_file /etc/nginx/.htpasswd_preview_sustainacore;

    add_header X-Robots-Tag "noindex, nofollow, noarchive, nosnippet" always;

    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_min_length 1024;
    gzip_http_version 1.1;
    gzip_types
        text/plain
        text/css
        application/json
        application/javascript
        text/xml
        application/xml
        application/xml+rss
        text/javascript
        image/svg+xml;

    location = /robots.txt {
        add_header Content-Type text/plain;
        return 200 "User-agent: *\\nDisallow: /\\n";
    }

    location / {
        include proxy_params;
        proxy_pass http://127.0.0.1:${GUNICORN_PORT};
    }

    location /static/ {
        alias ${PREVIEW_DIR}/website_django/staticfiles/;
        add_header Cache-Control "public, max-age=86400";
    }
}
NGINX
sudo -n nginx -t
sudo -n systemctl reload nginx

echo "[preview] Collecting static files..."
cd "$PREVIEW_DIR/website_django"
DJANGO_SECRET_KEY=dev-secret "$VENV_DIR/bin/python" manage.py collectstatic --noinput

echo "[preview] Deploy complete. build=$STATIC_VERSION"

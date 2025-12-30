# VM2 Django Website & Deployment

## Overview
- VM2 hosts the public Django website for Sustainacore, served by Gunicorn behind Nginx.
- The repository is checked out on VM2 at `/opt/code/Sustainacore`, and the Django project lives inside `website_django/`.

The site also proxies Ask2 traffic to VM1:
- `/ask2/` serves a minimal HTML form that posts messages to `/ask2/api/`.
- `/ask2/api/` accepts JSON or form data, normalizes `message`/`user_message`, and forwards `{ "user_message": "..." }` to VM1 `/api/ask2`.
- `BACKEND_API_BASE` defaults to `http://10.0.0.120:8080` (VM1) and `BACKEND_API_TOKEN` is read from the environment. When set, the proxy adds `Authorization: Bearer <token>`.
- VM1 `/api/news`, `/api/tech100`, and `/api/ask2` all expect `Authorization: Bearer $API_AUTH_TOKEN`. Set the same shared token in VM1 as `API_AUTH_TOKEN` and in VM2 as `BACKEND_API_TOKEN` so authenticated calls succeed.

## Environment Variables and Secrets
- The Gunicorn systemd unit uses:
  - `/etc/sustainacore.env`
  - `/etc/sustainacore/db.env`
  - `/etc/sysconfig/sustainacore-django.env`
- Oracle config uses `DB_USER`, `DB_PASSWORD` (or `DB_PASS`), and `DB_DSN` (preferred). `ORACLE_*` vars are accepted as fallback.

### Env Files & Permissions
- The deploy user may not have direct read access to `/etc/sustainacore*.env`.
- The deploy script validates env files via `sudo -n test -r` and uses `systemd-run` `EnvironmentFile=` to consume them.
- It never sources env files in shell.
- If sudo read access is unavailable, the deploy will fail fast with a link to this doc.
- Do NOT chmod secrets world-readable. If needed, adjust group permissions (optional):
  - `chgrp ubuntu /etc/sustainacore.env /etc/sustainacore/db.env`
  - `chmod 640 /etc/sustainacore.env /etc/sustainacore/db.env`

### Oracle Guardrail
- VM2 production MUST use Oracle (no sqlite).
- Verify using:
  - `scripts/vm2_manage.sh diagnose_db --fail-on-sqlite --verify-insert --timeout 60`

## Deployment Script: `deploy_vm2_website.sh`
- `cd` to the repository root.
- Use the same env files as gunicorn for Django management commands (via `sudo -n systemd-run` when available).
- Run `manage.py migrate --noinput` and `manage.py collectstatic --noinput` inside `website_django/`.
- Fail fast if Oracle is not active in production (`diagnose_db --fail-on-sqlite`).
- Restart `gunicorn.service` and reload `nginx` via `systemctl`.

Manual deploy commands:
```bash
cd /opt/code/Sustainacore
git fetch origin main
git reset --hard origin/main
bash ./deploy_vm2_website.sh
```

## GitHub Action: Deploy VM2 Django Website
- The GitHub Action connects to VM2 over SSH and invokes `bash ./deploy_vm2_website.sh` from the repo root.
- Keep workflow changes aligned with this script so it remains the single entry point for VM2 website deployments.

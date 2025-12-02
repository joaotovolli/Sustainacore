# VM2 Django Website & Deployment

## Overview
- VM2 hosts the public Django website for Sustainacore, served by Gunicorn behind Nginx.
- The repository is checked out on VM2 at `/opt/code/Sustainacore`, and the Django project lives inside `website_django/`.

The site also proxies Ask2 traffic to VM1:
- `/ask2/` serves a minimal HTML form that posts messages to `/ask2/api/`.
- `/ask2/api/` accepts JSON or form data, normalizes `message`/`user_message`, and forwards `{ "user_message": "..." }` to VM1 `/api/ask2`.
- `BACKEND_API_BASE` defaults to `http://10.0.0.120:8080` (VM1) and `BACKEND_API_TOKEN` is read from the environment. When set, the proxy adds `Authorization: Bearer <token>`.

## Environment Variables and Secrets
- The Gunicorn systemd unit uses `EnvironmentFile=/etc/sustainacore.env` to load production settings.
- A repo-local `.env.vm2` lives at the repo root on VM2 (not committed). It should include required Django settings plus the Ask2 proxy values:
  - `DJANGO_SECRET_KEY`, `DJANGO_ALLOWED_HOSTS`, `DJANGO_DEBUG`
  - `BACKEND_API_BASE` (VM1 base URL) and optional `BACKEND_API_TOKEN`
  - Any database variables needed for the site itself (not Ask2)
- The deploy script sources `.env.vm2` if it exists. Create or update this file directly on VM2 (or via Codex CLI) as needed and keep real secrets out of Git.

## Deployment Script: `deploy_vm2_website.sh`
- `cd` to the repository root.
- If `.env.vm2` exists, export its variables for the shell session with a clean `set -a`/`set +a` block.
- Prefer `website_django/venv/bin/python` when present; otherwise fall back to `python3`/`python` on `PATH`.
- Run `manage.py migrate --noinput` and `manage.py collectstatic --noinput` inside `website_django/`.
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

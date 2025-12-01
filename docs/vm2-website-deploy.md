# VM2 Django Website & Deployment

## Overview
- VM2 hosts the public Django website for Sustainacore, served by Gunicorn behind Nginx.
- The repository is checked out on VM2 at `/opt/code/Sustainacore`, and the Django project lives inside `website_django/`.

## Environment Variables and Secrets
- The Gunicorn systemd unit uses `EnvironmentFile=/etc/sustainacore.env` to load production settings.
- A repo-local `.env.vm2` lives at the repo root on VM2 (not committed). It provides private values such as `DJANGO_SECRET_KEY=***` and `DJANGO_DEBUG=***` when running management commands.
- The deploy script sources `.env.vm2` if it exists. Create or update this file directly on VM2 (or via Codex CLI) as needed and keep real secrets out of Git.

## Deployment Script: `deploy_vm2_website.sh`
- `cd` to the repository root.
- If `.env.vm2` exists, export its variables for the shell session.
- Prefer `website_django/venv/bin/python` when present; otherwise fall back to `python` on `PATH`.
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

## Ask SustainaCore Chat (`/ask2/`)
- `/ask2/` renders the public chat UI.
- `/ask2/api/` is a JSON POST endpoint (currently `@csrf_exempt`) that returns placeholder responses.
- The endpoint is intentionally stubbed so a future task can replace it with a real Ask2 backend (e.g., running on VM1) without altering the UI.

#!/usr/bin/env bash
set -euo pipefail

: "${PR_SHA:?Set PR_SHA to the PR commit SHA}"

repo_root="/opt/code/Sustainacore"
preview_root="/opt/code/Sustainacore_preview"
python_bin="/home/ubuntu/.venvs/sustainacore_vm2/bin/python"

if [[ ! -x "$python_bin" ]]; then
  echo "python venv not found at $python_bin" >&2
  exit 1
fi

if [[ ! -d "$repo_root/.git" ]]; then
  echo "repo root not found at $repo_root" >&2
  exit 1
fi

if [[ ! -d "$preview_root" ]]; then
  git -C "$repo_root" worktree add "$preview_root" origin/main
fi

cd "$preview_root"

git fetch origin

git checkout --detach "$PR_SHA"

"$python_bin" -m compileall website_django

DJANGO_SECRET_KEY=test "$python_bin" website_django/manage.py check

if ! sudo -n true >/dev/null 2>&1; then
  echo "sudo -n unavailable; cannot restart gunicorn-preview.service" >&2
  exit 1
fi

sudo -n systemctl restart gunicorn-preview.service

curl -fsS -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8001/

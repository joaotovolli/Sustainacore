#!/usr/bin/env bash
set -euo pipefail

repo_root="/opt/code/Sustainacore"
preview_root="/opt/code/Sustainacore_preview"
python_bin="/home/ubuntu/.venvs/sustainacore_vm2/bin/python"
arg="${1:-}"
pr_sha="${PR_SHA:-}"

if [[ -n "$arg" ]]; then
  if [[ "$arg" =~ ^[0-9]+$ ]]; then
    pr_sha="$(gh pr view -R joaotovolli/Sustainacore "$arg" --json headRefOid -q .headRefOid)"
  else
    pr_sha="$arg"
  fi
fi

if [[ -z "$pr_sha" ]]; then
  echo "usage: $0 <pr-number|sha> (or set PR_SHA)" >&2
  exit 2
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

git checkout --detach "$pr_sha"

if ! sudo -n true >/dev/null 2>&1; then
  echo "sudo -n unavailable; need NOPASSWD for gunicorn-preview.service restart" >&2
  exit 1
fi

sudo -n systemctl restart gunicorn-preview.service

curl -sS -o /dev/null -w "%{http_code}\n" --connect-timeout 1 --max-time 2 http://127.0.0.1:8001/

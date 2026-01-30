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

health_mode=""
health_target=""

bind_line="$(systemctl cat gunicorn-preview.service 2>/dev/null | rg -o -- '--bind\\s+[^ ]+' | head -n 1 || true)"
if [[ -n "$bind_line" ]]; then
  bind_value="$(echo "$bind_line" | awk '{print $2}')"
  if [[ "$bind_value" == unix:* ]]; then
    health_mode="unix"
    health_target="${bind_value#unix:}"
  else
    health_mode="tcp"
    health_target="$bind_value"
  fi
fi

if [[ -z "$health_mode" ]]; then
  proxy_line="$(sudo nginx -T 2>/dev/null | rg -o 'proxy_pass\\s+http://[^;]+' | rg -m 1 'preview|8001|127.0.0.1' || true)"
  if [[ -n "$proxy_line" ]]; then
    proxy_target="$(echo "$proxy_line" | awk '{print $2}' | sed 's#http://##')"
    health_mode="tcp"
    health_target="$proxy_target"
  fi
fi

if [[ -z "$health_mode" ]]; then
  health_mode="tcp"
  health_target="127.0.0.1:8001"
fi

check_local() {
  if [[ "$health_mode" == "unix" ]]; then
    curl -fsS --max-time 1 --unix-socket "$health_target" http://localhost/ >/dev/null 2>&1
  else
    local host="${health_target%%:*}"
    local port="${health_target##*:}"
    curl -fsS --max-time 1 "http://${host}:${port}/" >/dev/null 2>&1
  fi
}

echo "Preview healthcheck target: ${health_mode}:${health_target}"
local_ok="false"
for attempt in {1..20}; do
  if check_local; then
    local_ok="true"
    break
  fi
  sleep 0.3
done

if [[ "$local_ok" != "true" ]]; then
  echo "Local preview healthcheck failed; collecting diagnostics." >&2
  systemctl status --no-pager gunicorn-preview.service || true
  sudo journalctl -u gunicorn-preview.service -n 80 --no-pager || true
  external_status="$(curl -fsS -o /dev/null -w "%{http_code}" --max-time 3 https://preview.sustainacore.org/ || echo 000)"
  if [[ "$external_status" == "200" ]]; then
    echo "Warning: local healthcheck failed; external endpoint healthy." >&2
    exit 0
  fi
  echo "Preview healthcheck failed (local + external)." >&2
  exit 1
fi

echo "Local preview healthcheck ok."

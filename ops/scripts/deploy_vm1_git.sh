#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/opt/sustainacore-ai}
SC_IDX_DIR=${SC_IDX_DIR:-/home/opc/Sustainacore}
BRANCH=${BRANCH:-main}
REMOTE=${REMOTE:-origin}

deploy_checkout() {
  local target_dir="$1"

  if [ ! -d "$target_dir/.git" ]; then
    echo "ERROR: $target_dir is not a git checkout."
    exit 1
  fi

  cd "$target_dir"

  if [ -n "$(git status --porcelain)" ]; then
    echo "ERROR: $target_dir has uncommitted changes. Clean the working tree before deploy."
    exit 1
  fi

  git fetch "$REMOTE" "$BRANCH"
  git checkout "$BRANCH"
  git pull --ff-only "$REMOTE" "$BRANCH"

  if [ -f requirements.txt ]; then
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -U pip wheel
    pip install -r requirements.txt || true
  fi
}

targets=("$APP_DIR")
if [ "$SC_IDX_DIR" != "$APP_DIR" ]; then
  targets+=("$SC_IDX_DIR")
fi

for target_dir in "${targets[@]}"; do
  deploy_checkout "$target_dir"
done

sudo systemctl daemon-reload || true
sudo systemctl restart sustainacore-ai.service

curl -fsS "http://127.0.0.1:8080/healthz" >/dev/null
echo "deploy complete"

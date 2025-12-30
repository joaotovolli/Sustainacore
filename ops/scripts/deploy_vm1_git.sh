#!/usr/bin/env bash
set -euo pipefail

APP_DIR=${APP_DIR:-/opt/sustainacore-ai}
BRANCH=${BRANCH:-main}
REMOTE=${REMOTE:-origin}

if [ ! -d "$APP_DIR/.git" ]; then
  echo "ERROR: $APP_DIR is not a git checkout."
  exit 1
fi

cd "$APP_DIR"

if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: $APP_DIR has uncommitted changes. Clean the working tree before deploy."
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

sudo systemctl daemon-reload || true
sudo systemctl restart sustainacore-ai.service

curl -fsS "http://127.0.0.1:8080/healthz" >/dev/null
echo "deploy complete"

#!/usr/bin/env bash
set -euo pipefail
: "${VM_HOST:?Set VM_HOST}"; : "${SSH_KEY:?Set SSH_KEY}"
APP=/opt/sustainacore-ai
rsync -az --delete --exclude .git --exclude .venv -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" ./ opc@"$VM_HOST":"$APP"/
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new opc@"$VM_HOST" bash -lc "
  cd $APP && python3 -m venv .venv && source .venv/bin/activate &&
  pip install -U pip wheel && [ -f requirements.txt ] && pip install -r requirements.txt || true &&
  sudo systemctl daemon-reload || true &&
  sudo systemctl restart sustainacore-ai.service || true &&
  curl -fsS 'http://127.0.0.1:8080/ask2?q=ping&k=1' | head -c 400; echo"

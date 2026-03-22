#!/usr/bin/env bash
set -euo pipefail
: "${VM_HOST:?Set VM_HOST}"; : "${SSH_KEY:?Set SSH_KEY}"
VM_USER="${VM_USER:-opc}"
APP_DIR="${APP_DIR:-/opt/sustainacore-ai}"
SC_IDX_DIR="${SC_IDX_DIR:-/home/opc/Sustainacore}"
SC_IDX_USER="${SC_IDX_USER:-opc}"
# Stage into the SSH user's home so we can support images where `opc` cannot SSH (e.g., Ubuntu images where `ubuntu` is required).
STAGE="sustainacore_deploy_stage"

rsync -az --delete \
  --exclude .git \
  --exclude .venv \
  --exclude .env \
  --exclude wallet \
  --exclude 'wallet/**' \
  -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes" \
  ./ "${VM_USER}@${VM_HOST}:${STAGE}/"

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes \
  "${VM_USER}@${VM_HOST}" \
  "APP_DIR='${APP_DIR}' SC_IDX_DIR='${SC_IDX_DIR}' SC_IDX_USER='${SC_IDX_USER}' STAGE='${STAGE}' SQLCLI_BIN='${SQLCLI_BIN:-sql}' bash -s" <<'REMOTE'
set -euo pipefail
STAGE_PATH="$HOME/$STAGE"

sync_checkout() {
  local target_dir="$1"
  sudo mkdir -p "$target_dir"
  sudo rsync -a --delete \
    --exclude .venv \
    --exclude .env \
    --exclude wallet \
    --exclude 'wallet/**' \
    "$STAGE_PATH"/ "$target_dir"/
}

sync_checkout "$APP_DIR"

if [ "$SC_IDX_DIR" != "$APP_DIR" ]; then
  sync_checkout "$SC_IDX_DIR"
  if id -u "$SC_IDX_USER" >/dev/null 2>&1; then
    sudo chown -R "$SC_IDX_USER:$SC_IDX_USER" "$SC_IDX_DIR"
  fi
fi

cd "$APP_DIR"
if [ -x scripts/vm_deploy.sh ]; then
  SQLCLI_BIN="$SQLCLI_BIN" bash scripts/vm_deploy.sh
fi
python3 -m venv .venv && source .venv/bin/activate
pip install -U pip wheel
[ -f requirements.txt ] && pip install -r requirements.txt || true
sudo systemctl daemon-reload || true
sudo systemctl restart sustainacore-ai.service || true
curl -fsS 'http://127.0.0.1:8080/ask2?q=ping&k=1' | head -c 400
echo
REMOTE

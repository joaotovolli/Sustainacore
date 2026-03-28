#!/usr/bin/env bash
set -euo pipefail

: "${VM_HOST:?Set VM_HOST}"
: "${SSH_KEY:?Set SSH_KEY}"

VM_USER="${VM_USER:-opc}"
API_APP="${API_APP:-/opt/sustainacore-ai}"
SC_IDX_LINK="${SC_IDX_LINK:-/home/opc/Sustainacore}"
STAGE="${STAGE:-~/sustainacore_deploy_stage}"
TARGET_SHA="${TARGET_SHA:-$(git rev-parse --verify HEAD)}"
REPO_URL="${REPO_URL:-$(git remote get-url origin)}"

rsync -az --delete \
  --exclude .git \
  --exclude .venv \
  --exclude .env \
  --exclude wallet \
  --exclude 'wallet/**' \
  -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes" \
  ./ "${VM_USER}@${VM_HOST}:${STAGE}/"

ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes "${VM_USER}@${VM_HOST}" \
  TARGET_SHA="$TARGET_SHA" REPO_URL="$REPO_URL" API_APP="$API_APP" SC_IDX_LINK="$SC_IDX_LINK" STAGE="$STAGE" \
  bash -lc '
    set -euo pipefail

    ensure_sc_idx_release() {
      local current_target release_root release_dir venv_target

      current_target="$(readlink -f "$SC_IDX_LINK" 2>/dev/null || true)"
      release_root="/opt/sustainacore-sc-idx-${TARGET_SHA:0:12}"
      release_dir="$release_root"

      if [[ ! -d "$release_dir/.git" ]]; then
        sudo rm -rf "$release_dir"
        sudo git clone --no-checkout "$REPO_URL" "$release_dir"
      fi

      sudo git -C "$release_dir" fetch --depth 1 origin "$TARGET_SHA"
      sudo git -C "$release_dir" checkout --detach --force "$TARGET_SHA"
      sudo chown -R opc:opc "$release_dir"

      venv_target=""
      if [[ -n "$current_target" && -L "$current_target/.venv" ]]; then
        venv_target="$(readlink -f "$current_target/.venv" 2>/dev/null || true)"
      fi
      if [[ -z "$venv_target" && -L "$API_APP/.venv" ]]; then
        venv_target="$(readlink -f "$API_APP/.venv" 2>/dev/null || true)"
      fi
      if [[ -z "$venv_target" && -d "$API_APP/.venv" ]]; then
        venv_target="$API_APP/.venv"
      fi
      if [[ -n "$venv_target" ]]; then
        sudo -n -u opc ln -sfn "$venv_target" "$release_dir/.venv"
      fi

      sudo ln -sfn "$release_dir" "$SC_IDX_LINK"
      sudo cp "$release_dir"/infra/systemd/sc-idx-*.service /etc/systemd/system/
      sudo cp "$release_dir"/infra/systemd/sc-idx-*.timer /etc/systemd/system/
    }

    sudo mkdir -p "$API_APP"
    sudo rsync -a --delete \
      --exclude .venv \
      --exclude .env \
      --exclude wallet \
      --exclude "wallet/**" \
      "$STAGE"/ "$API_APP"/

    cd "$API_APP"
    if [[ -x scripts/vm_deploy.sh ]]; then
      SQLCLI_BIN="${SQLCLI_BIN:-sql}" bash scripts/vm_deploy.sh
    fi

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -U pip wheel
    if [[ -f requirements.txt ]]; then
      pip install -r requirements.txt || true
    fi

    ensure_sc_idx_release

    sudo systemctl daemon-reload
    sudo systemctl reset-failed sc-idx-pipeline.service sc-idx-price-ingest.service sc-idx-completeness-check.service sc-idx-index-calc.service || true
    sudo systemctl restart sustainacore-ai.service || true

    curl -fsS "http://127.0.0.1:8080/ask2?q=ping&k=1" | head -c 400
    echo
    printf "sc_idx_release=%s\n" "$(readlink -f "$SC_IDX_LINK")"
    sudo -n -u opc git -C "$(readlink -f "$SC_IDX_LINK")" rev-parse --short HEAD
  '

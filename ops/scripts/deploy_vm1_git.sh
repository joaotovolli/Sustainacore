#!/usr/bin/env bash
set -euo pipefail

AI_APP_DIR=${AI_APP_DIR:-/opt/sustainacore-ai}
SC_IDX_LINK=${SC_IDX_LINK:-/home/opc/Sustainacore}
BRANCH=${BRANCH:-main}
REMOTE=${REMOTE:-origin}

if [[ ! -d "$AI_APP_DIR/.git" ]]; then
  echo "ERROR: $AI_APP_DIR is not a git checkout."
  exit 1
fi

current_sc_idx=$(readlink -f "$SC_IDX_LINK" 2>/dev/null || true)
if [[ -n "$current_sc_idx" && ! -d "$current_sc_idx/.git" ]]; then
  echo "ERROR: $current_sc_idx is not a git checkout."
  exit 1
fi

cd "$AI_APP_DIR"
if [[ -n "$(git status --porcelain)" ]]; then
  echo "ERROR: $AI_APP_DIR has uncommitted changes. Clean the working tree before deploy."
  exit 1
fi

git fetch "$REMOTE" "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only "$REMOTE" "$BRANCH"

if [[ -f requirements.txt ]]; then
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -U pip wheel
  pip install -r requirements.txt || true
fi

repo_url="$(git config --get remote.${REMOTE}.url)"
target_sha="$(git rev-parse --verify HEAD)"
release_dir="/opt/sustainacore-sc-idx-${target_sha:0:12}"

if [[ ! -d "$release_dir/.git" ]]; then
  sudo rm -rf "$release_dir"
  sudo git clone --no-checkout "$repo_url" "$release_dir"
fi

sudo git -C "$release_dir" fetch --depth 1 "$REMOTE" "$target_sha"
sudo git -C "$release_dir" checkout --detach --force "$target_sha"
sudo chown -R opc:opc "$release_dir"

if [[ -L "$AI_APP_DIR/.venv" ]]; then
  sudo -n -u opc ln -sfn "$(readlink -f "$AI_APP_DIR/.venv")" "$release_dir/.venv"
elif [[ -d "$AI_APP_DIR/.venv" ]]; then
  sudo -n -u opc ln -sfn "$AI_APP_DIR/.venv" "$release_dir/.venv"
fi

sudo ln -sfn "$release_dir" "$SC_IDX_LINK"
sudo cp "$release_dir"/infra/systemd/sc-idx-*.service /etc/systemd/system/
sudo cp "$release_dir"/infra/systemd/sc-idx-*.timer /etc/systemd/system/
sudo systemctl daemon-reload || true
sudo systemctl reset-failed sc-idx-pipeline.service sc-idx-price-ingest.service sc-idx-completeness-check.service sc-idx-index-calc.service || true
sudo systemctl restart sustainacore-ai.service

curl -fsS "http://127.0.0.1:8080/healthz" >/dev/null
printf "deploy complete\n"
printf "sc_idx_release=%s\n" "$(readlink -f "$SC_IDX_LINK")"
sudo -n -u opc git -C "$(readlink -f "$SC_IDX_LINK")" rev-parse --short HEAD

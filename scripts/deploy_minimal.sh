#!/usr/bin/env bash
set -euo pipefail
TS=$(date +%Y%m%d-%H%M%S)
APP=/opt/sustainacore-ai
sudo mkdir -p /opt/backups

echo "[1/4] Backup"
sudo tar -C / -czf /opt/backups/sustainacore-ai_${TS}.tgz \
  ${APP}/app.py \
  ${APP}/app/retrieval \
  ${APP}/.ask_facade/wsgi_ask_facade.py || true

echo "[2/4] Sync changed files"
# rsync your built tree to /opt/sustainacore-ai/, or copy the specific files:
#   sudo rsync -av app.py ${APP}/
#   sudo rsync -av app/retrieval/adapter.py ${APP}/app/retrieval/
#   sudo rsync -av scripts/verify_ask2.sh ${APP}/scripts/

echo "[3/4] Restart"
sudo systemctl restart sustainacore-ai
sleep 1
systemctl --no-pager -l status sustainacore-ai | sed -n '1,40p'

echo "[4/4] Verify"
bash scripts/verify_ask2.sh

# Rollback commands:
# sudo tar -C / -xzf /opt/backups/sustainacore-ai_${TS}.tgz
# sudo systemctl restart sustainacore-ai


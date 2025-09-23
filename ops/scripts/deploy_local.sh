#!/usr/bin/env bash
set -euo pipefail
cd /opt/sustainacore-ai
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl daemon-reload || true
sudo systemctl restart sustainacore-ai.service || true
curl -fsS "http://127.0.0.1:8080/ask2?q=ping&k=1" | head -c 400; echo

#!/usr/bin/env bash
set -euo pipefail
cd /opt/sustainacore-ai
source .venv/bin/activate
uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080

#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

sudo cp "${script_dir}/gemini-jobs-worker.service" /etc/systemd/system/gemini-jobs-worker.service
sudo cp "${script_dir}/gemini-jobs-worker.timer" /etc/systemd/system/gemini-jobs-worker.timer
sudo systemctl daemon-reload
sudo systemctl disable --now gemini-jobs-worker.timer || true
sudo systemctl enable --now gemini-jobs-worker.service

systemctl status gemini-jobs-worker.service --no-pager

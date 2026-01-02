#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/opc/Sustainacore"
WORKER_DIR="$ROOT_DIR/tools/research_generator"
VENV_DIR="$WORKER_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/python" -m pip install python-docx matplotlib

sudo cp "$WORKER_DIR/systemd/research-generator.service" /etc/systemd/system/research-generator.service
sudo cp "$WORKER_DIR/systemd/research-generator.timer" /etc/systemd/system/research-generator.timer
sudo cp "$WORKER_DIR/systemd/research-generator-manual.service" /etc/systemd/system/research-generator-manual.service
sudo cp "$WORKER_DIR/systemd/research-generator-manual.timer" /etc/systemd/system/research-generator-manual.timer
sudo systemctl daemon-reload
sudo systemctl enable --now research-generator.timer
sudo systemctl enable --now research-generator-manual.timer
sudo systemctl status research-generator.timer --no-pager
sudo systemctl status research-generator-manual.timer --no-pager

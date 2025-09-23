#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

# Paths
APP_DIR="/opt/sustainacore-ai"
APP_PY="$APP_DIR/app.py"
MW_PY="$APP_DIR/orchestrator_mw.py"

TS="$(date +%F-%H%M%S)"
BACKUP="$APP_DIR/app.py.pre-$TS"

echo "==> Snapshot: $BACKUP"
sudo cp -a "$APP_PY" "$BACKUP"

echo "==> Installing orchestrator module"
sudo install -m 0644 "$MW_PY" "$APP_DIR/orchestrator_mw.py"

echo "==> Appending safe loader to app.py (idempotent)"
LOADER_MARK="# --- Multihit Orchestrator Loader v2 ---"
if ! sudo grep -qF "$LOADER_MARK" "$APP_PY"; then
sudo tee -a "$APP_PY" >/dev/null <<'PY'
# --- Multihit Orchestrator Loader v2 ---
try:
    import importlib, types
    from orchestrator_mw import install_multihit
    if isinstance(locals().get("app"), (object,)):
        app = install_multihit(app)
except Exception as _e:
    # do not crash the app on loader failure
    print("Multihit loader error:", _e)
# --- End Loader ---
PY
else
  echo "Loader already present; skipping append."
fi

echo "==> Syntax check"
sudo /opt/sustainacore-ai/.venv/bin/python -m py_compile "$APP_PY" || {
  echo "Compile failed: reverting to backup"
  sudo cp -a "$BACKUP" "$APP_PY"
  exit 1
}

echo "==> Reloading service"
if systemctl is-active --quiet sustainacore-ai.service; then
  sudo systemctl kill -s HUP sustainacore-ai.service || sudo systemctl restart sustainacore-ai.service
else
  sudo systemctl restart sustainacore-ai.service
fi

echo "==> Smoke test (local)"
sleep 1
set +e
curl -sS -m 6 -H 'Content-Type: application/json' \
  -d '{"question":"Is Cisco part of the TECH100 index?","top_k":8}' \
  http://127.0.0.1:8080/ask | head -c 300; echo
echo -e "\n==> Done."

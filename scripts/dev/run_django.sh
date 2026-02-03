#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PORT="${PORT:-8000}"

if [ ! -f "$ROOT_DIR/website_django/manage.py" ]; then
  echo "ERROR: website_django/manage.py not found." >&2
  exit 1
fi

bash "$ROOT_DIR/scripts/dev/setup_wsl2.sh"

# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

DJANGO_SECRET_KEY="${DJANGO_SECRET_KEY:-test}" \
NEWS_UI_DATA_MODE="${NEWS_UI_DATA_MODE:-fixture}" \
TECH100_UI_DATA_MODE="${TECH100_UI_DATA_MODE:-fixture}" \
AI_REG_UI_DATA_MODE="${AI_REG_UI_DATA_MODE:-fixture}" \
  python "$ROOT_DIR/website_django/manage.py" check

echo "Tip: in another shell, run scripts/dev/wait_for_http.sh http://127.0.0.1:${PORT}/"

DJANGO_SECRET_KEY="${DJANGO_SECRET_KEY:-test}" \
NEWS_UI_DATA_MODE="${NEWS_UI_DATA_MODE:-fixture}" \
TECH100_UI_DATA_MODE="${TECH100_UI_DATA_MODE:-fixture}" \
AI_REG_UI_DATA_MODE="${AI_REG_UI_DATA_MODE:-fixture}" \
  python "$ROOT_DIR/website_django/manage.py" runserver 127.0.0.1:"$PORT" --noreload

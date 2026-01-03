#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUTPUT_DIR="${ROOT_DIR}/tools/research_generator/output"
STATE_DIR="${ROOT_DIR}/tools/research_generator/.state"

DRY_RUN=0
DO_DELETE=0
for arg in "$@"; do
  if [[ "$arg" == "--dry-run" ]]; then
    DRY_RUN=1
  fi
  if [[ "$arg" == "--yes" ]]; then
    DO_DELETE=1
  fi
done

if [[ "$DO_DELETE" -eq 1 ]]; then
  /usr/bin/python3 -m tools.research_generator.cleanup --yes
else
  /usr/bin/python3 -m tools.research_generator.cleanup --dry-run
  exit 0
fi

rm -rf "${OUTPUT_DIR}"/* || true

if [[ -d "${STATE_DIR}" ]]; then
  find "${STATE_DIR}" -type f ! -name "quota_state.json" ! -name ".gitignore" -delete
  find "${STATE_DIR}" -type d -empty -delete
fi

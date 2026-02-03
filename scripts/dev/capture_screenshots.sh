#!/usr/bin/env bash
set -euo pipefail

URL_LIST="${1:-}"
OUT_DIR="${2:-}"

if [[ -z "$URL_LIST" || -z "$OUT_DIR" ]]; then
  echo "Usage: $0 <url_list.json> <output_dir>" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

python3 - <<'PY' "$URL_LIST" "$OUT_DIR"
import json
import os
import subprocess
import sys

url_list = sys.argv[1]
out_dir = sys.argv[2]

with open(url_list, "r", encoding="utf-8") as f:
    items = json.load(f)

for entry in items:
    slug = entry["slug"]
    local_url = entry["local_url"]
    prod_url = entry["prod_url"]
    local_path = os.path.join(out_dir, f"local_{slug}.png")
    prod_path = os.path.join(out_dir, f"prod_{slug}.png")

    subprocess.run([
        "npx",
        "playwright",
        "screenshot",
        "--wait-for-timeout=1000",
        "--full-page",
        local_url,
        local_path,
    ], check=False)

    subprocess.run([
        "npx",
        "playwright",
        "screenshot",
        "--wait-for-timeout=1000",
        "--full-page",
        prod_url,
        prod_path,
    ], check=False)
PY

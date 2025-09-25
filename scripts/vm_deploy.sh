#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

cd "${REPO_ROOT}"

echo "Running database migrations"
bash scripts/db_migrate.sh

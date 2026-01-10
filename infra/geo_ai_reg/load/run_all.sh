#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
<<<<<<< HEAD
Usage: run_all.sh --bundle <path> | --dir <path> [--drop-and-recreate] [--truncate] [--dry-run]

Options:
  --bundle <path>        Path to the bundle zip
  --dir <path>           Path to extracted bundle directory
=======
Usage: run_all.sh --bundle <path> [--drop-and-recreate] [--truncate] [--dry-run]

Options:
  --bundle <path>        Path to the bundle zip or extracted folder
>>>>>>> 8981e9a (Add GEO AI regulation Oracle loader and documentation)
  --drop-and-recreate    Drop tables and reapply DDL before loading
  --truncate             Truncate tables before loading
  --dry-run              Show load order and CSV counts without DB writes
EOF
}

bundle_path=""
<<<<<<< HEAD
bundle_dir=""
=======
>>>>>>> 8981e9a (Add GEO AI regulation Oracle loader and documentation)
drop_and_recreate=false
truncate=false
dry_run=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle)
      bundle_path="${2:-}"
      shift 2
      ;;
<<<<<<< HEAD
    --dir)
      bundle_dir="${2:-}"
      shift 2
      ;;
=======
>>>>>>> 8981e9a (Add GEO AI regulation Oracle loader and documentation)
    --drop-and-recreate)
      drop_and_recreate=true
      shift
      ;;
    --truncate)
      truncate=true
      shift
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
 done

<<<<<<< HEAD
if [[ -z "$bundle_path" && -z "$bundle_dir" ]]; then
  echo "Missing --bundle or --dir" >&2
=======
if [[ -z "$bundle_path" ]]; then
  echo "Missing --bundle" >&2
>>>>>>> 8981e9a (Add GEO AI regulation Oracle loader and documentation)
  usage
  exit 2
fi

<<<<<<< HEAD
if [[ -n "$bundle_path" && -n "$bundle_dir" ]]; then
  echo "Choose only one of --bundle or --dir" >&2
  exit 2
fi

=======
>>>>>>> 8981e9a (Add GEO AI regulation Oracle loader and documentation)
if [[ "$drop_and_recreate" == "true" && "$truncate" == "true" ]]; then
  echo "Choose only one of --drop-and-recreate or --truncate" >&2
  exit 2
fi

python3 tools/oracle/preflight_oracle.py

<<<<<<< HEAD
if [[ -n "$bundle_dir" ]]; then
  cmd=(python3 infra/geo_ai_reg/load/load_bundle.py --dir "$bundle_dir")
else
  cmd=(python3 infra/geo_ai_reg/load/load_bundle.py --bundle "$bundle_path")
fi
=======
cmd=(python3 infra/geo_ai_reg/load/load_bundle.py --bundle "$bundle_path")
>>>>>>> 8981e9a (Add GEO AI regulation Oracle loader and documentation)
if [[ "$drop_and_recreate" == "true" ]]; then
  cmd+=(--drop-and-recreate)
fi
if [[ "$truncate" == "true" ]]; then
  cmd+=(--truncate)
fi
if [[ "$dry_run" == "true" ]]; then
  cmd+=(--dry-run)
fi

"${cmd[@]}"

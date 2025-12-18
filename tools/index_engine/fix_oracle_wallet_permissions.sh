#!/usr/bin/env bash
set -euo pipefail

WALLET_DIR="${1:-}"
if [[ -z "${WALLET_DIR}" ]]; then
  WALLET_DIR="${TNS_ADMIN:-${DB_TNS_ADMIN:-${ORA_TNS_ADMIN:-${WALLET_LOCATION:-${ORACLE_WALLET_LOCATION:-${DB_WALLET_DIR:-/opt/adb_wallet}}}}}}"
fi

if [[ ! -d "${WALLET_DIR}" ]]; then
  echo "wallet_dir_missing=${WALLET_DIR}"
  exit 1
fi

echo "wallet_dir=${WALLET_DIR}"
ls -ld "${WALLET_DIR}"
ls -l "${WALLET_DIR}"

chown root:opc "${WALLET_DIR}"
chmod 750 "${WALLET_DIR}"
find "${WALLET_DIR}" -maxdepth 1 -type f -exec chown root:opc {} \;
find "${WALLET_DIR}" -maxdepth 1 -type f -exec chmod 640 {} \;

if command -v restorecon >/dev/null 2>&1; then
  restorecon -Rv "${WALLET_DIR}" >/dev/null 2>&1 || true
fi

echo "after_permissions:"
ls -ld "${WALLET_DIR}"
ls -l "${WALLET_DIR}"

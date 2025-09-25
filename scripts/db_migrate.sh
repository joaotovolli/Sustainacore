#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC2046,SC2163
  set -a
  source "${ENV_FILE}"
  set +a
fi

: "${ORACLE_USER:?Set ORACLE_USER in the environment or .env file}"
: "${ORACLE_PASSWORD:?Set ORACLE_PASSWORD in the environment or .env file}"
: "${ORACLE_CONNECT_STRING:?Set ORACLE_CONNECT_STRING in the environment or .env file}"
SQLCLI_BIN=${SQLCLI_BIN:-sql}

if ! command -v "${SQLCLI_BIN}" >/dev/null 2>&1; then
  echo "SQLcl binary '${SQLCLI_BIN}' not found on PATH" >&2
  exit 1
fi

CONN_STRING="${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_CONNECT_STRING}"
TOOLS_DIR="${REPO_ROOT}/db/tools"
MIGRATIONS_DIR="${REPO_ROOT}/db/migrations"

echo "Ensuring SCHEMA_VERSION table exists"
"${SQLCLI_BIN}" -S "${CONN_STRING}" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
@${TOOLS_DIR}/schema_version.sql
SQL

readarray -t migrations < <(find "${MIGRATIONS_DIR}" -maxdepth 1 -type f -name 'V*.sql' | sort)

baseline_version=""
if ((${#migrations[@]} > 0)); then
  first_migration=${migrations[0]}
  if [ -n "${first_migration}" ]; then
    baseline_version=$(basename "${first_migration}")
    baseline_version="${baseline_version%%__*}"
  fi
fi

for migration in "${migrations[@]}"; do
  [ -n "${migration}" ] || continue
  filename=$(basename "${migration}")
  version="${filename%%__*}"
  if [ -z "${version}" ]; then
    continue
  fi

  migration_version=$("${SQLCLI_BIN}" -S "${CONN_STRING}" <<SQL | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF VERIFY OFF PAGESIZE 0
SELECT COUNT(*) FROM schema_version WHERE version = '${version}';
EXIT
SQL
)

  if [[ "${migration_version}" == "0" ]]; then
    if [[ -n "${baseline_version}" && "${version}" == "${baseline_version}" ]]; then
      existing_objects=$("${SQLCLI_BIN}" -S "${CONN_STRING}" <<'SQL' | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF VERIFY OFF PAGESIZE 0
SELECT COUNT(*)
  FROM user_objects
 WHERE object_type IN (
          'TABLE', 'VIEW', 'INDEX', 'SEQUENCE', 'TRIGGER',
          'PACKAGE', 'PACKAGE BODY', 'FUNCTION', 'PROCEDURE',
          'TYPE', 'TYPE BODY', 'MATERIALIZED VIEW'
      )
   AND object_name <> 'SCHEMA_VERSION';
EXIT
SQL
)

      if [[ "${existing_objects}" != "0" ]]; then
        echo "Existing schema objects detected; marking ${filename} as applied"
        "${SQLCLI_BIN}" -S "${CONN_STRING}" <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
INSERT INTO schema_version(version) VALUES ('${version}');
COMMIT;
SQL
        continue
      fi
    fi

    abs_path=$(cd "$(dirname "${migration}")" && pwd)/$(basename "${migration}")
    echo "Applying migration ${filename}"
    "${SQLCLI_BIN}" -S "${CONN_STRING}" <<SQL
@${TOOLS_DIR}/migrate.sql ${version} ${abs_path}
SQL
  else
    echo "Skipping migration ${filename}; already applied"
  fi
done


#!/usr/bin/env bash
set -euo pipefail

SECRETS_FILE="/etc/sustainacore-ai/secrets.env"
DEFAULT_SMTP_USER="info@sustainacore.org"
DEFAULT_MAIL_FROM="info@sustainacore.org"
DEFAULT_MAIL_TO="joaotovolli@outlook.com"
DEFAULT_SMTP_HOST="smtp.ionos.co.uk"
DEFAULT_SMTP_PORT="587"

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    exec sudo "$0" "$@"
  fi
}

prompt() {
  local prompt_text="$1" default_value="$2"
  read -r -p "$prompt_text [$default_value]: " value
  if [ -z "$value" ]; then
    value="$default_value"
  fi
  printf '%s' "$value"
}

prompt_secret() {
  local prompt_text="$1"
  read -r -s -p "$prompt_text: " value
  printf '\n' >&2
  printf '%s' "$value"
}

upsert_key() {
  local file="$1" key="$2" value="$3"
  local tmp
  tmp="$(mktemp)"
  if [ -f "$file" ]; then
    awk -v k="$key" -v v="$value" 'BEGIN{found=0} $0 ~ "^"k"=" {print k"="v; found=1; next} {print} END{if(!found) print k"="v}' "$file" > "$tmp"
  else
    printf '%s=%s\n' "$key" "$value" > "$tmp"
  fi
  mv "$tmp" "$file"
}

connectivity_check() {
  local host="$1" port="$2"
  if ! timeout 5 bash -c "cat </dev/null >/dev/tcp/${host}/${port}" 2>/dev/null; then
    echo "ERROR: cannot reach ${host}:${port} (TCP connect failed)" >&2
    exit 1
  fi
}

main() {
  require_root "$@"

  echo "== SMTP setup for IONOS (VM1) =="
  smtp_user=$(prompt "SMTP username" "$DEFAULT_SMTP_USER")
  smtp_pass=$(prompt_secret "SMTP password")
  mail_from=$(prompt "MAIL_FROM" "$DEFAULT_MAIL_FROM")
  mail_to=$(prompt "MAIL_TO" "$DEFAULT_MAIL_TO")
  smtp_host=$(prompt "SMTP host" "$DEFAULT_SMTP_HOST")
  smtp_port=$(prompt "SMTP port" "$DEFAULT_SMTP_PORT")

  connectivity_check "$smtp_host" "$smtp_port"

  touch "$SECRETS_FILE"
  chmod 600 "$SECRETS_FILE"
  chown root:root "$SECRETS_FILE"

  upsert_key "$SECRETS_FILE" "SMTP_HOST" "$smtp_host"
  upsert_key "$SECRETS_FILE" "SMTP_PORT" "$smtp_port"
  upsert_key "$SECRETS_FILE" "SMTP_USER" "$smtp_user"
  upsert_key "$SECRETS_FILE" "SMTP_PASS" "$smtp_pass"
  upsert_key "$SECRETS_FILE" "MAIL_FROM" "$mail_from"
  upsert_key "$SECRETS_FILE" "MAIL_TO" "$mail_to"

  chmod 600 "$SECRETS_FILE"
  chown root:root "$SECRETS_FILE"

  set -a
  # shellcheck disable=SC1090
  source "$SECRETS_FILE"
  set +a

  echo "Running test send..."
  if python3 tools/email/send_test_email.py; then
    echo "Setup complete."
  else
    echo "ERROR: send_test_email failed" >&2
    exit 1
  fi
}

main "$@"

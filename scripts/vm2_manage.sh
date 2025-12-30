#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: scripts/vm2_manage.sh <manage.py args>" >&2
  exit 2
fi

subcmd="$1"
shift || true
timestamp="$(date +%s)"
unit="vm2-django-${subcmd}-${timestamp}"
python_bin="/home/ubuntu/.venvs/sustainacore_vm2/bin/python"

if ! sudo -n true >/dev/null 2>&1; then
  echo "sudo -n unavailable; vm2_manage.sh requires non-interactive sudo." >&2
  exit 1
fi

sudo -n systemd-run \
  --quiet \
  --collect \
  --wait \
  --pipe \
  --unit "${unit}" \
  --property WorkingDirectory=/opt/code/Sustainacore/website_django \
  --property EnvironmentFile=/etc/sustainacore.env \
  --property EnvironmentFile=/etc/sustainacore/db.env \
  --property EnvironmentFile=/etc/sysconfig/sustainacore-django.env \
  "${python_bin}" \
  /opt/code/Sustainacore/website_django/manage.py \
  "${subcmd}" "$@"

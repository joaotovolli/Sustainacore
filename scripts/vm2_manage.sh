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

sudo systemd-run \
  --quiet \
  --collect \
  --wait \
  --pipe \
  --unit "${unit}" \
  --property WorkingDirectory=/opt/code/Sustainacore/website_django \
  --property EnvironmentFile=/etc/sustainacore.env \
  --property EnvironmentFile=/etc/sustainacore/db.env \
  --property EnvironmentFile=/etc/sysconfig/sustainacore-django.env \
  /opt/sustainacore/website_django/venv/bin/python \
  /opt/code/Sustainacore/website_django/manage.py \
  "${subcmd}" "$@"

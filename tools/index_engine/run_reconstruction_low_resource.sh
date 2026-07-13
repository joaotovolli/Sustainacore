#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: run_reconstruction_low_resource.sh <repair-tool arguments>" >&2
  exit 2
fi

unit="sc-idx-reconstruction-$(date -u +%Y%m%d%H%M%S)"
repo="$(readlink -f /home/opc/Sustainacore)"
python="/home/opc/Sustainacore/.venv/bin/python"

exec sudo -n systemd-run \
  --wait \
  --pipe \
  --collect \
  --unit="${unit}" \
  --property=Type=exec \
  --property=User=opc \
  --property=WorkingDirectory="${repo}" \
  --property=Environment="PYTHONPATH=${repo}" \
  --property=EnvironmentFile=/etc/sustainacore/db.env \
  --property=EnvironmentFile=/etc/sustainacore/index.env \
  --property=EnvironmentFile=/etc/sustainacore-ai/app.env \
  --property=EnvironmentFile=/etc/sustainacore-ai/secrets.env \
  --property=StateDirectory=sustainacore/sc_idx \
  --property=Nice=10 \
  --property=IOSchedulingClass=idle \
  --property=CPUQuota=70% \
  --property=MemoryHigh=650M \
  --property=KillMode=control-group \
  --property=TimeoutStartSec=30 \
  --setenv=SC_IDX_RECONSTRUCTION_STATUS_FILE=/var/lib/sustainacore/sc_idx/reconstruction_status.json \
  --setenv=SC_IDX_INDEX_WRITE_BATCH_SIZE=250 \
  --setenv=SC_IDX_PORTFOLIO_WRITE_BATCH_SIZE=250 \
  -- "${python}" tools/db_migrations/repair_sc_idx_corporate_actions.py "$@"

#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: run_reconstruction_low_resource.sh <repair-tool arguments>" >&2
  exit 2
fi

unit="sc-idx-reconstruction-$(date -u +%Y%m%d%H%M%S)"
repo="$(readlink -f /home/opc/Sustainacore)"
python="/home/opc/Sustainacore/.venv/bin/python"
status_file="/var/lib/sustainacore/sc_idx/reconstruction_status.json"
call_timeout_ms="${SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS:-300000}"

sudo -n systemd-run \
  --quiet \
  --no-block \
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
  --setenv="SC_IDX_RECONSTRUCTION_STATUS_FILE=${status_file}" \
  --setenv="SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS=${call_timeout_ms}" \
  --setenv=SC_IDX_INDEX_WRITE_BATCH_SIZE=250 \
  --setenv=SC_IDX_PORTFOLIO_WRITE_BATCH_SIZE=250 \
  -- "${python}" tools/db_migrations/repair_sc_idx_corporate_actions.py "$@"

echo "launch_status=ACCEPTED"
echo "unit_name=${unit}.service"
echo "status_file=${status_file}"
echo "status_command=sudo -n systemctl show ${unit}.service -p ActiveState -p SubState -p Result -p ExecMainStatus --no-pager && ${python} tools/index_engine/reconstruction_status.py --status-file ${status_file}"

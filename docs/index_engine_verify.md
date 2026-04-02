## SC_IDX pipeline verification (VM1)

Run from the repo root unless noted.

### 1. LangGraph smoke path

No Oracle writes, no provider credits:

```bash
source .venv/bin/activate
python tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

Expected signals:

- exit code `0`
- text/JSON reports under `tools/audit/output/pipeline_runs/`
- telemetry JSON under `tools/audit/output/pipeline_telemetry/`

### 2. Targeted orchestration tests

```bash
source .venv/bin/activate
pytest -q \
  tests/test_index_engine_alerts.py \
  tests/test_index_engine_alert_state.py \
  tests/test_index_engine_daily_telemetry_report.py \
  tests/test_run_pipeline.py \
  tests/test_run_pipeline_helpers.py \
  tests/test_run_daily_selection.py \
  tests/test_run_daily_guards.py \
  tests/test_run_daily_oracle_preflight.py \
  tests/test_run_daily_trading_days.py \
  tests/test_update_trading_days_auto.py \
  tests/test_market_data_readiness.py \
  tests/test_index_engine_impute_replacement.py
```

Expected result:

- all tests pass

### 3. Safe failure-email verification

Check the real SMTP config path without printing secrets:

```bash
source .venv/bin/activate
python - <<'PY'
from app.index_engine.alerts import smtp_configuration_status
print(smtp_configuration_status())
PY
```

Expected result:

- output only includes booleans, counts, `delivery_state`, and `missing_env`
- no SMTP password, token, or recipient values are printed

Failure-path decision coverage:

```bash
source .venv/bin/activate
pytest -q \
  tests/test_index_engine_alerts.py \
  tests/test_index_engine_alert_state.py \
  tests/test_run_pipeline.py
```

Expected result:

- `failed` and `blocked` alert paths are covered
- stale and repeated-degraded alert paths are covered
- same-day duplicate suppression is covered
- failed SMTP sends do not consume the once-per-day gate

### 4. Daily telemetry report rendering

```bash
source .venv/bin/activate
python tools/index_engine/daily_telemetry_report.py --skip-db --dry-run
```

Expected result:

- the report renders from the latest pipeline artifacts
- output includes freshness, alignment, stale signals, expected target date, alert state, repo identity, and artifact paths
- artifacts are written under `tools/audit/output/pipeline_daily/`

### 5. Oracle preflight

```bash
python3 tools/oracle/preflight_oracle.py
```

Expected result:

- Oracle user prints successfully
- no wallet/env error

### 6. VM1 operational run

```bash
python3 tools/index_engine/run_pipeline.py --restart
```

Expected signals:

- latest `SC_IDX_PIPELINE_STATE` rows show the node sequence for the run
- latest `SC_IDX_JOB_RUNS` row for `job_name='sc_idx_pipeline'` ends in one of:
  - `OK`
  - `DEGRADED`
  - `SKIP`
  - `ERROR`
  - `BLOCKED`
- latest report exists in `tools/audit/output/pipeline_runs/`
- latest telemetry snapshot exists in `tools/audit/output/pipeline_telemetry/`
- latest report/health artifacts show the active `repo_root` and `repo_head`
- if the run is `BLOCKED` at `acquire_lock`, there should be no newly executed
  `determine_target_dates` stage for that `run_id`
- a new invocation after a terminal `BLOCKED` or `FAILED` run should create a new `run_id`; only
  incomplete runs should resume
- even an early blocked run should still show last-known `expected_target_date`,
  `latest_complete_date`, and key table max dates when Oracle preflight succeeded
- a run is not truly fresh until `SC_IDX_LEVELS`, `SC_IDX_STATS_DAILY`,
  `SC_IDX_PORTFOLIO_ANALYTICS_DAILY`, and `SC_IDX_PORTFOLIO_POSITION_DAILY`
  all reach the same expected target date
- `SC_IDX_PORTFOLIO_ANALYTICS_DAILY` and `SC_IDX_PORTFOLIO_POSITION_DAILY` max dates match the latest `SC_IDX_LEVELS` trade date

### 7. Scheduler checks

```bash
systemctl list-timers --all | rg -i "sc-idx"
systemctl status sc-idx-pipeline.service
systemctl status sc-telemetry-report.service
sudo journalctl -u sc-idx-pipeline.service -n 200 --no-pager
sudo journalctl -u sc-telemetry-report.service -n 200 --no-pager
systemd-analyze verify \
  infra/systemd/sc-idx-pipeline.service \
  infra/systemd/sc-idx-pipeline.timer \
  infra/systemd/sc-telemetry-report.service \
  infra/systemd/sc-telemetry-report.timer
```

Expected result:

- `sc-idx-pipeline.timer` is present and scheduled
- `sc-idx-pipeline.service` is the primary VM1 orchestration service
- `systemctl show sc-idx-pipeline.service -p ExecStart` does not include `/usr/bin/flock`
- `sc-telemetry-report.timer` is present and scheduled
- `sc-telemetry-report.service` points to `tools/index_engine/daily_telemetry_report.py --send`

### 8. Freshness and deploy-drift triage

```bash
systemctl show sc-idx-pipeline.service -p WorkingDirectory -p ExecStart
grep -E '^(repo_root|repo_head|calendar_max_date|levels_max_date|stats_max_date|portfolio_max_date|portfolio_position_max_date|alignment_verdict|last_error)=' \
  tools/audit/output/pipeline_health_latest.txt
```

Expected result:

- the active `WorkingDirectory` matches the repo root reported in the health artifact
- the reported `repo_head` matches the intended deployed commit
- freshness dates and `last_error` explain whether the incident is scheduler drift, stale data, or a true code failure

### 9. Run-state verification query

```sql
SELECT run_id, status, error_msg, started_at, ended_at
FROM SC_IDX_JOB_RUNS
WHERE job_name = 'sc_idx_pipeline'
ORDER BY started_at DESC
FETCH FIRST 20 ROWS ONLY;
```

```sql
SELECT run_id, stage_name, stage_status, started_at, ended_at
FROM SC_IDX_PIPELINE_STATE
ORDER BY started_at DESC
FETCH FIRST 50 ROWS ONLY;
```

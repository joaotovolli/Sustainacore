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
  tests/test_run_pipeline.py \
  tests/test_run_pipeline_helpers.py \
  tests/test_run_daily_selection.py \
  tests/test_run_daily_guards.py \
  tests/test_run_daily_oracle_preflight.py \
  tests/test_run_daily_trading_days.py \
  tests/test_market_data_readiness.py \
  tests/test_index_engine_impute_replacement.py
```

Expected result:

- all tests pass

### 3. Oracle preflight

```bash
python3 tools/oracle/preflight_oracle.py
```

Expected result:

- Oracle user prints successfully
- no wallet/env error

### 4. VM1 operational run

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
- `SC_IDX_PORTFOLIO_ANALYTICS_DAILY` and `SC_IDX_PORTFOLIO_POSITION_DAILY` max dates match the latest `SC_IDX_LEVELS` trade date

### 5. Scheduler checks

```bash
systemctl list-timers --all | rg -i "sc-idx"
systemctl status sc-idx-pipeline.service
sudo journalctl -u sc-idx-pipeline.service -n 200 --no-pager
```

Expected result:

- `sc-idx-pipeline.timer` is present and scheduled
- `sc-idx-pipeline.service` is the primary VM1 orchestration service

### 6. Run-state verification query

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

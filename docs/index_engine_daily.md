<!-- cspell:ignore LOOKBACK -->
# SC_IDX daily pipeline cadence

## Primary VM1 flow

The primary VM1 operational path is the LangGraph pipeline:

```bash
python3 tools/index_engine/run_pipeline.py
```

It is scheduled by `sc-idx-pipeline.timer` and coordinates:

- Oracle preflight
- trading-day refresh
- provider budget planning
- provider readiness probe
- ingest
- completeness check
- imputation plus bounded replacement attempts
- index + statistics calculation
- TECH100 portfolio analytics refresh
- report generation
- alert decisions
- telemetry emission
- terminal status persistence

The standalone ingest, completeness, and index-calc units remain compatibility tools and should not be treated as the primary orchestration path.

## Scheduling

Primary systemd schedule on VM1:

- ingest compatibility timer: `00:00`, `05:00`, `09:00`, `13:00` UTC
- primary LangGraph pipeline timer: `00:30`, `05:30`, `09:30`, `13:30` UTC

The pipeline timer is deliberately offset from ingest so VM1 can catch the latest EOD availability without overlapping the compatibility unit.

## Budget and readiness behavior

- Daily call budget is derived from `SC_IDX_JOB_RUNS` usage for `provider='MARKET_DATA'`.
- Budget envs:
  - `SC_IDX_MARKET_DATA_DAILY_LIMIT`
  - `SC_IDX_MARKET_DATA_DAILY_BUFFER`
  - `SC_IDX_MARKET_DATA_CREDIT_BUFFER` (back-compat alias)
- If the budget is exhausted before work begins, the run concludes `clean_skip` with reason `daily_budget_stop`.
- Provider readiness probes use a bounded fallback window and conclude `clean_skip` with reason `provider_not_ready` when no candidate date is ready.
- Mid-run budget exhaustion is recorded as a degraded outcome instead of looping forever.
- Provider/API calls remain serialized through the existing lock and throttle controls; LangGraph only decides when to call them.

## Portfolio analytics refresh

After `calc_index`, the graph refreshes the TECH100 portfolio analytics backend through:

```bash
python3 tools/index_engine/build_portfolio_analytics.py --apply-ddl --skip-preflight
```

Operational rules:

- the stage is additive and uses only existing TECH100 / SC_IDX Oracle sources
- it advances `SC_IDX_PORTFOLIO_ANALYTICS_DAILY`, `SC_IDX_PORTFOLIO_POSITION_DAILY`, and `SC_IDX_PORTFOLIO_OPT_INPUTS`
- it verifies that portfolio freshness reaches the current `SC_IDX_LEVELS` max trade date
- it runs only for the missing portfolio window, not for the full history on every timer tick

## Resume and terminal states

- Safe completed nodes are resumed by default using `SC_IDX_PIPELINE_STATE`.
- Force a fresh run:

```bash
python3 tools/index_engine/run_pipeline.py --restart
```

Terminal outcomes:

- `success`
- `success_with_degradation`
- `clean_skip`
- `failed`
- `blocked`

Oracle stores the compact terminal code in `SC_IDX_JOB_RUNS.STATUS`:

- `OK`
- `DEGRADED`
- `SKIP`
- `ERROR`
- `BLOCKED`

## Compatibility and operator flags

- Skip ingest but keep downstream DB-only stages when possible:

```bash
python3 tools/index_engine/run_pipeline.py --skip-ingest
```

- Back-compat skip-ingest env for manual operator runs:

```bash
SC_IDX_PIPELINE_SKIP_INGEST=1 python3 tools/index_engine/run_pipeline.py
```

- No-provider smoke path:

```bash
python3 tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

## Key env and guardrails

- Provider/API:
  - `SC_MARKET_DATA_API_KEY` or `MARKET_DATA_API_KEY`
  - `MARKET_DATA_API_BASE_URL`
- Oracle runtime:
  - systemd units load `/etc/sustainacore/db.env`, `/etc/sustainacore/index.env`, `/etc/sustainacore-ai/secrets.env`
  - systemd units set `TNS_ADMIN=/opt/adb_wallet`
- Oracle retry knobs:
  - `SC_IDX_ORACLE_RETRY_ATTEMPTS`
  - `SC_IDX_ORACLE_RETRY_BASE_SEC`
- Impute guardrails:
  - `SC_IDX_IMPUTE_LOOKBACK_DAYS`
  - `SC_IDX_IMPUTE_TIMEOUT_SEC`
- Imputed replacement guardrails:
  - `SC_IDX_IMPUTED_REPLACEMENT_DAYS`
  - `SC_IDX_IMPUTED_REPLACEMENT_LIMIT`
- Alert options:
  - `SC_IDX_EMAIL_ON_BUDGET_STOP`
  - `SC_IDX_EMAIL_ON_DEGRADED`

## Outputs

- Reports: `tools/audit/output/pipeline_runs/`
- Telemetry: `tools/audit/output/pipeline_telemetry/`
- Health snapshot: `tools/audit/output/pipeline_health_latest.txt`
- Oracle evidence on failures: `tools/audit/output/oracle_health_*.txt`

## Provider readiness probe

- Provider latest EOD is detected via the probe symbol on the latest trading day at or before today.
- Readiness uses a bounded fallback window and never loops forever.
- If none of the candidate dates is ready, the run exits `clean_skip` with `provider_not_ready`.
- The pipeline never writes future-date CANON or IMPUTED rows.

## Oracle preflight

- `tools/index_engine/run_pipeline.py` runs Oracle preflight before any provider/API work.
- If the wallet/env is broken, the run records a terminal `blocked` or `failed` outcome with a concrete remediation token.
- Always start VM1 diagnostics with:

```bash
python3 tools/oracle/preflight_oracle.py
```

## VM1 verification

Targeted test coverage:

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

Smoke path:

```bash
source .venv/bin/activate
python tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

Operational checks:

```bash
systemctl status sc-idx-pipeline.service
sudo journalctl -u sc-idx-pipeline.service -n 200 --no-pager
```

Portfolio freshness spot-check:

```sql
SELECT MAX(trade_date) FROM SC_IDX_LEVELS;
SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY;
SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_POSITION_DAILY;
```

See [SC_IDX LangGraph orchestration](index_engine_langgraph_orchestration.md) for the graph layout and persistence rules, and [TECH100 portfolio backend runbook](runbooks/tech100_portfolio_backend.md) for the portfolio stage details.

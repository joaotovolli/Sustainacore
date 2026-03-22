# SC_IDX daily pipeline cadence

## Primary VM1 flow

The primary VM1 operational path is now the LangGraph pipeline:

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
- report generation
- alert decisioning
- telemetry emission
- terminal status persistence

## Scheduling

Primary systemd schedule on VM1:

- ingest compatibility timer: `00:00`, `05:00`, `09:00`, `13:00` UTC
- primary LangGraph pipeline timer: `00:30`, `05:30`, `09:30`, `13:30` UTC

`sc-idx-pipeline.timer` is the documented control plane. The standalone ingest, completeness, and index-calc units remain compatibility tools and should not be treated as the primary orchestration path.

## Budget and readiness behavior

- Daily call budget is derived from `SC_IDX_JOB_RUNS` usage for `provider='MARKET_DATA'`.
- Budget envs:
  - `SC_IDX_MARKET_DATA_DAILY_LIMIT`
  - `SC_IDX_MARKET_DATA_DAILY_BUFFER`
  - `SC_IDX_MARKET_DATA_CREDIT_BUFFER` (back-compat alias)
- If the budget is exhausted before work begins, the run concludes `clean_skip` with reason `daily_budget_stop`.
- Provider readiness probes use a bounded fallback window and conclude `clean_skip` with reason `provider_not_ready` when no candidate date is ready.
- Mid-run budget exhaustion is recorded as a degraded outcome instead of looping forever.

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

- No-provider smoke path:

```bash
python3 tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

## Key env and guardrails

- Provider/API:
  - `SC_MARKET_DATA_API_KEY` or `MARKET_DATA_API_KEY`
  - `MARKET_DATA_API_BASE_URL`
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

See [SC_IDX LangGraph orchestration](index_engine_langgraph_orchestration.md) for the graph layout and persistence rules.

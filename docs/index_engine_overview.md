# SC_IDX / TECH100 index engine overview

## Current control plane

The SC_IDX / TECH100 operational pipeline now runs through a LangGraph orchestration layer on VM1.

- CLI entrypoint: `tools/index_engine/run_pipeline.py`
- Primary scheduler: `infra/systemd/sc-idx-pipeline.timer`
- Oracle-backed stage state: `SC_IDX_PIPELINE_STATE`
- Run summaries: `SC_IDX_JOB_RUNS`
- JSON/text reports: `tools/audit/output/pipeline_runs/`
- Structured telemetry snapshots: `tools/audit/output/pipeline_telemetry/`

LangGraph is the orchestrator, not the compute engine. Existing scripts still do the heavy work:

- ingest: `tools/index_engine/ingest_prices.py`
- provider/budget/readiness helpers: `tools/index_engine/run_daily.py`
- completeness: `tools/index_engine/check_price_completeness.py`
- imputation: `tools/index_engine/impute_missing_prices.py`
- index + statistics: `tools/index_engine/calc_index.py`
- portfolio analytics: `tools/index_engine/build_portfolio_analytics.py`

## Operational stages

The primary VM1 graph coordinates:

1. Oracle preflight
2. lock acquisition
3. target-date selection and budget planning
4. provider readiness probe
5. ingest
6. completeness check
7. imputation plus bounded replacement attempts
8. index and statistics calculation
9. portfolio analytics refresh
10. report generation
11. alert decisions
12. telemetry emission
13. terminal status persistence
14. lock release

## Data tables

- `SC_IDX_PRICES_RAW`
  - provider responses and missing/error rows
- `SC_IDX_PRICES_CANON`
  - canonical close and adjusted-close rows, including imputed rows when policy allows
- `SC_IDX_TRADING_DAYS`
  - explicit trading calendar used by ingest and calc
- `SC_IDX_IMPUTATIONS`
  - carry-forward imputation audit table
- `SC_IDX_LEVELS`
  - total-return index levels
- `SC_IDX_CONSTITUENT_DAILY`
  - daily holdings snapshot
- `SC_IDX_CONTRIBUTION_DAILY`
  - daily constituent contribution rows
- `SC_IDX_STATS_DAILY`
  - returns and volatility statistics
- `SC_IDX_PORTFOLIO_ANALYTICS_DAILY`
  - TECH100 portfolio KPI snapshots
- `SC_IDX_PORTFOLIO_POSITION_DAILY`
  - TECH100 model/benchmark holdings and attribution rows
- `SC_IDX_PORTFOLIO_OPT_INPUTS`
  - optimizer-ready portfolio signals
- `SC_IDX_PIPELINE_STATE`
  - LangGraph node-level durable state
- `SC_IDX_JOB_RUNS`
  - run-level summary and terminal status
- `SC_IDX_ALERT_STATE`
  - once-per-day alert suppression state

## Terminal outcomes

Each run must conclude as one of:

- `success`
- `success_with_degradation`
- `clean_skip`
- `failed`
- `blocked`

Oracle stores the compact status code in `SC_IDX_JOB_RUNS.STATUS`:

- `OK`
- `DEGRADED`
- `SKIP`
- `ERROR`
- `BLOCKED`

## Resource model

The design is constrained for VM1:

- one short-lived process per run
- no watch loop
- no background workers
- bounded retries only
- bounded provider readiness fallback
- no external queue or cache infrastructure

See [SC_IDX LangGraph orchestration](index_engine_langgraph_orchestration.md) for the detailed node and persistence model.

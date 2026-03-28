# SC_IDX LangGraph orchestration

## Why this exists

The SC_IDX / TECH100 operational pipeline now uses LangGraph as its control plane on VM1.

This change is not a new compute stack. The existing Oracle helpers, ingest CLI, completeness checks,
imputation logic, index calculation, systemd timers, and SMTP path stay in place. LangGraph only owns
the stateful orchestration between those steps so every run:

- has a stable `run_id`
- records node-level attempts, timings, counts, and outcomes
- distinguishes `success`, `success_with_degradation`, `clean_skip`, `failed`, and `blocked`
- always reaches a terminal conclusion
- can resume completed safe nodes without repeating them

This is designed for the VM1 constraints: 1 CPU core, about 1 GB RAM, no permanent daemon, no Redis,
no Celery, no watch loops, and no unbounded retries.

## Primary entrypoint

- Compatibility CLI: `python3 tools/index_engine/run_pipeline.py`
- Primary VM1 timer/service: `infra/systemd/sc-idx-pipeline.timer` -> `infra/systemd/sc-idx-pipeline.service`

The CLI remains the operator entrypoint. Internally it now builds and executes a LangGraph state graph.

## Graph shape

The current graph executes these nodes in order, with conditional terminal branches:

1. `preflight_oracle`
2. `acquire_lock`
3. `determine_target_dates`
4. `readiness_probe`
5. `ingest_prices`
6. `completeness_check`
7. `imputation_or_replacement`
8. `calc_index`
9. `portfolio_analytics`
10. `generate_run_report`
11. `decide_alerts`
12. `emit_telemetry`
13. `persist_terminal_status`
14. `release_lock`

Important routing behavior:

- `preflight_oracle` can end the run as `failed` or `blocked`
- `determine_target_dates` can end the run as `clean_skip` for `up_to_date` or `daily_budget_stop`
- `readiness_probe` can end the run as `clean_skip` for `provider_not_ready`
- `completeness_check` can degrade into `imputation_or_replacement` instead of failing immediately
- `imputation_or_replacement` rechecks completeness before `calc_index`
- `portfolio_analytics` refreshes additive TECH100 portfolio tables after index/statistics advance
- `generate_run_report`, `decide_alerts`, `emit_telemetry`, `persist_terminal_status`, and `release_lock`
  still run after failures so the run concludes cleanly

## Persistence model

LangGraph is the orchestrator, but the durability layer is repo-native and Oracle-friendly:

- `SC_IDX_PIPELINE_STATE`
  - node-level persisted state
  - stage status codes: `OK`, `DEGRADED`, `SKIP`, `FAILED`, `BLOCKED`
  - Oracle rows keep compact JSON details so node state does not overflow `VARCHAR2(4000)`
  - the full same-run payload is also written to `tools/audit/output/pipeline_state_latest.json`
    and is keyed by `run_id`, not just by UTC day
- `SC_IDX_JOB_RUNS`
  - run-level summary row
  - short terminal codes: `OK`, `DEGRADED`, `SKIP`, `ERROR`, `BLOCKED`
- `tools/audit/output/pipeline_runs/`
  - machine-readable JSON run reports
  - human-readable text run reports
- `tools/audit/output/pipeline_telemetry/`
  - structured operational telemetry snapshots

LangGraph’s built-in checkpoint backend is intentionally not used here because the existing Oracle state and
repo-local artifacts are lighter, already operationally accepted, and safer for the VM limits.

## Retry and timeout rules

- Oracle/transient node retries are bounded and stage-specific
- repeated identical failures stop retrying and branch to a terminal outcome
- provider readiness uses a bounded fallback window instead of polling forever
- imputation uses `SC_IDX_IMPUTE_TIMEOUT_SEC`
- provider/API calls stay bounded by the existing provider client and budget controls
- systemd remains the outer hard stop via `RuntimeMaxSec`

## Terminal outcomes

- `success`
  - the target window completed without degradations
- `success_with_degradation`
  - the run completed, but with cached calendar usage, imputation, budget partials, or similar bounded degradation
- `clean_skip`
  - nothing safe to do yet, such as `up_to_date`, `provider_not_ready`, or `daily_budget_stop`
- `failed`
  - bounded retries were exhausted or a non-blocking operational failure remained
- `blocked`
  - a user or operator action is required, such as Oracle wallet/env breakage or lock contention

## Reporting outputs

Each run writes:

- JSON: `tools/audit/output/pipeline_runs/sc_idx_pipeline_<RUN_ID>.json`
- text: `tools/audit/output/pipeline_runs/sc_idx_pipeline_<RUN_ID>.txt`
- latest JSON pointer: `tools/audit/output/pipeline_runs/sc_idx_pipeline_latest.json`
- latest text pointer: `tools/audit/output/pipeline_runs/sc_idx_pipeline_latest.txt`
- telemetry JSON: `tools/audit/output/pipeline_telemetry/sc_idx_pipeline_<RUN_ID>.json`

The report includes:

- terminal status
- overall operator health verdict (`Healthy`, `Degraded`, `Failed`, `Blocked`, `Stale`, `Skipped`)
- node-by-node status
- latest data date
- expected target date and source
- latest complete downstream date
- impacted date range
- provider/readiness budget data
- retry counts
- ingest/imputation/index/statistics counts
- portfolio analytics counts
- alert decision
- SMTP delivery state and message ID when email is attempted
- stale signals and freshness lag by key table
- deployed `repo_root` and `repo_head`
- root cause token
- next remediation step

Alert rules:

- `failed` and `blocked` attempt email by default
- `stale` attempts email by default, even if the graph technically concluded
- repeated `success_with_degradation` attempts email by default after
  `SC_IDX_ALERT_DEGRADED_REPEAT_THRESHOLD` consecutive degraded runs
- single `success_with_degradation` only attempts email when `SC_IDX_EMAIL_ON_DEGRADED=1`
- `daily_budget_stop` only attempts email when `SC_IDX_EMAIL_ON_BUDGET_STOP=1`
- `clean_skip` only stays quiet when freshness is not stale
- smoke runs do not send email
- the once-per-day gate in `SC_IDX_ALERT_STATE` is only marked after successful SMTP delivery

Stale detection:

- The graph does not treat a completed control flow as equivalent to a healthy data outcome.
- A run/report becomes `Stale` when:
  - `SC_IDX_PRICES_CANON` advances ahead of `SC_IDX_LEVELS`
  - `SC_IDX_LEVELS` advances ahead of `SC_IDX_STATS_DAILY`
  - `SC_IDX_LEVELS` advances ahead of `SC_IDX_PORTFOLIO_ANALYTICS_DAILY` or
    `SC_IDX_PORTFOLIO_POSITION_DAILY`
  - the latest complete downstream date lags the expected target date by more than
    `SC_IDX_STALE_ALLOWED_LAG_DAYS`
- When trading-day refresh degrades on a persistent provider timeout or 403, the planner can use a
  bounded weekday fallback (`SC_IDX_TRADING_DAY_FALLBACK_MAX_GAP`) to avoid masking a one-day stale
  calendar table behind an incorrect `clean_skip`.
- LangGraph coordinates retries, routing, and reporting; it does not magically fix a VM1 checkout
  checkout, a broken provider, or a stale Oracle table unless a stage explicitly detects and surfaces
  that condition.

The daily operator digest is a separate bounded script:

```bash
python tools/index_engine/daily_telemetry_report.py --send
```

It reads the latest pipeline report plus telemetry snapshot, optionally enriches with lightweight Oracle
freshness checks, and writes artifacts under `tools/audit/output/pipeline_daily/`.

## Verification commands

Smoke path, no provider burn:

```bash
source .venv/bin/activate
python tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

Targeted unit coverage:

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

VM1 operator run:

```bash
python3 tools/oracle/preflight_oracle.py
python3 tools/index_engine/run_pipeline.py --restart
```

Expected signals:

- text report exists under `tools/audit/output/pipeline_runs/`
- telemetry JSON exists under `tools/audit/output/pipeline_telemetry/`
- `SC_IDX_PIPELINE_STATE` contains node rows for the run
- `SC_IDX_JOB_RUNS` latest `sc_idx_pipeline` row shows `OK`, `DEGRADED`, `SKIP`, `ERROR`, or `BLOCKED`
- the latest report exposes `expected_target_date`, `latest_complete_date`, stale signals, and
  deployed `repo_root` / `repo_head`

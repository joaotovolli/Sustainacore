# TECH100 portfolio backend runbook
<!-- cspell:ignore herfindahl LOWVOL -->

This runbook covers the Oracle-backed portfolio analytics layer for the future `/tech100/portfolio/` page.

## What this builds

The refresh job reads only from existing TECH100/SC_IDX sources:

- `SC_IDX_LEVELS`
- `SC_IDX_STATS_DAILY`
- `SC_IDX_CONSTITUENT_DAILY`
- `SC_IDX_CONTRIBUTION_DAILY`
- `TECH11_AI_GOV_ETH_INDEX`
- `SC_IDX_PRICES_CANON`

It writes additive objects only:

- `SC_IDX_PORTFOLIO_ANALYTICS_DAILY`
- `SC_IDX_PORTFOLIO_POSITION_DAILY`
- `SC_IDX_PORTFOLIO_OPT_INPUTS`
- `SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS`
- `SC_IDX_PORTFOLIO_SECTOR_DAILY_V`

## Supported now

- Official TECH100 daily portfolio snapshot and KPI history.
- Model portfolio comparisons for:
  - `TECH100`
  - `TECH100_EQ`
  - `TECH100_GOV`
  - `TECH100_MOM`
  - `TECH100_LOWVOL`
  - `TECH100_GOV_MOM`
- Rolling returns, 20d/60d volatility, current drawdown, and trailing 252-day max drawdown.
- Holdings snapshots with active weight vs official TECH100.
- Attribution windows for 1d, 5d, 20d, MTD, and YTD.
- Governance summaries from `AIGES_COMPOSITE_AVERAGE` and the existing TECH100 pillar scores.
- Sector weight / sector contribution views when `GICS_SECTOR` is populated.
- Data-quality visibility via `PRICE_QUALITY` / imputed-row counts.
- Optimizer-ready signal inputs and model constraints tables.

## Explicitly deferred

These are intentionally not produced because the current Oracle sources do not prove them:

- Classical value.
- Quality as a fundamentals factor.
- Dividend yield.
- Small cap.
- Fundamentally weighted portfolios.
- A live optimizer / solver running on VM1.

The current backend stays honest by exposing only governance, momentum, low volatility, concentration, and sector tilt.

## Refresh commands

Always start with Oracle preflight:

```bash
python3 tools/oracle/preflight_oracle.py
```

If the VM1 Oracle env files are only readable through systemd, run the branch from an
isolated worktree with the service-like context instead of sourcing env files:

```bash
sudo -n systemd-run --wait --collect --pipe --unit=tech100-portfolio-preflight \
  -p WorkingDirectory=/home/opc/Sustainacore-tech100-portfolio \
  -p User=opc \
  -p Group=opc \
  -p Environment=PYTHONPATH=/home/opc/Sustainacore-tech100-portfolio \
  -p EnvironmentFile=/etc/sustainacore/db.env \
  -p EnvironmentFile=/etc/sustainacore/index.env \
  -p EnvironmentFile=/etc/sustainacore-ai/app.env \
  -p EnvironmentFile=/etc/sustainacore-ai/secrets.env \
  -- /home/opc/Sustainacore/.venv/bin/python tools/oracle/preflight_oracle.py
```

Apply DDL and dry-run the latest window:

```bash
python3 tools/index_engine/build_portfolio_analytics.py --apply-ddl --dry-run --start 2026-03-06 --end 2026-03-06
```

Run a real small-scope refresh:

```bash
python3 tools/index_engine/build_portfolio_analytics.py --apply-ddl --start 2026-03-06 --end 2026-03-06
```

Rebuild the full available history:

```bash
python3 tools/index_engine/build_portfolio_analytics.py --apply-ddl
```

VM1 service-like full refresh:

```bash
sudo -n systemd-run --wait --collect --pipe --unit=tech100-portfolio-refresh \
  -p WorkingDirectory=/home/opc/Sustainacore-tech100-portfolio \
  -p User=opc \
  -p Group=opc \
  -p Environment=PYTHONPATH=/home/opc/Sustainacore-tech100-portfolio \
  -p EnvironmentFile=/etc/sustainacore/db.env \
  -p EnvironmentFile=/etc/sustainacore/index.env \
  -p EnvironmentFile=/etc/sustainacore-ai/app.env \
  -p EnvironmentFile=/etc/sustainacore-ai/secrets.env \
  -- /home/opc/Sustainacore/.venv/bin/python tools/index_engine/build_portfolio_analytics.py --apply-ddl
```

## Verification SQL

Latest portfolio analytics date and model coverage:

```sql
SELECT MAX(trade_date) AS max_trade_date,
       COUNT(DISTINCT model_code) AS models_on_latest_day
FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY
WHERE trade_date = (SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY);
```

Latest daily summaries:

```sql
SELECT model_code, level_tr, ret_1d, ret_20d, ret_ytd, vol_20d, vol_60d,
       n_constituents, n_imputed, top5_weight, herfindahl
FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY
WHERE trade_date = (SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY)
ORDER BY model_code;
```

Latest holdings / attribution rows:

```sql
SELECT model_code, ticker, model_weight, benchmark_weight, active_weight,
       contrib_1d, contrib_20d, contrib_mtd, contrib_ytd, governance_score, momentum_20d, low_vol_60d
FROM SC_IDX_PORTFOLIO_POSITION_DAILY
WHERE trade_date = (SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_POSITION_DAILY)
ORDER BY model_code, model_weight DESC, ticker;
```

Latest sector weights / sector attribution:

```sql
SELECT model_code, sector, sector_weight, benchmark_sector_weight, active_sector_weight,
       contrib_1d, contrib_20d, contrib_mtd, contrib_ytd
FROM SC_IDX_PORTFOLIO_SECTOR_DAILY_V
WHERE trade_date = (SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY)
ORDER BY model_code, sector_weight DESC, sector;
```

Latest optimizer inputs:

```sql
SELECT trade_date, ticker, benchmark_weight, governance_score, momentum_20d, low_vol_60d,
       governance_rank, momentum_rank, low_vol_rank, hybrid_rank, price_quality, eligible_flag
FROM SC_IDX_PORTFOLIO_OPT_INPUTS
WHERE trade_date = (SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_OPT_INPUTS)
ORDER BY benchmark_weight DESC, ticker;
```

Constraint rows:

```sql
SELECT model_code, constraint_key, constraint_type, constraint_value
FROM SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS
ORDER BY model_code, constraint_key;
```

## Pipeline integration

`tools/index_engine/run_pipeline.py` now runs a `portfolio_analytics` stage after `calc_index`.

The stage:

- Creates the additive DDL when missing.
- Rebuilds the portfolio tables for the requested write window.
- Verifies that `SC_IDX_PORTFOLIO_ANALYTICS_DAILY` advanced to the current `SC_IDX_LEVELS` max trade date.

## Rollback

Schema rollback:

```bash
python3 - <<'PY'
from pathlib import Path
from app.index_engine.db_portfolio_analytics import apply_ddl, DROP_DDL_PATH

apply_ddl(DROP_DDL_PATH)
PY
```

Operational rollback steps:

1. Revert the Python changes that invoke `build_portfolio_analytics.py` from the pipeline.
2. Apply `oracle_scripts/sc_idx_portfolio_analytics_v1_drop.sql`.
3. Re-run the normal SC_IDX pipeline health checks and confirm existing `SC_IDX_*` objects still advance normally.

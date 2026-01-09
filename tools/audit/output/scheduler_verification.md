# Scheduler verification (VM1)

Generated: 2026-01-09T08:30:00.112074Z

## Manual pipeline runs (systemd-run, User=opc)
- No-ingest run: `SC_IDX_PIPELINE_SKIP_INGEST=1 python3 tools/index_engine/run_pipeline.py --restart` -> SUCCESS.
- Normal run: `python3 tools/index_engine/run_pipeline.py --resume` -> resumed prior run (all stages already OK).

## Latest SC_IDX_JOB_RUNS (sc_idx_pipeline)
- run_id=5461d3c1-c0e7-49d8-8d3f-637e71b6cb43
- status=OK
- started_at=2026-01-09 08:28:21.296775
- ended_at=2026-01-09 08:28:53.813222
- error_msg=calendar_max_date=2026-01-08
canon_max_date=2026-01-08
canon_count_latest_day=105
levels_max_date=2026-01-08
level_latest=1093.159863
stats_max_date=2026-01-08
ret_1d_latest=-0.004675610548128595
contrib_max_date=2026-01-08
contrib_count_latest_day=25
next_missing_trading_day=None
oracle_error_counts_24h=24
last_error=None
stage_duration_completeness_check_sec=3.65
stage_duration_impute_sec=0.79
stage_duration_ingest_prices_sec=0.26
stage_duration_calc_index_sec=6.65
stage_duration_update_trading_days_sec=0.27

## Max trade_date per table
- SC_IDX_TRADING_DAYS max_trade_date=2026-01-08 00:00:00
- SC_IDX_PRICES_CANON max_trade_date=2026-01-08 00:00:00
- SC_IDX_LEVELS max_trade_date=2026-01-08 00:00:00
- SC_IDX_STATS_DAILY max_trade_date=2026-01-08 00:00:00

## Pipeline health artifact
- path=/home/opc/Sustainacore/tools/audit/output/pipeline_health_latest.txt
- updated_utc=2026-01-09T08:28:53.744745Z
- last_error=None

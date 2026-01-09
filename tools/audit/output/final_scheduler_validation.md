# Final scheduler validation (VM1)

Generated: 2026-01-09T09:24:54.956678Z

## Unit definitions (sc-idx-pipeline)
```ini
# /etc/systemd/system/sc-idx-pipeline.service
[Unit]
Description=SC_IDX pipeline (ingest + completeness + impute + index calc)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=opc
WorkingDirectory=/home/opc/Sustainacore
Environment=PYTHONPATH=/home/opc/Sustainacore
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/etc/sustainacore/db.env
EnvironmentFile=/etc/sustainacore/index.env
EnvironmentFile=/etc/sustainacore-ai/secrets.env
ExecStartPre=/usr/bin/test -r /etc/sustainacore/db.env
ExecStartPre=/usr/bin/test -r /etc/sustainacore/index.env
ExecStartPre=/usr/bin/test -r /etc/sustainacore-ai/secrets.env
ExecStartPre=/bin/ls -l /etc/sustainacore/db.env /etc/sustainacore/index.env /etc/sustainacore-ai/secrets.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore/db.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore/index.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore-ai/secrets.env
ExecStart=/usr/bin/flock -n /tmp/sc_idx_pipeline.lock /usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_pipeline.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/sc-idx-pipeline.timer
[Unit]
Description=Run SC_IDX pipeline every 6 hours (UTC)

[Timer]
OnCalendar=*-*-* 00:30:00 UTC
OnCalendar=*-*-* 05:30:00 UTC
Persistent=true
Unit=sc-idx-pipeline.service

[Install]
WantedBy=timers.target
```

## Systemd environment visibility
```
Environment=PYTHONPATH=/home/opc/Sustainacore PYTHONUNBUFFERED=1
EnvironmentFiles=/etc/sustainacore/db.env (ignore_errors=no)
EnvironmentFiles=/etc/sustainacore/index.env (ignore_errors=no)
EnvironmentFiles=/etc/sustainacore-ai/secrets.env (ignore_errors=no)
```

## Env file presence
```text
-rw-r-----. 1 root opc 722 Dec 25 10:39 /etc/sustainacore-ai/secrets.env
-rw-r-----. 1 root opc 387 Dec 25 10:39 /etc/sustainacore/db.env
-rw-r-----. 1 root opc  87 Jan  9 08:23 /etc/sustainacore/index.env
MARKET_DATA_API_BASE_URL=https://api.twelvedata.com
```

## Scheduler next triggers
```
Sat 2026-01-10 00:00:00 GMT 14h left      Fri 2026-01-09 09:13:53 GMT 11min ago    sc-idx-price-ingest.timer       sc-idx-price-ingest.service
Sat 2026-01-10 00:30:00 GMT 15h left      Fri 2026-01-09 05:30:15 GMT 3h 54min ago sc-idx-pipeline.timer           sc-idx-pipeline.service
```

## Repo checkouts
- /home/opc/Sustainacore: 5831596 Fix SC_IDX scheduler env loading (58315966671b1cc5e534f6909e6838916260421a)
- /opt/sustainacore-ai: 2793a08 Add auth request/verify code endpoints (2793a082abd717b476417c370d808124b4c8c3c1)

## Systemd run evidence (journal excerpt)
```text
Jan 09 09:22:42 esg-project2 systemd[1]: Starting SC_IDX pipeline (ingest + completeness + impute + index calc)...
Jan 09 09:22:42 esg-project2 ls[2139057]: -rw-r-----. 1 root opc 722 Dec 25 10:39 /etc/sustainacore-ai/secrets.env
Jan 09 09:22:42 esg-project2 ls[2139057]: -rw-r-----. 1 root opc 387 Dec 25 10:39 /etc/sustainacore/db.env
Jan 09 09:22:42 esg-project2 ls[2139057]: -rw-r-----. 1 root opc  87 Jan  9 08:23 /etc/sustainacore/index.env
Jan 09 09:22:42 esg-project2 namei[2139058]: f: /etc/sustainacore/db.env
Jan 09 09:22:42 esg-project2 namei[2139058]: dr-xr-xr-x root root /
Jan 09 09:22:42 esg-project2 namei[2139058]: drwxr-xr-x root root etc
Jan 09 09:22:42 esg-project2 namei[2139058]: drwxr-x--- root opc  sustainacore
Jan 09 09:22:42 esg-project2 namei[2139058]: -rw-r----- root opc  db.env
Jan 09 09:22:42 esg-project2 namei[2139059]: f: /etc/sustainacore/index.env
Jan 09 09:22:42 esg-project2 namei[2139059]: dr-xr-xr-x root root /
Jan 09 09:22:42 esg-project2 namei[2139059]: drwxr-xr-x root root etc
Jan 09 09:22:42 esg-project2 namei[2139059]: drwxr-x--- root opc  sustainacore
Jan 09 09:22:42 esg-project2 namei[2139059]: -rw-r----- root opc  index.env
Jan 09 09:22:42 esg-project2 namei[2139060]: f: /etc/sustainacore-ai/secrets.env
Jan 09 09:22:42 esg-project2 namei[2139060]: dr-xr-xr-x root root /
Jan 09 09:22:42 esg-project2 namei[2139060]: drwxr-xr-x root root etc
Jan 09 09:22:42 esg-project2 namei[2139060]: drwxr-x--- root opc  sustainacore-ai
Jan 09 09:22:42 esg-project2 namei[2139060]: -rw-r----- root opc  secrets.env
Jan 09 09:22:42 esg-project2 systemd[1]: Started SC_IDX pipeline (ingest + completeness + impute + index calc).
Jan 09 09:22:42 esg-project2 flock[2139065]: sc_idx_pipeline_run: started_at_utc=2026-01-09T09:22:42Z head=5831596 skip_ingest=False resume=True
Jan 09 09:22:43 esg-project2 flock[2139065]: [pipeline] start update_trading_days args=[]
Jan 09 09:22:43 esg-project2 flock[2139065]: [pipeline] end update_trading_days exit=0 duration_sec=0.2
Jan 09 09:22:44 esg-project2 flock[2139065]: [pipeline] start ingest_prices args=['--debug']
Jan 09 09:22:45 esg-project2 flock[2139065]: index_engine_daily: end=2026-01-08 calls_used_today=0 remaining_daily=800 daily_limit=800 daily_buffer=25 max_provider_calls=775 minute_limit=8 minute_used=2
Jan 09 09:22:49 esg-project2 flock[2139065]: backfill_window: start=2026-01-08 end=2026-01-08 trading_days=1 tickers=25
Jan 09 09:23:04 esg-project2 flock[2139065]: debug: dates=2 total_unique_tickers=1 backfill=True
Jan 09 09:23:04 esg-project2 flock[2139065]: debug: oracle_user=WKSP_ESGAPEX
Jan 09 09:23:04 esg-project2 flock[2139065]: debug: provider_called=True provider_rows=1 raw_rows=1 canon_rows=1 provider_calls_used=1
Jan 09 09:23:04 esg-project2 flock[2139065]: backfill_summary: raw_upserts=1 canon_upserts=1 provider_calls_used=1
Jan 09 09:23:07 esg-project2 flock[2139065]: [pipeline] end ingest_prices exit=0 duration_sec=23.4
Jan 09 09:23:07 esg-project2 flock[2139065]: [pipeline] start completeness_check args=[]
Jan 09 09:23:08 esg-project2 flock[2139065]: [pipeline] end completeness_check exit=0 duration_sec=0.4
Jan 09 09:23:08 esg-project2 flock[2139065]: [pipeline] start calc_index args=[]
Jan 09 09:23:09 esg-project2 flock[2139065]: [pipeline] end calc_index exit=0 duration_sec=0.3
Jan 09 09:23:09 esg-project2 flock[2139065]: [pipeline] start impute args=[]
Jan 09 09:23:12 esg-project2 flock[2139065]: [pipeline] end impute exit=0 duration_sec=3.4
Jan 09 09:23:17 esg-project2 flock[2139065]: [pipeline] DONE
Jan 09 09:23:17 esg-project2 systemd[1]: sc-idx-pipeline.service: Deactivated successfully.
Jan 09 09:23:17 esg-project2 systemd[1]: sc-idx-pipeline.service: Consumed 1.333s CPU time.
```

## Pipeline health snapshot
```text
calendar_max_date=2026-01-08
canon_max_date=2026-01-08
canon_count_latest_day=105
levels_max_date=2026-01-08
level_latest=1093.159863
stats_max_date=2026-01-08
ret_1d_latest=-0.004675610548128595
contrib_max_date=2026-01-08
contrib_count_latest_day=25
next_missing_trading_day=None
oracle_error_counts_24h=17
last_error=None
stage_duration_update_trading_days_sec=0.48
stage_duration_ingest_prices_sec=23.67
stage_duration_completeness_check_sec=0.59
stage_duration_calc_index_sec=0.51
stage_duration_impute_sec=3.61
```

## Oracle evidence
### Latest SC_IDX_JOB_RUNS (sc_idx_pipeline)
```text
run_id=e6261fe8-f0ab-49b0-954d-4e8656c39fc6
status=OK
started_at=2026-01-09 09:22:43.440498
ended_at=2026-01-09 09:23:17.384961
error_msg=calendar_max_date=2026-01-08
canon_max_date=2026-01-08
canon_count_latest_day=105
levels_max_date=2026-01-08
level_latest=1093.159863
stats_max_date=2026-01-08
ret_1d_latest=-0.004675610548128595
contrib_max_date=2026-01-08
contrib_count_latest_day=25
next_missing_trading_day=None
oracle_error_counts_24h=17
last_error=None
stage_duration_update_trading_days_sec=0.48
stage_duration_ingest_prices_sec=23.67
stage_duration_completeness_check_sec=0.59
stage_duration_calc_index_sec=0.51
stage_duration_impute_sec=3.61
```

### Max trade_date per table
```text
SC_IDX_TRADING_DAYS: 2026-01-08 00:00:00
SC_IDX_PRICES_CANON: 2026-01-08 00:00:00
SC_IDX_LEVELS: 2026-01-08 00:00:00
SC_IDX_STATS_DAILY: 2026-01-08 00:00:00
SC_IDX_CONTRIBUTION_DAILY: 2026-01-08 00:00:00
SC_IDX_CONTRIBUTION_DAILY latest_count=25 for 2026-01-08 00:00:00
```

## Conclusion
PASS: systemd now runs the /home/opc/Sustainacore checkout with valid timers; sc-idx-pipeline.service completed end-to-end with status=0 and produced a clean health snapshot.

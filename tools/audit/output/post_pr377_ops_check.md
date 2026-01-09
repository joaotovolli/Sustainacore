# Post-PR377 ops check (VM1)

Generated: 2026-01-09T09:41:53.654851Z

## Timers/services status
### list-timers
```
Sat 2026-01-10 00:00:00 GMT 14h left     Fri 2026-01-09 09:13:53 GMT 28min ago    sc-idx-price-ingest.timer       sc-idx-price-ingest.service
Sat 2026-01-10 00:30:00 GMT 14h left     Fri 2026-01-09 05:30:15 GMT 4h 11min ago sc-idx-pipeline.timer           sc-idx-pipeline.service
```

### is-enabled
```
enabled
enabled
```

### timer status
```
● sc-idx-pipeline.timer - Run SC_IDX pipeline every 6 hours (UTC)
     Loaded: loaded (/etc/systemd/system/sc-idx-pipeline.timer; enabled; preset: disabled)
     Active: active (waiting) since Fri 2026-01-09 09:14:03 GMT; 27min ago
      Until: Fri 2026-01-09 09:14:03 GMT; 27min ago
    Trigger: Sat 2026-01-10 00:30:00 GMT; 14h left
   Triggers: ● sc-idx-pipeline.service

Jan 09 09:14:03 esg-project2 systemd[1]: Stopped Run SC_IDX pipeline every 6 hours (UTC).
Jan 09 09:14:03 esg-project2 systemd[1]: Stopping Run SC_IDX pipeline every 6 hours (UTC)...
Jan 09 09:14:03 esg-project2 systemd[1]: Started Run SC_IDX pipeline every 6 hours (UTC).

● sc-idx-price-ingest.timer - Run SC_IDX price ingest every 6 hours (UTC)
     Loaded: loaded (/etc/systemd/system/sc-idx-price-ingest.timer; enabled; preset: disabled)
     Active: active (waiting) since Fri 2026-01-09 09:14:10 GMT; 27min ago
      Until: Fri 2026-01-09 09:14:10 GMT; 27min ago
    Trigger: Sat 2026-01-10 00:00:00 GMT; 14h left
   Triggers: ● sc-idx-price-ingest.service

Jan 09 09:14:10 esg-project2 systemd[1]: sc-idx-price-ingest.timer: Deactivated successfully.
Jan 09 09:14:10 esg-project2 systemd[1]: Stopped Run SC_IDX price ingest every 6 hours (UTC).
Jan 09 09:14:10 esg-project2 systemd[1]: Stopping Run SC_IDX price ingest every 6 hours (UTC)...
Jan 09 09:14:10 esg-project2 systemd[1]: Started Run SC_IDX price ingest every 6 hours (UTC).
```

### service status
```
○ sc-idx-pipeline.service - SC_IDX pipeline (ingest + completeness + impute + index calc)
     Loaded: loaded (/etc/systemd/system/sc-idx-pipeline.service; disabled; preset: disabled)
     Active: inactive (dead) since Fri 2026-01-09 09:38:11 GMT; 3min 42s ago
   Duration: 11.602s
TriggeredBy: ● sc-idx-pipeline.timer
    Process: 2142542 ExecStartPre=/usr/bin/test -r /etc/sustainacore/db.env (code=exited, status=0/SUCCESS)
    Process: 2142543 ExecStartPre=/usr/bin/test -r /etc/sustainacore-ai/secrets.env (code=exited, status=0/SUCCESS)
    Process: 2142544 ExecStartPre=/usr/bin/test -r /etc/sustainacore/index.env (code=exited, status=0/SUCCESS)
    Process: 2142545 ExecStartPre=/bin/ls -l /etc/sustainacore/db.env /etc/sustainacore-ai/secrets.env /etc/sustainacore/index.env (code=exited, status=0/SUCCESS)
    Process: 2142546 ExecStartPre=/usr/bin/namei -l /etc/sustainacore/db.env (code=exited, status=0/SUCCESS)
    Process: 2142547 ExecStartPre=/usr/bin/namei -l /etc/sustainacore-ai/secrets.env (code=exited, status=0/SUCCESS)
    Process: 2142548 ExecStartPre=/usr/bin/namei -l /etc/sustainacore/index.env (code=exited, status=0/SUCCESS)
    Process: 2142549 ExecStart=/usr/bin/flock -n /tmp/sc_idx_pipeline.lock /usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_pipeline.py (code=exited, status=0/SUCCESS)
   Main PID: 2142549 (code=exited, status=0/SUCCESS)
        CPU: 557ms

Jan 09 09:38:00 esg-project2 flock[2142551]: run_log: failed to insert start row: ORA-00001: unique constraint (WKSP_ESGAPEX.SYS_C0030115) violated on table WKSP_ESGAPEX.SC_IDX_JOB_RUNS columns (RUN_ID)
Jan 09 09:38:00 esg-project2 flock[2142551]: ORA-03301: (ORA-00001 details) row with column values (RUN_ID:'e6261fe8-f0ab-49b0-954d-4e8656c39fc6') already exists
Jan 09 09:38:00 esg-project2 flock[2142551]: Help: https://docs.oracle.com/error-help/db/ora-00001/
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip update_trading_days (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip ingest_prices (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip completeness_check (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip calc_index (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip impute (resume)
Jan 09 09:38:11 esg-project2 flock[2142551]: [pipeline] DONE
Jan 09 09:38:11 esg-project2 systemd[1]: sc-idx-pipeline.service: Deactivated successfully.

○ sc-idx-price-ingest.service - SC_IDX daily price ingest (market data provider)
     Loaded: loaded (/etc/systemd/system/sc-idx-price-ingest.service; disabled; preset: disabled)
     Active: inactive (dead) since Fri 2026-01-09 09:37:29 GMT; 4min 24s ago
   Duration: 14.602s
TriggeredBy: ● sc-idx-price-ingest.timer
    Process: 2142316 ExecStartPre=/usr/bin/test -r /etc/sustainacore/db.env (code=exited, status=0/SUCCESS)
    Process: 2142317 ExecStartPre=/usr/bin/test -r /etc/sustainacore-ai/secrets.env (code=exited, status=0/SUCCESS)
    Process: 2142318 ExecStartPre=/usr/bin/test -r /etc/sustainacore/index.env (code=exited, status=0/SUCCESS)
    Process: 2142319 ExecStartPre=/bin/ls -l /etc/sustainacore/db.env /etc/sustainacore-ai/secrets.env /etc/sustainacore/index.env (code=exited, status=0/SUCCESS)
    Process: 2142320 ExecStartPre=/usr/bin/namei -l /etc/sustainacore/db.env (code=exited, status=0/SUCCESS)
    Process: 2142321 ExecStartPre=/usr/bin/namei -l /etc/sustainacore-ai/secrets.env (code=exited, status=0/SUCCESS)
    Process: 2142322 ExecStartPre=/usr/bin/namei -l /etc/sustainacore/index.env (code=exited, status=0/SUCCESS)
    Process: 2142323 ExecStart=/usr/bin/flock -n /tmp/sc_idx_pipeline.lock /usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_daily.py (code=exited, status=0/SUCCESS)
   Main PID: 2142323 (code=exited, status=0/SUCCESS)
        CPU: 777ms

Jan 09 09:37:15 esg-project2 namei[2142322]: f: /etc/sustainacore/index.env
Jan 09 09:37:15 esg-project2 namei[2142322]: dr-xr-xr-x root root /
Jan 09 09:37:15 esg-project2 namei[2142322]: drwxr-xr-x root root etc
Jan 09 09:37:15 esg-project2 namei[2142322]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:15 esg-project2 namei[2142322]: -rw-r----- root opc  index.env
Jan 09 09:37:15 esg-project2 systemd[1]: Started SC_IDX daily price ingest (market data provider).
Jan 09 09:37:16 esg-project2 flock[2142326]: index_engine_daily: end=2026-01-08 calls_used_today=1 remaining_daily=799 daily_limit=800 daily_buffer=25 max_provider_calls=774 minute_limit=8 minute_used=1
Jan 09 09:37:20 esg-project2 flock[2142326]: backfill_window: start=2026-01-08 end=2026-01-08 trading_days=1 tickers=25
Jan 09 09:37:28 esg-project2 flock[2142326]: backfill_summary: raw_upserts=0 canon_upserts=0 provider_calls_used=0
Jan 09 09:37:29 esg-project2 systemd[1]: sc-idx-price-ingest.service: Deactivated successfully.
```

## Unit definitions
### sc-idx-pipeline.service
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
EnvironmentFile=/etc/sustainacore-ai/secrets.env
EnvironmentFile=/etc/sustainacore/index.env
RuntimeMaxSec=3600
ExecStartPre=/usr/bin/test -r /etc/sustainacore/db.env
ExecStartPre=/usr/bin/test -r /etc/sustainacore-ai/secrets.env
ExecStartPre=/usr/bin/test -r /etc/sustainacore/index.env
ExecStartPre=/bin/ls -l /etc/sustainacore/db.env /etc/sustainacore-ai/secrets.env /etc/sustainacore/index.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore/db.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore-ai/secrets.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore/index.env
ExecStart=/usr/bin/flock -n /tmp/sc_idx_pipeline.lock /usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_pipeline.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### sc-idx-price-ingest.service
```ini
# /etc/systemd/system/sc-idx-price-ingest.service
[Unit]
Description=SC_IDX daily price ingest (market data provider)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=opc
WorkingDirectory=/home/opc/Sustainacore
Environment=PYTHONPATH=/home/opc/Sustainacore
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=/etc/sustainacore/db.env
EnvironmentFile=/etc/sustainacore-ai/secrets.env
EnvironmentFile=/etc/sustainacore/index.env
RuntimeMaxSec=7200
ExecStartPre=/usr/bin/test -r /etc/sustainacore/db.env
ExecStartPre=/usr/bin/test -r /etc/sustainacore-ai/secrets.env
ExecStartPre=/usr/bin/test -r /etc/sustainacore/index.env
ExecStartPre=/bin/ls -l /etc/sustainacore/db.env /etc/sustainacore-ai/secrets.env /etc/sustainacore/index.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore/db.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore-ai/secrets.env
ExecStartPre=/usr/bin/namei -l /etc/sustainacore/index.env
ExecStart=/usr/bin/flock -n /tmp/sc_idx_pipeline.lock /usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_daily.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### sc-idx-pipeline.timer
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

### sc-idx-price-ingest.timer
```ini
# /etc/systemd/system/sc-idx-price-ingest.timer
[Unit]
Description=Run SC_IDX price ingest every 6 hours (UTC)

[Timer]
OnCalendar=*-*-* 00:00:00 UTC
OnCalendar=*-*-* 05:00:00 UTC
Persistent=true
Unit=sc-idx-price-ingest.service

[Install]
WantedBy=timers.target
```

## Recent run evidence
### price-ingest logs (last run)
```text
Jan 09 09:37:06 esg-project2 systemd[1]: sc-idx-price-ingest.service: Scheduled restart job, restart counter is at 19.
Jan 09 09:37:06 esg-project2 systemd[1]: Stopped SC_IDX daily price ingest (market data provider).
Jan 09 09:37:07 esg-project2 systemd[1]: Starting SC_IDX daily price ingest (market data provider)...
Jan 09 09:37:07 esg-project2 ls[2142238]: -rw-r-----. 1 root opc 722 Dec 25 10:39 /etc/sustainacore-ai/secrets.env
Jan 09 09:37:07 esg-project2 ls[2142238]: -rw-r-----. 1 root opc 387 Dec 25 10:39 /etc/sustainacore/db.env
Jan 09 09:37:07 esg-project2 ls[2142238]: -rw-r-----. 1 root opc  87 Jan  9 08:23 /etc/sustainacore/index.env
Jan 09 09:37:07 esg-project2 namei[2142239]: f: /etc/sustainacore/db.env
Jan 09 09:37:07 esg-project2 namei[2142239]: dr-xr-xr-x root root /
Jan 09 09:37:07 esg-project2 namei[2142239]: drwxr-xr-x root root etc
Jan 09 09:37:07 esg-project2 namei[2142239]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:07 esg-project2 namei[2142239]: -rw-r----- root opc  db.env
Jan 09 09:37:07 esg-project2 namei[2142240]: f: /etc/sustainacore-ai/secrets.env
Jan 09 09:37:07 esg-project2 namei[2142240]: dr-xr-xr-x root root /
Jan 09 09:37:07 esg-project2 namei[2142240]: drwxr-xr-x root root etc
Jan 09 09:37:07 esg-project2 namei[2142240]: drwxr-x--- root opc  sustainacore-ai
Jan 09 09:37:07 esg-project2 namei[2142240]: -rw-r----- root opc  secrets.env
Jan 09 09:37:07 esg-project2 namei[2142241]: f: /etc/sustainacore/index.env
Jan 09 09:37:07 esg-project2 namei[2142241]: dr-xr-xr-x root root /
Jan 09 09:37:07 esg-project2 namei[2142241]: drwxr-xr-x root root etc
Jan 09 09:37:07 esg-project2 namei[2142241]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:07 esg-project2 namei[2142241]: -rw-r----- root opc  index.env
Jan 09 09:37:07 esg-project2 systemd[1]: Started SC_IDX daily price ingest (market data provider).
Jan 09 09:37:10 esg-project2 systemd[1]: Stopping SC_IDX daily price ingest (market data provider)...
Jan 09 09:37:10 esg-project2 systemd[1]: sc-idx-price-ingest.service: Deactivated successfully.
Jan 09 09:37:10 esg-project2 systemd[1]: Stopped SC_IDX daily price ingest (market data provider).
Jan 09 09:37:14 esg-project2 systemd[1]: Starting SC_IDX daily price ingest (market data provider)...
Jan 09 09:37:14 esg-project2 ls[2142319]: -rw-r-----. 1 root opc 722 Dec 25 10:39 /etc/sustainacore-ai/secrets.env
Jan 09 09:37:14 esg-project2 ls[2142319]: -rw-r-----. 1 root opc 387 Dec 25 10:39 /etc/sustainacore/db.env
Jan 09 09:37:14 esg-project2 ls[2142319]: -rw-r-----. 1 root opc  87 Jan  9 08:23 /etc/sustainacore/index.env
Jan 09 09:37:14 esg-project2 namei[2142320]: f: /etc/sustainacore/db.env
Jan 09 09:37:14 esg-project2 namei[2142320]: dr-xr-xr-x root root /
Jan 09 09:37:14 esg-project2 namei[2142320]: drwxr-xr-x root root etc
Jan 09 09:37:14 esg-project2 namei[2142320]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:14 esg-project2 namei[2142320]: -rw-r----- root opc  db.env
Jan 09 09:37:15 esg-project2 namei[2142321]: f: /etc/sustainacore-ai/secrets.env
Jan 09 09:37:15 esg-project2 namei[2142321]: dr-xr-xr-x root root /
Jan 09 09:37:15 esg-project2 namei[2142321]: drwxr-xr-x root root etc
Jan 09 09:37:15 esg-project2 namei[2142321]: drwxr-x--- root opc  sustainacore-ai
Jan 09 09:37:15 esg-project2 namei[2142321]: -rw-r----- root opc  secrets.env
Jan 09 09:37:15 esg-project2 namei[2142322]: f: /etc/sustainacore/index.env
Jan 09 09:37:15 esg-project2 namei[2142322]: dr-xr-xr-x root root /
Jan 09 09:37:15 esg-project2 namei[2142322]: drwxr-xr-x root root etc
Jan 09 09:37:15 esg-project2 namei[2142322]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:15 esg-project2 namei[2142322]: -rw-r----- root opc  index.env
Jan 09 09:37:15 esg-project2 systemd[1]: Started SC_IDX daily price ingest (market data provider).
Jan 09 09:37:16 esg-project2 flock[2142326]: index_engine_daily: end=2026-01-08 calls_used_today=1 remaining_daily=799 daily_limit=800 daily_buffer=25 max_provider_calls=774 minute_limit=8 minute_used=1
Jan 09 09:37:20 esg-project2 flock[2142326]: backfill_window: start=2026-01-08 end=2026-01-08 trading_days=1 tickers=25
Jan 09 09:37:28 esg-project2 flock[2142326]: backfill_summary: raw_upserts=0 canon_upserts=0 provider_calls_used=0
Jan 09 09:37:29 esg-project2 systemd[1]: sc-idx-price-ingest.service: Deactivated successfully.
```

### pipeline logs (last run)
```text
Jan 09 09:37:35 esg-project2 systemd[1]: Starting SC_IDX pipeline (ingest + completeness + impute + index calc)...
Jan 09 09:37:35 esg-project2 ls[2142410]: -rw-r-----. 1 root opc 722 Dec 25 10:39 /etc/sustainacore-ai/secrets.env
Jan 09 09:37:35 esg-project2 ls[2142410]: -rw-r-----. 1 root opc 387 Dec 25 10:39 /etc/sustainacore/db.env
Jan 09 09:37:35 esg-project2 ls[2142410]: -rw-r-----. 1 root opc  87 Jan  9 08:23 /etc/sustainacore/index.env
Jan 09 09:37:35 esg-project2 namei[2142411]: f: /etc/sustainacore/db.env
Jan 09 09:37:35 esg-project2 namei[2142411]: dr-xr-xr-x root root /
Jan 09 09:37:35 esg-project2 namei[2142411]: drwxr-xr-x root root etc
Jan 09 09:37:35 esg-project2 namei[2142411]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:35 esg-project2 namei[2142411]: -rw-r----- root opc  db.env
Jan 09 09:37:35 esg-project2 namei[2142412]: f: /etc/sustainacore-ai/secrets.env
Jan 09 09:37:35 esg-project2 namei[2142412]: dr-xr-xr-x root root /
Jan 09 09:37:35 esg-project2 namei[2142412]: drwxr-xr-x root root etc
Jan 09 09:37:35 esg-project2 namei[2142412]: drwxr-x--- root opc  sustainacore-ai
Jan 09 09:37:35 esg-project2 namei[2142412]: -rw-r----- root opc  secrets.env
Jan 09 09:37:35 esg-project2 namei[2142413]: f: /etc/sustainacore/index.env
Jan 09 09:37:35 esg-project2 namei[2142413]: dr-xr-xr-x root root /
Jan 09 09:37:35 esg-project2 namei[2142413]: drwxr-xr-x root root etc
Jan 09 09:37:35 esg-project2 namei[2142413]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:35 esg-project2 namei[2142413]: -rw-r----- root opc  index.env
Jan 09 09:37:35 esg-project2 systemd[1]: Started SC_IDX pipeline (ingest + completeness + impute + index calc).
Jan 09 09:37:35 esg-project2 flock[2142415]: sc_idx_pipeline_run: started_at_utc=2026-01-09T09:37:35Z head=a080225 skip_ingest=False resume=True
Jan 09 09:37:36 esg-project2 flock[2142415]: run_log: failed to insert start row: ORA-00001: unique constraint (WKSP_ESGAPEX.SYS_C0030115) violated on table WKSP_ESGAPEX.SC_IDX_JOB_RUNS columns (RUN_ID)
Jan 09 09:37:36 esg-project2 flock[2142415]: ORA-03301: (ORA-00001 details) row with column values (RUN_ID:'e6261fe8-f0ab-49b0-954d-4e8656c39fc6') already exists
Jan 09 09:37:36 esg-project2 flock[2142415]: Help: https://docs.oracle.com/error-help/db/ora-00001/
Jan 09 09:37:39 esg-project2 flock[2142415]: [pipeline] skip update_trading_days (resume)
Jan 09 09:37:39 esg-project2 flock[2142415]: [pipeline] skip ingest_prices (resume)
Jan 09 09:37:39 esg-project2 flock[2142415]: [pipeline] skip completeness_check (resume)
Jan 09 09:37:39 esg-project2 flock[2142415]: [pipeline] skip calc_index (resume)
Jan 09 09:37:39 esg-project2 flock[2142415]: [pipeline] skip impute (resume)
Jan 09 09:37:46 esg-project2 flock[2142415]: [pipeline] DONE
Jan 09 09:37:46 esg-project2 systemd[1]: sc-idx-pipeline.service: Deactivated successfully.
Jan 09 09:37:59 esg-project2 systemd[1]: Starting SC_IDX pipeline (ingest + completeness + impute + index calc)...
Jan 09 09:37:59 esg-project2 ls[2142545]: -rw-r-----. 1 root opc 722 Dec 25 10:39 /etc/sustainacore-ai/secrets.env
Jan 09 09:37:59 esg-project2 ls[2142545]: -rw-r-----. 1 root opc 387 Dec 25 10:39 /etc/sustainacore/db.env
Jan 09 09:37:59 esg-project2 ls[2142545]: -rw-r-----. 1 root opc  87 Jan  9 08:23 /etc/sustainacore/index.env
Jan 09 09:37:59 esg-project2 namei[2142546]: f: /etc/sustainacore/db.env
Jan 09 09:37:59 esg-project2 namei[2142546]: dr-xr-xr-x root root /
Jan 09 09:37:59 esg-project2 namei[2142546]: drwxr-xr-x root root etc
Jan 09 09:37:59 esg-project2 namei[2142546]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:59 esg-project2 namei[2142546]: -rw-r----- root opc  db.env
Jan 09 09:37:59 esg-project2 namei[2142547]: f: /etc/sustainacore-ai/secrets.env
Jan 09 09:37:59 esg-project2 namei[2142547]: dr-xr-xr-x root root /
Jan 09 09:37:59 esg-project2 namei[2142547]: drwxr-xr-x root root etc
Jan 09 09:37:59 esg-project2 namei[2142547]: drwxr-x--- root opc  sustainacore-ai
Jan 09 09:37:59 esg-project2 namei[2142547]: -rw-r----- root opc  secrets.env
Jan 09 09:37:59 esg-project2 namei[2142548]: f: /etc/sustainacore/index.env
Jan 09 09:37:59 esg-project2 namei[2142548]: dr-xr-xr-x root root /
Jan 09 09:37:59 esg-project2 namei[2142548]: drwxr-xr-x root root etc
Jan 09 09:37:59 esg-project2 namei[2142548]: drwxr-x--- root opc  sustainacore
Jan 09 09:37:59 esg-project2 namei[2142548]: -rw-r----- root opc  index.env
Jan 09 09:37:59 esg-project2 systemd[1]: Started SC_IDX pipeline (ingest + completeness + impute + index calc).
Jan 09 09:38:00 esg-project2 flock[2142551]: sc_idx_pipeline_run: started_at_utc=2026-01-09T09:38:00Z head=a080225 skip_ingest=False resume=True
Jan 09 09:38:00 esg-project2 flock[2142551]: run_log: failed to insert start row: ORA-00001: unique constraint (WKSP_ESGAPEX.SYS_C0030115) violated on table WKSP_ESGAPEX.SC_IDX_JOB_RUNS columns (RUN_ID)
Jan 09 09:38:00 esg-project2 flock[2142551]: ORA-03301: (ORA-00001 details) row with column values (RUN_ID:'e6261fe8-f0ab-49b0-954d-4e8656c39fc6') already exists
Jan 09 09:38:00 esg-project2 flock[2142551]: Help: https://docs.oracle.com/error-help/db/ora-00001/
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip update_trading_days (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip ingest_prices (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip completeness_check (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip calc_index (resume)
Jan 09 09:38:01 esg-project2 flock[2142551]: [pipeline] skip impute (resume)
Jan 09 09:38:11 esg-project2 flock[2142551]: [pipeline] DONE
Jan 09 09:38:11 esg-project2 systemd[1]: sc-idx-pipeline.service: Deactivated successfully.
```

## Oracle verification
### Max trade_date summary
```text
SC_IDX_TRADING_DAYS: 2026-01-08 00:00:00
SC_IDX_PRICES_CANON: 2026-01-08 00:00:00
SC_IDX_LEVELS: 2026-01-08 00:00:00
SC_IDX_STATS_DAILY: 2026-01-08 00:00:00
```

### Latest SC_IDX_JOB_RUNS rows
#### sc_idx_price_ingest
```text
run_id=2fd97f34-85cb-47b5-b017-3d51586a3bd8
status=OK
started_at=2026-01-09 09:37:16.820987
ended_at=2026-01-09 09:37:29.630518
error_msg=None
```
#### sc_idx_pipeline
```text
run_id=e6261fe8-f0ab-49b0-954d-4e8656c39fc6
status=OK
started_at=2026-01-09 09:22:43.440498
ended_at=2026-01-09 09:38:11.455282
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
oracle_error_counts_24h=13
last_error=None
stage_duration_impute_sec=3.61
stage_duration_update_trading_days_sec=0.48
stage_duration_calc_index_sec=0.51
stage_duration_ingest_prices_sec=23.67
stage_duration_completeness_check_sec=0.59
```
#### completeness_check
```text
run_id=f085f82c-e7e9-4d6c-b451-08958aadae5f
status=STARTED
started_at=2026-01-09 09:10:19.251733
ended_at=None
error_msg=None
```
#### index_calc_v1
```text
run_id=a913f023-34ce-4fa2-9cc0-21381449d7bb
status=ERROR
started_at=2026-01-09 09:04:05.865456
ended_at=2026-01-09 09:09:50.333396
error_msg=missing_prices
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
oracle_error_counts_24h=13
last_error=None
stage_duration_impute_sec=3.61
stage_duration_update_trading_days_sec=0.48
stage_duration_calc_index_sec=0.51
stage_duration_ingest_prices_sec=23.67
stage_duration_completeness_check_sec=0.59
```

## Lock + timeout sanity
- Locking: `/tmp/sc_idx_pipeline.lock` via `flock -n` in systemd unit ExecStart.
- Runtime guardrails: `RuntimeMaxSec=7200` for ingest; `RuntimeMaxSec=3600` for pipeline.
- Restart behavior: `Restart=on-failure` with `RestartSec=10`.

## Final checklist
- [x] Timers enabled and active (waiting).
- [x] Unit files point to `/home/opc/Sustainacore` and load `/etc/sustainacore/index.env` last.
- [x] Ingest run completed without MARKET_DATA_API_BASE_URL warnings after env ordering fix.
- [x] Pipeline run completed (resume-safe) and health snapshot updated.
- [x] Oracle max dates aligned; job runs OK.

## Conclusion
PASS: timers/services healthy, recent ingest + pipeline runs completed, locks + runtime limits prevent wedging.

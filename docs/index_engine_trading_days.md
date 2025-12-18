## Trading day calendar (SC_IDX_TRADING_DAYS)

The index engine uses an explicit trading-day calendar to remove weekend and holiday noise.

### Source of truth

- Trading days are derived from SPY daily bars via Twelve Data.
- `tools/index_engine/update_trading_days.py` backfills and upserts the calendar.
- Source label is `TWELVEDATA_SPY`.

### Why this matters

- Weekends and market holidays are no longer treated as missing data.
- Completeness checks use the calendar instead of weekday guessing.
- Ingest and imputation only operate on explicit trading dates.

### How to refresh the calendar

```bash
python tools/index_engine/update_trading_days.py
```

Output example:

```
inserted_count=252 total_count=252
```

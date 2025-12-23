## Index price completeness checker

This tool verifies whether TECH100 has enough canonical price history to calculate index levels. It focuses on canonical prices in `SC_IDX_PRICES_CANON` and ignores weekends and inferred market holidays.

### What it checks

- Generates weekday candidates between the requested start and end dates.
- Uses the TECH100 constituent list (latest rebalance via `tech11_ai_gov_eth_index`) as the expected ticker set.
- Counts canonical prices for each weekday using `canon_adj_close_px` (preferred) and `canon_close_px` when `--allow-canon-close` is set.
- Infers market holidays when daily coverage is extremely low (default: <= 10%).
- Flags weekdays that fall below the minimum daily coverage threshold.

### Holiday inference

Weekdays are treated as holidays when overall coverage is at or below the `--holiday-coverage-threshold` (default: `0.10`). These days are ignored as gaps even if raw data contains `MISSING` rows.

### Running the tool

Examples:

```bash
python tools/index_engine/check_price_completeness.py --start 2025-01-02 --end 2025-12-31
python tools/index_engine/check_price_completeness.py --since-base --end 2025-12-31
python tools/index_engine/check_price_completeness.py --since-base --end today --email-on-fail
```

Optional flags:

- `--min-daily-coverage 0.90`
- `--holiday-coverage-threshold 0.10`
- `--max-bad-days 0`
- `--allow-canon-close` (count `canon_close_px` when `canon_adj_close_px` is missing)

### Interpreting output

The summary line reports:

- `expected_trading_days`: weekdays between start and end (Mon–Fri)
- `actual_trading_days`: weekdays minus inferred holidays
- `total_gaps`: total missing ticker-days on trading days
- `worst_dates`: lowest-coverage trading dates
- `worst_tickers`: tickers with most missing trading days

The tool also prints top-10 lists for low coverage dates and tickers with the most missing days.

### Recommended thresholds for TECH100

- `min_daily_coverage`: `0.90` (minimum 90% of tickers available per trading day)
- `holiday_coverage_threshold`: `0.10` (treat days with <= 10% coverage as holidays)
- `max_bad_days`: `0` (no bad days allowed)

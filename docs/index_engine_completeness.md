## Index price completeness checker

This tool verifies whether TECH100 has enough canonical price history to calculate index levels. It focuses on canonical prices in `SC_IDX_PRICES_CANON` and uses the explicit trading-day calendar in `SC_IDX_TRADING_DAYS`.

### What it checks

- Uses `SC_IDX_TRADING_DAYS` as the expected trading date set.
- Uses the TECH100 impacted universe for each trade_date: `PORT_WEIGHT > 0`, top 25, with `port_date = MAX(port_date) <= trade_date`.
- Counts canonical prices for each weekday using `canon_adj_close_px` (preferred) and `canon_close_px` when `--allow-canon-close` is set.
- Flags trading days that fall below the minimum daily coverage threshold.

### Running the tool

Examples:

```bash
python tools/index_engine/check_price_completeness.py --start 2025-01-02 --end 2025-12-31
python tools/index_engine/check_price_completeness.py --since-base --end 2025-12-31
python tools/index_engine/check_price_completeness.py --since-base --end today --email-on-fail
```

Optional flags:

- `--min-daily-coverage 1.00`
- `--max-bad-days 0`
- `--allow-canon-close` (count `canon_close_px` when `canon_adj_close_px` is missing)
- `--allow-imputation` (treat gaps as pass for calculation mode)

### Interpreting output

The summary line reports:

- `expected_trading_days`: trading days between start and end
- `actual_trading_days`: same as expected trading days
- `total_gaps`: total missing ticker-days on trading days
- `worst_dates`: lowest-coverage trading dates
- `worst_tickers`: tickers with most missing trading days

The tool also prints top-10 lists for low coverage dates and tickers with the most missing days.

### Recommended thresholds for TECH100

- `min_daily_coverage`: `0.90` (minimum 90% of tickers available per trading day)
- `min_daily_coverage`: `1.00` (require 100% canonical coverage per trading day)
- `max_bad_days`: `0` (no bad days allowed)

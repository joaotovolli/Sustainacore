# Alpha Vantage cleanup (one-time)

If Alpha Vantage ingestion was enabled previously, Oracle may contain rows with `provider='ALPHAVANTAGE'` (often `ERROR` status). To remove them, run the one-time cleanup SQL:

- Script: `oracle_scripts/sc_idx_cleanup_alphavantage_rows_v1.sql`
- Effect: `DELETE FROM SC_IDX_PRICES_RAW WHERE provider='ALPHAVANTAGE'; COMMIT;`

Run it manually in your Oracle client (SQL*Plus / SQLcl / APEX SQL Workshop). Do not schedule it.

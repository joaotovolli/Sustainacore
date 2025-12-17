-- One-time cleanup: remove Alpha Vantage rows from SC_IDX_PRICES_RAW.
-- Run manually as a privileged schema owner if you previously enabled the provider.

DELETE FROM SC_IDX_PRICES_RAW WHERE provider = 'ALPHAVANTAGE';
COMMIT;

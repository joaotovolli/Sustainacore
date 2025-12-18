-- Remove NULL price rows from RAW and CANON (idempotent)
DELETE FROM SC_IDX_PRICES_RAW
WHERE close_px IS NULL OR adj_close_px IS NULL;

DELETE FROM SC_IDX_PRICES_CANON
WHERE canon_close_px IS NULL OR canon_adj_close_px IS NULL;

COMMIT;

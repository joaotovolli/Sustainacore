<!-- cspell:ignore CRWD -->
# TECH100 corporate-action reconstruction

TECH100 uses canonical adjusted closing prices. A confirmed split is handled by refreshing adjusted
history onto one basis and rebuilding dependent history. Synthetic shares are not additionally
multiplied for that action.

## Safety contract

- Run Oracle preflight first through the live service environment.
- Stop the pipeline timer before an apply or rollback.
- Dry-run is the default; `--apply` is required for writes.
- Require authoritative confirmation before applying an action.
- Backups are created before canonical or derived rows change.
- Every backup belongs to one manifest run and is validated for object existence, columns, row count,
  and date range before any source data changes.
- Backup object collisions fail closed. Reuse requires `--reuse-backups`, the exact run identifier,
  and a complete validated manifest.
- An unresolved ratio-like move blocks publication.

## Dry-run

```bash
python3 tools/db_migrations/repair_sc_idx_corporate_actions.py \
  --ticker CRWD --effective-date 2026-07-02 --ratio 4 \
  --source-reference '<authoritative-reference>' \
  --report tools/audit/output/corporate_action_forensics/repair_dry_run.txt
```

Dry-run executes metadata and diagnostic `SELECT` statements only.

## Controlled apply

After merge and deployment, stop the timer and rerun dry-run. Apply using either a reviewed adjusted
history CSV or the configured bounded ticker-history refresh:

```bash
python3 tools/db_migrations/repair_sc_idx_corporate_actions.py --apply \
  --ticker CRWD --effective-date 2026-07-02 --ratio 4 \
  --source-reference '<authoritative-reference>' \
  --refresh-adjusted-history --max-refresh-calls 500 \
  --start 2025-01-02 \
  --report tools/audit/output/corporate_action_forensics/repair_apply.txt
```

The default backup tag contains UTC seconds and a random suffix. Raw and canonical price history are
both included because the automated refresh updates both. The tool records the run identifier,
tag, target and backup objects, date range, source and backup row counts, creation timestamp, and
validation status in `SC_IDX_CA_BACKUP_MANIFEST`.

The apply order is fixed: validate the complete backup set, refresh or update the confirmed ticker,
prove the earliest changed date and split continuity, rebuild official outputs, rebuild portfolio
outputs, run strict quantitative verification, and only then mark the action `APPLIED`. Any rebuild or
verification failure leaves it `CONFIRMED` and prints the exact rollback command.

CSV repair rejects duplicate or out-of-range dates, does not permit implicit inserts, and requires the
Oracle affected-row count to match the intended update count. Automated refresh success requires an
actual material change and an economically continuous adjusted-price series across the effective date.

## Rollback

Keep the release symlink target recorded before deployment. Restore the prior release symlink first,
then restore Oracle rows using the exact apply backup tag:

```bash
python3 tools/db_migrations/repair_sc_idx_corporate_actions.py \
  --rollback-tag <validated-tag> --start 2025-01-02 --end <backup-end-date>
```

Rollback validates the entire manifest, every production and backup object, column compatibility, row
counts, and date ranges before its first `DELETE`. Restoration runs in one transaction. Any object
failure triggers connection rollback and reports that object; it never skips a missing table or commits
a partial restoration. Leave the timer stopped and preserve every backup table after any failure.

## Required verification

- CRWD has no split-only loss and no duplicate adjustment.
- `market_value = shares * price_used` within 1e-6.
- daily index return equals summed contributions within 1e-6.
- every rebalance bridge agrees with the prior level within max(1e-6, level × 1e-8).
- every rebalance constituent has an exact previous-trading-day canonical adjusted price and no stale
  implied anchor or substitute quality.
- the configured unexplained daily-return threshold is enforced.
- no partial constituent or contribution dates exist.
- no pending or unresolved action affects the published window.
- all official calc tables share the expected maximum date.
- portfolio daily tables reach the official level date and optimizer inputs reach the latest required
  rebalance date.

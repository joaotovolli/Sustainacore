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
  --start 2025-01-02 --backup-tag 20260712A \
  --report tools/audit/output/corporate_action_forensics/repair_apply.txt
```

The tool refreshes the confirmed ticker, detects the earliest changed dependency, rebuilds holdings,
divisors, levels, constituent history, contributions, statistics, and then rebuilds portfolio
analytics, positions, and optimizer inputs.

## Rollback

Keep the release symlink target recorded before deployment. Restore the prior release symlink first,
then restore Oracle rows using the exact apply backup tag:

```bash
python3 tools/db_migrations/repair_sc_idx_corporate_actions.py \
  --rollback-tag 20260712A --start 2025-01-02
```

Run preflight, quantitative verification, and freshness checks before restarting the timer. If the
rollback fails, leave the timer stopped and preserve every backup table.

## Required verification

- CRWD has no split-only loss and no duplicate adjustment.
- `market_value = shares * price_used` within 1e-6.
- daily index return equals summed contributions within 1e-6.
- every rebalance bridge agrees with the prior level within max(1e-6, level × 1e-8).
- all official calc tables share the expected maximum date.
- portfolio daily tables reach the official level date and optimizer inputs reach the latest required
  rebalance date.

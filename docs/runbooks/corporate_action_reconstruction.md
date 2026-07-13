<!-- cspell:ignore CRWD fsync oneshot -->
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

## Exhaustive reconstruction readiness

Run the full-range readiness sweep before every apply. It is also invoked automatically by the apply
tool before schema, backup, price, or derived-table writes:

```bash
python3 tools/index_engine/reconstruction_readiness.py \
  --start 2025-01-02 \
  --adjusted-price-csv /tmp/reviewed_adjusted_history.csv \
  --ticker CRWD \
  --probe-missing-anchors \
  --require-quiescent \
  --rehearse-portfolio
```

The command uses `SELECT` statements only, overlays the reviewed CSV in memory, traverses every active
universe and rebalance, aggregates all exact-anchor defects, probes each missing ticker/date once, and
classifies every large move. Missing anchors are acceptable only when the bounded exact-date probe can
recover them. Ratio-like unresolved moves, stale or substitute anchors, missing active prices, schema
or restore-shape defects, and incomplete holdings all fail readiness. Output must include
`mode=RECONSTRUCTION_READINESS`, `oracle_writes=0`, and `overall_status=PASS`.

The portfolio stage is rehearsed using its dry-run path. Oracle backup DDL, manifest inserts, canonical
updates, official/portfolio persistence, rollback DML, final status transition, timer restoration, and
research export cannot be write-equivalently rehearsed against production without writes. They remain
protected by static DDL checks, prior validated restore-shape evidence, focused transaction fixtures,
the manifest-backed compensation path, and post-reconstruction strict verification.

### Failure history and earlier detection

| Pull Request | Production-discovered defect | Why earlier checks missed it | Earlier detection now |
| --- | --- | --- | --- |
| #547 | Oracle rejected binds in CTAS DDL | Fake cursors accepted DDL binds | Readiness inspects CTAS for bind-free typed dates; regression executes DDL without binds |
| #548 | CTAS relaxed nullability | Compatibility required byte-for-byte metadata | Readiness validates restore shape against a complete production backup while ignoring only nullability |
| #549 | Ordinary large moves stopped reconstruction | Tests covered classification but not full history | Readiness enumerates and classifies every threshold breach across the full range |
| #550 | Exact entrant anchors were absent | Runtime stopped at the first rebalance | Readiness aggregates every rebalance and probes all missing ticker/date requirements before apply |

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
verification failure enters the same compensation path as a price-repair failure.

Immediately after backup validation, the tool persists and flushes the backup tag, run identifier,
repair range, and exact rollback command before the first mutation-capable operation. Every later stage
uses one controlled failure handler, including external ingestion, CSV update, post-write validation,
action confirmation, both rebuilds, strict verification, and final status transition.

After any possible source mutation, failure handling first rolls back uncommitted parent-connection
work, then performs manifest-backed atomic restoration and verifies restored counts and ranges. A
successful compensation reports `automatic_rollback=PASS`. If compensation fails, the report retains
both the original stage error and rollback error plus the manual rollback command. The timer must stay
stopped. A failure before any mutation reports `automatic_rollback=NOT_REQUIRED` and does not rewrite
otherwise valid source tables. Status reporting distinguishes `NOT_RECORDED`, `CONFIRMED`, and
`APPLIED`; after successful compensation it reports the status actually restored from the backup set.

CSV repair rejects duplicate or out-of-range dates, does not permit implicit inserts, and requires the
Oracle affected-row count to match the intended update count. Automated refresh success requires an
actual material change and an economically continuous adjusted-price series across the effective date.

During `--rebuild --strict` only, missing exact rebalance anchors receive one bounded exact-date fetch
and are then fetched again and validated. The historical fallback window is date-aware and capped. Ordinary
scheduled calculation does not make this retry. Any still-missing, stale, historical, or current-day
anchor blocks publication.

### Low-resource VM1 execution profile

VM1 has one vCPU and approximately 1 GB RAM. Use the checked-in launcher for a controlled apply after
the normal dry-run and readiness gates pass:

```bash
bash tools/index_engine/run_reconstruction_low_resource.sh --apply \
  --ticker CRWD --effective-date 2026-07-02 --ratio 4 \
  --source-reference '<authoritative-reference>' \
  --adjusted-price-csv /tmp/reviewed_adjusted_history.csv \
  --start 2025-01-02 --end <latest-complete-date>
```

The transient unit uses `Type=exec`, `Nice=10`, idle I/O scheduling, a 70% CPU quota, and a soft
650 MB `MemoryHigh`. It deliberately has no `MemoryMax` and no hard `RuntimeMaxSec`: forced cgroup
termination cannot run manifest compensation. Each heavy Python subprocess instead has a bounded
timeout which raises into the normal compensation handler. Do not use `Type=oneshot` with
`RuntimeMaxSec`; systemd ignores that combination while the start job is active.

The launcher submits with `systemd-run --no-block` and returns after unit acceptance. It prints the
generated unit, durable status path, and one bounded inspection command; it never attaches Codex to
the reconstruction output or starts a polling loop. Expected acceptance coordinates are:

```text
launch_status=ACCEPTED
unit_name=<generated-unit>.service
status_file=/var/lib/sustainacore/sc_idx/reconstruction_status.json
status_command=<one-shot-command>
```

Portfolio output is generated one model at a time and written in 250-row batches. Each committed
model or table remains covered by the validated reconstruction manifest. An Oracle disconnect is not
blindly retried because commit state is ambiguous; it fails into fresh-connection manifest rollback.
The official index writer also bounds Oracle array DML while retaining the same pre-publication
mathematical validation.

Model portfolio constraints are static reconstruction prerequisites. Before the first portfolio
output deletion, the low-resource path generates the expected constraints and compares the normalized
set exactly with Oracle. A mismatch fails before mutation. When they match, the controlled path never
deletes or rewrites `SC_IDX_MODEL_PORTFOLIO_CONSTRAINTS`; therefore the dated 12-object manifest covers
every table that the path can mutate.

`SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS` bounds each Oracle statement on the parent reconstruction
connection, inherited raw/canonical price-ingestion connections, and every fresh persistence,
compensation, restoration-verification, and final-status connection. The default is 300000
milliseconds, chosen to accommodate the validated backup sizes on VM1 while preventing an Oracle call
from hanging indefinitely. A timeout before mutation does not restore; a timeout after possible
mutation enters manifest-backed compensation with fresh connections.

Swap is not created automatically. Adding swap changes VM-wide failure and latency behavior and
requires separate operator approval. Verify memory and kernel evidence before considering it.

### One-shot progress inspection

The controlled apply atomically updates:

```text
/var/lib/sustainacore/sc_idx/reconstruction_status.json
```

Inspect it once without polling:

```bash
python3 tools/index_engine/reconstruction_status.py
```

The file contains recovery coordinates, deployed revision, current stage, status, failure class,
rollback status, last completed date, rows processed, current model, completed model count, and
committed analytics, position and optimizer row counts. It never contains credentials, environment
values or provider responses. Status updates are written through a temporary file, `fsync`, and atomic
rename. A status-write error is reported as a secondary diagnostic and never prevents database
compensation.

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

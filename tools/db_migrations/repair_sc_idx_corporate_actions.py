#!/usr/bin/env python3
"""Back up, repair, verify, or atomically roll back TECH100 history."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import secrets
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping

ROOT = Path(os.getenv("SC_IDX_REPO_ROOT") or Path(__file__).resolve().parents[2])
APP = ROOT / "app"
for path in (ROOT, APP):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from db_helper import get_connection
from index_engine.corporate_actions import earliest_material_change
from index_engine.reconstruction_status import write_reconstruction_status
from tools.oracle.env_bootstrap import load_env_files

BASE_DATE = dt.date(2025, 1, 2)
MANIFEST_TABLE = "SC_IDX_CA_BACKUP_MANIFEST"
BACKUP_OBJECTS = {
    "PR": "SC_IDX_PRICES_RAW",
    "PC": "SC_IDX_PRICES_CANON",
    "HD": "SC_IDX_HOLDINGS",
    "DV": "SC_IDX_DIVISOR",
    "LV": "SC_IDX_LEVELS",
    "CD": "SC_IDX_CONSTITUENT_DAILY",
    "CO": "SC_IDX_CONTRIBUTION_DAILY",
    "ST": "SC_IDX_STATS_DAILY",
    "PA": "SC_IDX_PORTFOLIO_ANALYTICS_DAILY",
    "PP": "SC_IDX_PORTFOLIO_POSITION_DAILY",
    "PO": "SC_IDX_PORTFOLIO_OPT_INPUTS",
    "CA": "SC_IDX_CORPORATE_ACTIONS",
}


class BackupValidationError(RuntimeError):
    """The complete backup set could not be proven before a write."""


class BackupCollisionError(BackupValidationError):
    """A generated or requested backup object already exists."""


class RestorationError(RuntimeError):
    """Atomic rollback failed for one target object."""


class PriceRepairError(RuntimeError):
    """Adjusted-price input or affected-row validation failed."""


class ReconstructionError(RuntimeError):
    """Rebuild, strict verification, or final status transition failed."""


class ApplyStageError(RuntimeError):
    """One named post-backup apply stage failed."""

    def __init__(self, stage: str, original: Exception):
        self.stage = stage
        self.original = original
        super().__init__(f"{stage}_failed:{original}")


@dataclass(frozen=True)
class BackupRecord:
    run_id: str
    tag: str
    target_object: str
    backup_object: str
    start_date: dt.date
    end_date: dt.date
    source_row_count: int
    backup_row_count: int
    validation_status: str


@dataclass(frozen=True)
class ApplyContext:
    tag: str
    run_id: str
    start: dt.date
    end: dt.date
    rollback_command: str
    report_path: Path
    status_path: Path | None = None
    revision: str = ""


@dataclass(frozen=True)
class ApplyStage:
    name: str
    operation: Callable[[], None]
    may_mutate: bool = False
    action_status_after: str | None = None


def _as_date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    return value if isinstance(value, dt.date) else None


def new_backup_tag() -> str:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{timestamp}{secrets.token_hex(2).upper()}"


def deployed_revision() -> str:
    configured = os.getenv("SC_IDX_DEPLOYED_REVISION", "").strip()
    if configured:
        return configured
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    return result.stdout.strip() if result.returncode == 0 else "UNKNOWN"


def backup_name(tag: str, code: str) -> str:
    clean = re.sub(r"[^A-Z0-9]", "", tag.upper())
    if not clean or len(clean) > 18:
        raise ValueError("invalid_backup_tag")
    name = f"SCB_CA_{clean}_{code}"
    if len(name) > 30:
        raise ValueError("backup_object_name_too_long")
    return name


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Repair TECH100 history after a confirmed corporate action")
    result.add_argument("--apply", action="store_true")
    result.add_argument("--rollback-tag")
    result.add_argument("--reuse-backups", action="store_true")
    result.add_argument("--run-id")
    result.add_argument("--ticker")
    result.add_argument("--effective-date", type=dt.date.fromisoformat)
    result.add_argument("--ratio", type=float)
    result.add_argument("--action-type", choices=("FORWARD_SPLIT", "REVERSE_SPLIT"), default="FORWARD_SPLIT")
    result.add_argument("--source-type", default="REGULATORY_FILING")
    result.add_argument("--source-reference")
    result.add_argument("--adjusted-price-csv", type=Path)
    result.add_argument("--refresh-adjusted-history", action="store_true")
    result.add_argument("--max-refresh-calls", type=int, default=500)
    result.add_argument("--start", type=dt.date.fromisoformat, default=BASE_DATE)
    result.add_argument("--end", type=dt.date.fromisoformat)
    result.add_argument("--backup-tag")
    result.add_argument("--report", type=Path)
    result.add_argument(
        "--status-file",
        type=Path,
        default=Path(
            os.getenv(
                "SC_IDX_RECONSTRUCTION_STATUS_FILE",
                "/var/lib/sustainacore/sc_idx/reconstruction_status.json",
            )
        ),
    )
    return result


def _date_column(table: str) -> str:
    columns = {
        "SC_IDX_PRICES_RAW": "TRADE_DATE",
        "SC_IDX_PRICES_CANON": "TRADE_DATE",
        "SC_IDX_HOLDINGS": "REBALANCE_DATE",
        "SC_IDX_DIVISOR": "EFFECTIVE_DATE",
        "SC_IDX_LEVELS": "TRADE_DATE",
        "SC_IDX_CONSTITUENT_DAILY": "TRADE_DATE",
        "SC_IDX_CONTRIBUTION_DAILY": "TRADE_DATE",
        "SC_IDX_STATS_DAILY": "TRADE_DATE",
        "SC_IDX_PORTFOLIO_ANALYTICS_DAILY": "TRADE_DATE",
        "SC_IDX_PORTFOLIO_POSITION_DAILY": "TRADE_DATE",
        "SC_IDX_PORTFOLIO_OPT_INPUTS": "TRADE_DATE",
        "SC_IDX_CORPORATE_ACTIONS": "EFFECTIVE_DATE",
    }
    return columns[table]


def _write_report(path: Path | None, lines: Iterable[str]) -> None:
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _object_exists(conn, object_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM user_tables WHERE table_name=:name", {"name": object_name})
    return int(cur.fetchone()[0]) == 1


def _columns(conn, object_name: str) -> list[tuple[object, ...]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name,data_type,data_length,data_precision,data_scale,nullable "
        "FROM user_tab_columns WHERE table_name=:name ORDER BY column_id",
        {"name": object_name},
    )
    return [tuple(row) for row in cur.fetchall()]


def _columns_compatible(conn, target_object: str, backup_object: str) -> bool:
    """Compare the ordered restore shape while ignoring CTAS nullability relaxation."""
    target = _columns(conn, target_object)
    backup = _columns(conn, backup_object)
    return [column[:5] for column in target] == [column[:5] for column in backup]


def _count_and_range(
    conn,
    object_name: str,
    date_column: str,
    start: dt.date,
    end: dt.date,
) -> tuple[int, dt.date | None, dt.date | None]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*),MIN({date_column}),MAX({date_column}) FROM {object_name} "
        f"WHERE {date_column} BETWEEN :start_date AND :end_date",
        {"start_date": start, "end_date": end},
    )
    count, minimum, maximum = cur.fetchone()
    return int(count), _as_date(minimum), _as_date(maximum)


def _total_count_and_range(
    conn,
    object_name: str,
    date_column: str,
) -> tuple[int, dt.date | None, dt.date | None]:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*),MIN({date_column}),MAX({date_column}) FROM {object_name}")
    count, minimum, maximum = cur.fetchone()
    return int(count), _as_date(minimum), _as_date(maximum)


def _manifest_rows(conn, tag: str) -> list[BackupRecord]:
    if not _object_exists(conn, MANIFEST_TABLE):
        raise BackupValidationError("backup_manifest_table_missing")
    cur = conn.cursor()
    cur.execute(
        "SELECT run_id,backup_tag,target_object,backup_object,start_date,end_date,"
        "source_row_count,backup_row_count,validation_status "
        f"FROM {MANIFEST_TABLE} WHERE backup_tag=:tag ORDER BY target_object",
        {"tag": tag},
    )
    return [
        BackupRecord(
            run_id=str(row[0]),
            tag=str(row[1]),
            target_object=str(row[2]),
            backup_object=str(row[3]),
            start_date=_as_date(row[4]),
            end_date=_as_date(row[5]),
            source_row_count=int(row[6]),
            backup_row_count=int(row[7]),
            validation_status=str(row[8]),
        )
        for row in cur.fetchall()
    ]


def validate_backup_set(
    conn,
    tag: str,
    start: dt.date,
    end: dt.date,
    *,
    expected_run_id: str | None = None,
) -> list[BackupRecord]:
    """Prove one complete, compatible backup set without executing DML."""
    records = _manifest_rows(conn, tag)
    expected_targets = set(BACKUP_OBJECTS.values())
    actual_targets = {record.target_object for record in records}
    if actual_targets != expected_targets:
        missing = sorted(expected_targets - actual_targets)
        extra = sorted(actual_targets - expected_targets)
        raise BackupValidationError(f"backup_manifest_incomplete:missing={missing}:extra={extra}")
    run_ids = {record.run_id for record in records}
    if len(run_ids) != 1:
        raise BackupValidationError("backup_manifest_multiple_run_ids")
    run_id = next(iter(run_ids))
    if expected_run_id is not None and run_id != expected_run_id:
        raise BackupValidationError("backup_manifest_run_id_mismatch")

    validated: list[BackupRecord] = []
    for record in records:
        if record.validation_status != "VALIDATED":
            raise BackupValidationError(f"backup_manifest_not_validated:{record.target_object}")
        if record.start_date != start or record.end_date != end:
            raise BackupValidationError(f"backup_manifest_range_mismatch:{record.target_object}")
        if not _object_exists(conn, record.target_object):
            raise BackupValidationError(f"production_object_missing:{record.target_object}")
        if not _object_exists(conn, record.backup_object):
            raise BackupValidationError(f"backup_object_missing:{record.backup_object}")
        if not _columns_compatible(conn, record.target_object, record.backup_object):
            raise BackupValidationError(f"backup_columns_mismatch:{record.target_object}")
        count, minimum, maximum = _total_count_and_range(
            conn, record.backup_object, _date_column(record.target_object)
        )
        if count != record.source_row_count or count != record.backup_row_count:
            raise BackupValidationError(f"backup_row_count_mismatch:{record.target_object}")
        if minimum is not None and minimum < start:
            raise BackupValidationError(f"backup_min_date_outside_range:{record.target_object}")
        if maximum is not None and maximum > end:
            raise BackupValidationError(f"backup_max_date_outside_range:{record.target_object}")
        validated.append(record)
    return validated


def ensure_action_schema(conn) -> None:
    blocks: list[str] = []
    current: list[str] = []
    for line in (ROOT / "oracle_scripts/sc_idx_corporate_actions.sql").read_text(encoding="utf-8").splitlines():
        if line.strip() == "/":
            if current:
                blocks.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        blocks.append("\n".join(current))
    cur = conn.cursor()
    for block in blocks:
        cur.execute(block)


def create_backups(
    conn,
    tag: str,
    run_id: str,
    start: dt.date,
    end: dt.date,
    *,
    reuse: bool = False,
) -> list[BackupRecord]:
    if reuse:
        return validate_backup_set(conn, tag, start, end, expected_run_id=run_id)

    names = {code: backup_name(tag, code) for code in BACKUP_OBJECTS}
    collisions = sorted(name for name in names.values() if _object_exists(conn, name))
    if collisions:
        raise BackupCollisionError(f"backup_object_collision:{','.join(collisions)}")

    cur = conn.cursor()
    records: list[BackupRecord] = []
    for code, target in BACKUP_OBJECTS.items():
        if not _object_exists(conn, target):
            raise BackupValidationError(f"production_object_missing:{target}")
        backup = names[code]
        date_column = _date_column(target)
        source_count, _, _ = _count_and_range(conn, target, date_column, start, end)
        # Oracle does not permit bind variables in DDL (DPI-1059). These values
        # are parsed argparse dates, so render typed DATE literals while keeping
        # all identifiers constrained by the static backup allowlist above.
        start_literal = f"DATE '{start.isoformat()}'"
        end_literal = f"DATE '{end.isoformat()}'"
        cur.execute(
            f"CREATE TABLE {backup} AS SELECT * FROM {target} "
            f"WHERE {date_column} BETWEEN {start_literal} AND {end_literal}"
        )
        if not _columns_compatible(conn, target, backup):
            raise BackupValidationError(f"backup_columns_mismatch:{target}")
        backup_count, _, _ = _total_count_and_range(conn, backup, date_column)
        if backup_count != source_count:
            raise BackupValidationError(f"backup_row_count_mismatch:{target}")
        records.append(
            BackupRecord(run_id, tag, target, backup, start, end, source_count, backup_count, "VALIDATED")
        )

    cur.executemany(
        f"INSERT INTO {MANIFEST_TABLE} "
        "(run_id,backup_tag,target_object,backup_object,start_date,end_date,source_row_count,"
        "backup_row_count,created_at,validation_status) "
        "VALUES (:run_id,:tag,:target,:backup,:start_date,:end_date,:source_count,"
        ":backup_count,SYSTIMESTAMP,:status)",
        [
            {
                "run_id": record.run_id,
                "tag": record.tag,
                "target": record.target_object,
                "backup": record.backup_object,
                "start_date": record.start_date,
                "end_date": record.end_date,
                "source_count": record.source_row_count,
                "backup_count": record.backup_row_count,
                "status": record.validation_status,
            }
            for record in records
        ],
    )
    conn.commit()
    return validate_backup_set(conn, tag, start, end, expected_run_id=run_id)


def load_adjusted_prices(
    path: Path,
    ticker: str,
    start: dt.date = BASE_DATE,
    end: dt.date | None = None,
) -> dict[dt.date, float]:
    end = end or dt.date.max
    rows: dict[dt.date, float] = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"ticker", "trade_date", "adjusted_close"}
        if not reader.fieldnames or not required.issubset({item.lower() for item in reader.fieldnames}):
            raise PriceRepairError("price_csv_columns_required:ticker,trade_date,adjusted_close")
        keys = {item.lower(): item for item in reader.fieldnames}
        for row in reader:
            if str(row[keys["ticker"]]).strip().upper() != ticker:
                continue
            day = dt.date.fromisoformat(str(row[keys["trade_date"]])[:10])
            if day in rows:
                raise PriceRepairError(f"duplicate_adjusted_price_date:{day.isoformat()}")
            if not start <= day <= end:
                raise PriceRepairError(f"adjusted_price_date_outside_repair_range:{day.isoformat()}")
            value = float(row[keys["adjusted_close"]])
            if value <= 0:
                raise PriceRepairError(f"invalid_adjusted_price:{day.isoformat()}")
            rows[day] = value
    if not rows:
        raise PriceRepairError("no_adjusted_prices_for_ticker")
    return rows


def fetch_adjusted_prices(
    conn,
    ticker: str,
    start: dt.date,
    end: dt.date,
) -> dict[dt.date, float]:
    cur = conn.cursor()
    cur.execute(
        "SELECT trade_date,canon_adj_close_px FROM SC_IDX_PRICES_CANON "
        "WHERE ticker=:ticker AND trade_date BETWEEN :start_date AND :end_date ORDER BY trade_date",
        {"ticker": ticker, "start_date": start, "end_date": end},
    )
    return {
        _as_date(day): float(price)
        for day, price in cur.fetchall()
        if _as_date(day) is not None and price is not None
    }


def validate_price_update_scope(
    conn,
    ticker: str,
    prices: Mapping[dt.date, float],
    start: dt.date,
    end: dt.date,
) -> dict[dt.date, float]:
    outside = sorted(day for day in prices if not start <= day <= end)
    if outside:
        raise PriceRepairError(f"adjusted_price_dates_outside_backup:{outside[0].isoformat()}")
    stored = fetch_adjusted_prices(conn, ticker, start, end)
    missing = sorted(set(prices) - set(stored))
    if missing:
        raise PriceRepairError(f"canonical_rows_missing:no_inserts_permitted:{missing[0].isoformat()}")
    return stored


def validate_canonical_dates_backed_up(
    conn,
    records: Iterable[BackupRecord],
    ticker: str,
    dates: Iterable[dt.date],
) -> None:
    intended = sorted(set(dates))
    canonical = next(
        (record for record in records if record.target_object == "SC_IDX_PRICES_CANON"),
        None,
    )
    if canonical is None:
        raise BackupValidationError("canonical_backup_manifest_missing")
    if not intended:
        raise BackupValidationError("canonical_backup_dates_empty")
    placeholders = ",".join(f":day{index}" for index in range(len(intended)))
    binds: dict[str, object] = {"ticker": ticker}
    binds.update({f"day{index}": day for index, day in enumerate(intended)})
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(DISTINCT trade_date) FROM {canonical.backup_object} "
        f"WHERE ticker=:ticker AND trade_date IN ({placeholders})",
        binds,
    )
    backed_count = int(cur.fetchone()[0])
    if backed_count != len(intended):
        raise BackupValidationError(
            f"canonical_backup_date_count_mismatch:expected={len(intended)}:actual={backed_count}"
        )


def apply_prices(conn, ticker: str, prices: Mapping[dt.date, float]) -> int:
    rows = [{"ticker": ticker, "trade_date": day, "price": price} for day, price in sorted(prices.items())]
    cur = conn.cursor()
    cur.executemany(
        "UPDATE SC_IDX_PRICES_CANON SET canon_adj_close_px=:price,computed_at=SYSTIMESTAMP "
        "WHERE ticker=:ticker AND trade_date=:trade_date",
        rows,
    )
    actual = int(cur.rowcount)
    if actual != len(rows):
        conn.rollback()
        raise PriceRepairError(f"canonical_update_count_mismatch:expected={len(rows)}:actual={actual}")
    conn.commit()
    return actual


def validate_refreshed_history(
    before: Mapping[dt.date, float],
    after: Mapping[dt.date, float],
    *,
    effective_date: dt.date,
    start: dt.date,
    end: dt.date,
    max_economic_return: float = 0.20,
) -> tuple[dt.date, int, float]:
    changed = earliest_material_change(before, after)
    if changed is None:
        raise PriceRepairError("adjusted_history_refresh_no_material_change")
    changed_dates = [day for day in sorted(set(before) & set(after)) if before[day] != after[day]]
    if any(day < start or day > end for day in changed_dates):
        raise PriceRepairError("adjusted_history_change_outside_backup_range")
    previous_dates = [day for day in after if day < effective_date]
    if not previous_dates or effective_date not in after:
        raise PriceRepairError("split_continuity_prices_missing")
    previous_date = max(previous_dates)
    economic_return = after[effective_date] / after[previous_date] - 1.0
    if abs(economic_return) > max_economic_return:
        raise PriceRepairError(
            f"split_discontinuity_unresolved:return={economic_return:.12g}:threshold={max_economic_return:.12g}"
        )
    return changed, len(changed_dates), economic_return


def upsert_action_confirmed(conn, args: argparse.Namespace, start: dt.date, run_id: str) -> None:
    conn.cursor().execute(
        """
        MERGE INTO SC_IDX_CORPORATE_ACTIONS d
        USING (SELECT 'TECH100' index_code,:ticker ticker,:action_type action_type,
                      :effective_date effective_date FROM dual) s
        ON (d.index_code=s.index_code AND d.ticker=s.ticker AND d.action_type=s.action_type
            AND d.effective_date=s.effective_date)
        WHEN MATCHED THEN UPDATE SET ratio=:ratio,confirmation_status='CONFIRMED',
          source_type=:source_type,source_reference=:source_reference,
          confirmed_at=NVL(d.confirmed_at,SYSTIMESTAMP),applied_at=NULL,
          affected_start_date=:affected_start_date,processing_run_id=:run_id
        WHEN NOT MATCHED THEN INSERT
          (index_code,ticker,action_type,effective_date,ratio,confirmation_status,source_type,
           source_reference,confirmed_at,processing_method,applied_at,affected_start_date,processing_run_id)
        VALUES
          ('TECH100',:ticker,:action_type,:effective_date,:ratio,'CONFIRMED',:source_type,
           :source_reference,SYSTIMESTAMP,'REFRESH_ADJUSTED_HISTORY',NULL,:affected_start_date,:run_id)
        """,
        {
            "ticker": args.ticker,
            "action_type": args.action_type,
            "effective_date": args.effective_date,
            "ratio": args.ratio,
            "source_type": args.source_type,
            "source_reference": args.source_reference,
            "affected_start_date": start,
            "run_id": run_id,
        },
    )
    conn.commit()


def mark_action_applied(conn, args: argparse.Namespace, run_id: str) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE SC_IDX_CORPORATE_ACTIONS SET confirmation_status='APPLIED',applied_at=SYSTIMESTAMP "
        "WHERE index_code='TECH100' AND ticker=:ticker AND action_type=:action_type "
        "AND effective_date=:effective_date AND processing_run_id=:run_id "
        "AND confirmation_status='CONFIRMED'",
        {
            "ticker": args.ticker,
            "action_type": args.action_type,
            "effective_date": args.effective_date,
            "run_id": run_id,
        },
    )
    if int(cur.rowcount) != 1:
        conn.rollback()
        raise RuntimeError("action_applied_status_update_count_mismatch")
    conn.commit()


def rebuild_official(start: dt.date, end: dt.date) -> None:
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools/index_engine/calc_index.py"),
            "--start",
            start.isoformat(),
            "--end",
            end.isoformat(),
            "--rebuild",
            "--strict",
            "--allow-reviewed-non-split-moves",
            "--no-preflight-self-heal",
        ],
        check=True,
        cwd=ROOT,
        timeout=int(os.getenv("SC_IDX_RECON_OFFICIAL_TIMEOUT_SEC", "1800")),
    )


def rebuild_portfolio(
    start: dt.date,
    end: dt.date,
    *,
    backup_tag: str | None = None,
    run_id: str | None = None,
) -> None:
    if not backup_tag or not run_id:
        raise ReconstructionError("portfolio_rebuild_requires_manifest_coordinates")
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools/index_engine/build_portfolio_analytics.py"),
            "--start",
            start.isoformat(),
            "--end",
            end.isoformat(),
            "--low-resource",
            "--backup-tag",
            backup_tag,
            "--run-id",
            run_id,
        ],
        check=True,
        cwd=ROOT,
        timeout=int(os.getenv("SC_IDX_RECON_PORTFOLIO_TIMEOUT_SEC", "1800")),
    )


def rebuild(start: dt.date, end: dt.date, *, backup_tag: str, run_id: str) -> None:
    """Compatibility wrapper preserving the documented rebuild order."""
    rebuild_official(start, end)
    rebuild_portfolio(start, end, backup_tag=backup_tag, run_id=run_id)


def verify_strict(start: dt.date, end: dt.date, run_id: str) -> None:
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools/index_engine/verify_index_integrity.py"),
            "--start",
            start.isoformat(),
            "--end",
            end.isoformat(),
            "--processing-run-id",
            run_id,
        ],
        check=True,
        cwd=ROOT,
        timeout=int(os.getenv("SC_IDX_RECON_VERIFY_TIMEOUT_SEC", "600")),
    )


def complete_reconstruction(
    *,
    rebuild_fn: Callable[[], None],
    verify_fn: Callable[[], None],
    mark_applied_fn: Callable[[], None],
) -> None:
    for stage, operation in (
        ("rebuild", rebuild_fn),
        ("strict_verification", verify_fn),
        ("mark_applied", mark_applied_fn),
    ):
        try:
            operation()
        except Exception as exc:
            raise ReconstructionError(f"{stage}_failed:{exc}") from exc


def rollback(conn, tag: str, start: dt.date, end: dt.date) -> list[str]:
    """Validate everything first, then restore the complete set in one transaction."""
    records = validate_backup_set(conn, tag, start, end)
    by_target = {record.target_object: record for record in records}
    restored: list[str] = []
    cur = conn.cursor()
    try:
        for target in reversed(list(BACKUP_OBJECTS.values())):
            record = by_target[target]
            date_column = _date_column(target)
            try:
                cur.execute(
                    f"DELETE FROM {target} WHERE {date_column} BETWEEN :start_date AND :end_date",
                    {"start_date": start, "end_date": end},
                )
                cur.execute(
                    f"INSERT INTO {target} SELECT * FROM {record.backup_object}",
                )
            except Exception as exc:
                raise RestorationError(f"restore_failed:{target}:{exc}") from exc
            restored.append(target)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return restored


def verify_restoration(conn, tag: str, start: dt.date, end: dt.date) -> None:
    """Verify the compensated target range against the validated manifest."""
    records = validate_backup_set(conn, tag, start, end)
    for record in records:
        count, minimum, maximum = _count_and_range(
            conn,
            record.target_object,
            _date_column(record.target_object),
            start,
            end,
        )
        if count != record.backup_row_count:
            raise RestorationError(f"restoration_row_count_mismatch:{record.target_object}")
        backup_count, backup_minimum, backup_maximum = _total_count_and_range(
            conn,
            record.backup_object,
            _date_column(record.target_object),
        )
        if (count, minimum, maximum) != (backup_count, backup_minimum, backup_maximum):
            raise RestorationError(f"restoration_range_mismatch:{record.target_object}")


def fetch_action_status(conn, args: argparse.Namespace, run_id: str) -> str:
    cur = conn.cursor()
    cur.execute(
        "SELECT confirmation_status FROM SC_IDX_CORPORATE_ACTIONS "
        "WHERE index_code='TECH100' AND ticker=:ticker AND action_type=:action_type "
        "AND effective_date=:effective_date AND processing_run_id=:run_id",
        {
            "ticker": args.ticker,
            "action_type": args.action_type,
            "effective_date": args.effective_date,
            "run_id": run_id,
        },
    )
    row = cur.fetchone()
    return str(row[0]) if row else "NOT_RECORDED"


def _rollback_with_fresh_connection(tag: str, start: dt.date, end: dt.date) -> None:
    with get_connection() as restore_conn:
        rollback(restore_conn, tag, start, end)


def _verify_restoration_with_fresh_connection(tag: str, start: dt.date, end: dt.date) -> None:
    with get_connection() as verify_conn:
        verify_restoration(verify_conn, tag, start, end)


def _fetch_action_status_with_fresh_connection(args: argparse.Namespace, run_id: str) -> str:
    with get_connection() as status_conn:
        return fetch_action_status(status_conn, args, run_id)


def _mark_action_applied_with_fresh_connection(args: argparse.Namespace, run_id: str) -> None:
    with get_connection() as action_conn:
        mark_action_applied(action_conn, args, run_id)


def emit_backup_context(
    context: ApplyContext,
    lines: list[str],
    *,
    printer: Callable[..., None] = print,
) -> None:
    context_lines = [
        f"backup_tag={context.tag}",
        f"backup_run_id={context.run_id}",
        f"repair_start={context.start.isoformat()}",
        f"repair_end={context.end.isoformat()}",
        f"rollback_command={context.rollback_command}",
    ]
    lines.extend(context_lines)
    _write_report(context.report_path, lines)
    for line in context_lines:
        printer(line, flush=True)


def execute_controlled_apply(
    conn,
    context: ApplyContext,
    stages: Iterable[ApplyStage],
    lines: list[str],
    *,
    rollback_fn: Callable[[], None] | None = None,
    verify_restoration_fn: Callable[[], None] | None = None,
    action_status_fn: Callable[[], str] | None = None,
    printer: Callable[..., None] = print,
) -> int:
    """Run every post-backup stage with manifest-backed compensation."""
    emit_backup_context(context, lines, printer=printer)

    def update_status(
        status: str,
        stage: str,
        *,
        failure_class: str | None = None,
        rollback_status: str | None = None,
    ) -> None:
        if context.status_path is None:
            return
        write_reconstruction_status(
            context.status_path,
            run_id=context.run_id,
            backup_tag=context.tag,
            revision=context.revision,
            repair_start=context.start.isoformat(),
            repair_end=context.end.isoformat(),
            stage=stage,
            stage_started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            last_completed_date=None,
            rows_processed=0,
            status=status,
            failure_class=failure_class,
            rollback_status=rollback_status,
        )

    update_status("PENDING", "backups_validated")
    rollback_fn = rollback_fn or (lambda: rollback(conn, context.tag, context.start, context.end))
    verify_restoration_fn = verify_restoration_fn or (
        lambda: verify_restoration(conn, context.tag, context.start, context.end)
    )
    action_status_fn = action_status_fn or (lambda: "NOT_RECORDED")
    mutation_started = False
    action_status = "NOT_RECORDED"
    success_output_start = len(lines)

    for stage in stages:
        try:
            update_status(
                "VERIFYING" if stage.name == "strict_verification" else "RUNNING",
                stage.name,
            )
            if stage.may_mutate:
                mutation_started = True
            stage.operation()
        except Exception as exc:
            failure = ApplyStageError(stage.name, exc)
            action_status_before_rollback = action_status
            if mutation_started:
                try:
                    update_status(
                        "COMPENSATING",
                        stage.name,
                        failure_class=type(exc).__name__,
                        rollback_status="RUNNING",
                    )
                except Exception as status_exc:
                    lines.append(f"status_update_error={status_exc}")
            parent_rollback_error: Exception | None = None
            try:
                conn.rollback()
            except Exception as rollback_exc:
                parent_rollback_error = rollback_exc
            automatic_rollback = "NOT_REQUIRED"
            rollback_error: Exception | None = None
            if mutation_started:
                try:
                    rollback_fn()
                    verify_restoration_fn()
                    automatic_rollback = "PASS"
                except Exception as restore_exc:
                    automatic_rollback = "FAIL"
                    rollback_error = restore_exc
            try:
                action_status = action_status_fn()
            except Exception as status_exc:
                lines.append(f"action_status_check_error={status_exc}")

            failure_lines = [
                "mode=APPLY_FAILED",
                "pipeline_timer=LEAVE_STOPPED",
                f"backup_tag={context.tag}",
                f"backup_run_id={context.run_id}",
                f"failing_stage={failure.stage}",
                f"original_error={failure.original}",
                f"action_status_before_rollback={action_status_before_rollback}",
                f"action_status={action_status}",
                f"automatic_rollback={automatic_rollback}",
            ]
            if parent_rollback_error is not None:
                failure_lines.append(f"parent_connection_rollback_error={parent_rollback_error}")
            if rollback_error is not None:
                failure_lines.append(f"rollback_error={rollback_error}")
            failure_lines.append(f"rollback_command={context.rollback_command}")
            try:
                update_status(
                    "ROLLED_BACK" if automatic_rollback == "PASS" else "FAILED",
                    stage.name,
                    failure_class=type(exc).__name__,
                    rollback_status=automatic_rollback,
                )
            except Exception as status_exc:
                failure_lines.append(f"status_update_error={status_exc}")
            lines.extend(failure_lines)
            _write_report(context.report_path, lines)
            for line in failure_lines:
                printer(line, flush=True)
            return 2
        if stage.action_status_after is not None:
            action_status = stage.action_status_after

    lines.extend(("mode=APPLY", "strict_verification=PASS", "action_status=APPLIED"))
    update_status("SUCCEEDED", "mark_action_applied")
    _write_report(context.report_path, lines)
    for line in lines[success_output_start:]:
        printer(line, flush=True)
    return 0


def dry_run(conn, args: argparse.Namespace) -> tuple[dt.date, list[str]]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS")
    max_day = args.end or _as_date(cur.fetchone()[0])
    cur.execute(
        """
        WITH p AS (
          SELECT ticker,trade_date,canon_adj_close_px,
                 LAG(canon_adj_close_px) OVER(PARTITION BY ticker ORDER BY trade_date) previous_px
          FROM SC_IDX_PRICES_CANON
        )
        SELECT COUNT(*),MIN(trade_date),MAX(trade_date) FROM p
        WHERE previous_px>0 AND ABS(canon_adj_close_px/previous_px-1)>.20
        """
    )
    candidate_count, candidate_min, candidate_max = cur.fetchone()
    lines = [
        "mode=DRY_RUN",
        "oracle_writes=0",
        f"rebuild_start={args.start.isoformat()}",
        f"rebuild_end={max_day.isoformat()}",
        f"price_candidates={candidate_count}",
        f"candidate_min={_as_date(candidate_min).isoformat() if candidate_min else ''}",
        f"candidate_max={_as_date(candidate_max).isoformat() if candidate_max else ''}",
    ]
    for table in BACKUP_OBJECTS.values():
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            lines.append(f"{table}_rows={cur.fetchone()[0]}")
        except Exception as exc:
            if "ORA-00942" in str(exc):
                lines.append(f"{table}_rows=UNAVAILABLE")
            else:
                raise
    return max_day, lines


def run_reconstruction_readiness(args: argparse.Namespace, end: dt.date) -> None:
    command = [
        sys.executable,
        str(ROOT / "tools/index_engine/reconstruction_readiness.py"),
        "--start",
        args.start.isoformat(),
        "--end",
        end.isoformat(),
        "--ticker",
        args.ticker,
        "--probe-missing-anchors",
        "--require-quiescent",
        "--rehearse-portfolio",
    ]
    if args.adjusted_price_csv:
        command.extend(("--adjusted-price-csv", str(args.adjusted_price_csv)))
    subprocess.run(
        command,
        check=True,
        cwd=ROOT,
        timeout=int(os.getenv("SC_IDX_RECON_READINESS_TIMEOUT_SEC", "600")),
    )


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    load_env_files()
    with get_connection() as conn:
        max_day, lines = dry_run(conn, args)
        if args.rollback_tag:
            restored = rollback(conn, args.rollback_tag, args.start, max_day)
            lines.extend(("mode=ROLLBACK", f"restored_objects={len(restored)}"))
        elif args.apply:
            if not all((args.ticker, args.effective_date, args.ratio, args.source_reference)):
                raise SystemExit("apply_requires_ticker_effective_date_ratio_source_reference")
            if bool(args.adjusted_price_csv) == bool(args.refresh_adjusted_history):
                raise SystemExit("apply_requires_exactly_one_adjusted_history_source")
            if args.reuse_backups and not args.run_id:
                raise SystemExit("backup_reuse_requires_run_id")
            if args.reuse_backups and not args.backup_tag:
                raise SystemExit("backup_reuse_requires_backup_tag")

            args.ticker = args.ticker.strip().upper()
            run_id = args.run_id or f"ca-{uuid.uuid4()}"
            tag = args.backup_tag or new_backup_tag()
            run_reconstruction_readiness(args, max_day)
            ensure_action_schema(conn)
            backups = create_backups(
                conn,
                tag,
                run_id,
                args.start,
                max_day,
                reuse=args.reuse_backups,
            )
            lines.append(f"backup_objects={len(backups)}")
            rollback_command = (
                "python3 tools/db_migrations/repair_sc_idx_corporate_actions.py "
                f"--rollback-tag {tag} --start {args.start.isoformat()} --end {max_day.isoformat()}"
            )
            report_path = args.report or (
                ROOT
                / "tools/audit/output/corporate_action_forensics"
                / f"repair_apply_{tag}.txt"
            )
            context = ApplyContext(
                tag,
                run_id,
                args.start,
                max_day,
                rollback_command,
                report_path,
                args.status_file,
                deployed_revision(),
            )
            state: dict[str, object] = {}
            stages: list[ApplyStage] = []

            if args.refresh_adjusted_history:
                def prepare_refresh() -> None:
                    before = fetch_adjusted_prices(conn, args.ticker, args.start, max_day)
                    validate_canonical_dates_backed_up(conn, backups, args.ticker, before)
                    state["before"] = before

                def refresh_prices() -> None:
                    subprocess.run(
                        [
                            sys.executable,
                            str(ROOT / "tools/index_engine/ingest_prices.py"),
                            "--start",
                            args.start.isoformat(),
                            "--end",
                            args.effective_date.isoformat(),
                            "--backfill",
                            "--tickers",
                            args.ticker,
                            "--max-provider-calls",
                            str(args.max_refresh_calls),
                        ],
                        check=True,
                        cwd=ROOT,
                    )

                def validate_refresh() -> None:
                    before = state["before"]
                    after = fetch_adjusted_prices(conn, args.ticker, args.start, max_day)
                    changed, changed_count, event_return = validate_refreshed_history(
                        before,
                        after,
                        effective_date=args.effective_date,
                        start=args.start,
                        end=max_day,
                    )
                    changed_dates = {
                        day for day in set(before) & set(after) if before[day] != after[day]
                    }
                    validate_canonical_dates_backed_up(
                        conn, backups, args.ticker, changed_dates
                    )
                    lines.extend(
                        (
                            f"earliest_material_change={changed.isoformat()}",
                            f"materially_changed_rows={changed_count}",
                            f"event_economic_return={event_return:.12g}",
                        )
                    )

                stages.extend(
                    (
                        ApplyStage("prepare_automated_refresh", prepare_refresh),
                        ApplyStage("automated_price_ingestion", refresh_prices, may_mutate=True),
                        ApplyStage("validate_refreshed_history", validate_refresh),
                    )
                )
            else:
                def prepare_csv_repair() -> None:
                    prices = load_adjusted_prices(
                        args.adjusted_price_csv, args.ticker, args.start, max_day
                    )
                    before = validate_price_update_scope(
                        conn, args.ticker, prices, args.start, max_day
                    )
                    validate_canonical_dates_backed_up(
                        conn, backups, args.ticker, prices
                    )
                    changed = earliest_material_change(before, prices)
                    if changed is None:
                        raise PriceRepairError("no_material_adjusted_price_change")
                    state.update(prices=prices, before=before)

                def update_csv_prices() -> None:
                    apply_prices(conn, args.ticker, state["prices"])

                def validate_csv_repair() -> None:
                    before = state["before"]
                    after = fetch_adjusted_prices(conn, args.ticker, args.start, max_day)
                    validate_canonical_dates_backed_up(
                        conn, backups, args.ticker, state["prices"]
                    )
                    changed, changed_count, event_return = validate_refreshed_history(
                        before,
                        after,
                        effective_date=args.effective_date,
                        start=args.start,
                        end=max_day,
                    )
                    lines.extend(
                        (
                            f"earliest_material_change={changed.isoformat()}",
                            f"materially_changed_rows={changed_count}",
                            f"event_economic_return={event_return:.12g}",
                        )
                    )

                stages.extend(
                    (
                        ApplyStage("prepare_csv_repair", prepare_csv_repair),
                        ApplyStage("csv_canonical_update", update_csv_prices, may_mutate=True),
                        ApplyStage("validate_csv_history", validate_csv_repair),
                    )
                )

            stages.extend(
                (
                    ApplyStage(
                        "confirm_action",
                        lambda: upsert_action_confirmed(conn, args, args.start, run_id),
                        may_mutate=True,
                        action_status_after="CONFIRMED",
                    ),
                    ApplyStage(
                        "rebuild_official_index",
                        lambda: rebuild_official(args.start, max_day),
                        may_mutate=True,
                    ),
                    ApplyStage(
                        "rebuild_portfolio_outputs",
                        lambda: rebuild_portfolio(
                            args.start,
                            max_day,
                            backup_tag=tag,
                            run_id=run_id,
                        ),
                        may_mutate=True,
                    ),
                    ApplyStage(
                        "strict_verification",
                        lambda: verify_strict(args.start, max_day, run_id),
                    ),
                    ApplyStage(
                        "mark_action_applied",
                        lambda: _mark_action_applied_with_fresh_connection(args, run_id),
                        may_mutate=True,
                        action_status_after="APPLIED",
                    ),
                )
            )
            return execute_controlled_apply(
                conn,
                context,
                stages,
                lines,
                rollback_fn=lambda: _rollback_with_fresh_connection(tag, args.start, max_day),
                verify_restoration_fn=lambda: _verify_restoration_with_fresh_connection(
                    tag, args.start, max_day
                ),
                action_status_fn=lambda: _fetch_action_status_with_fresh_connection(args, run_id),
            )

        _write_report(args.report, lines)
        for line in lines:
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

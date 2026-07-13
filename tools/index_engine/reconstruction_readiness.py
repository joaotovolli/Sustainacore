#!/usr/bin/env python3
"""Exhaustive, read-only TECH100 reconstruction readiness sweep."""

from __future__ import annotations

import argparse
import datetime as dt
import inspect
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Iterator, Mapping
from typing import Callable

ROOT = Path(os.getenv("SC_IDX_REPO_ROOT") or Path(__file__).resolve().parents[2])
sys.path[:0] = [str(ROOT), str(ROOT / "app")]

from db_helper import get_connection
from index_engine.oracle_runtime import configure_reconstruction_connection
from index_engine.corporate_actions import detect_split_candidate
from tools.db_migrations import repair_sc_idx_corporate_actions as repair
from tools.oracle.env_bootstrap import load_env_files

INDEX_CODE = "TECH100"
REQUIRED_OBJECTS = tuple(repair.BACKUP_OBJECTS.values()) + (
    repair.MANIFEST_TABLE,
    "SC_IDX_TRADING_DAYS",
    "TECH11_AI_GOV_ETH_INDEX",
)
SUBSTITUTE_QUALITIES = {"HISTORICAL", "CURRENT"}
STALE_QUALITIES = {"STALE"}


def _as_date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    return value if isinstance(value, dt.date) else None


@dataclass(frozen=True)
class AnchorIssue:
    rebalance_date: dt.date
    previous_date: dt.date
    ticker: str
    reason: str
    recoverable: bool | None = None


@dataclass(frozen=True)
class LargeMove:
    ticker: str
    previous_date: dt.date
    trade_date: dt.date
    return_1d: float
    classification: str


@dataclass
class ReadinessReport:
    start: dt.date
    end: dt.date
    rebalance_count: int = 0
    anchors: list[AnchorIssue] = field(default_factory=list)
    large_moves: list[LargeMove] = field(default_factory=list)
    unresolved_splits: list[LargeMove] = field(default_factory=list)
    missing_holdings_count: int = 0
    partial_source_dates: set[dt.date] = field(default_factory=set)
    schema_blockers: list[str] = field(default_factory=list)
    process_blockers: list[str] = field(default_factory=list)

    @property
    def missing_exact_anchor_count(self) -> int:
        return sum(issue.reason == "MISSING" for issue in self.anchors)

    @property
    def unrecoverable_anchor_count(self) -> int:
        return sum(
            issue.reason == "MISSING" and issue.recoverable is not True
            for issue in self.anchors
        )

    @property
    def stale_anchor_count(self) -> int:
        return sum(issue.reason == "STALE" for issue in self.anchors)

    @property
    def substitute_anchor_count(self) -> int:
        return sum(issue.reason == "SUBSTITUTE" for issue in self.anchors)

    @property
    def passed(self) -> bool:
        return not any(
            (
                self.unrecoverable_anchor_count,
                self.stale_anchor_count,
                self.substitute_anchor_count,
                len(self.unresolved_splits),
                self.missing_holdings_count,
                len(self.partial_source_dates),
                len(self.schema_blockers),
                len(self.process_blockers),
            )
        )


class SelectOnlyCursor:
    def __init__(self, cursor):
        self._cursor = cursor
        self._fallback_consumed = False

    def execute(self, sql: str, binds: dict[str, object] | None = None):
        normalized = sql.lstrip().upper()
        if not normalized.startswith(("SELECT", "WITH")):
            raise RuntimeError("readiness_non_select_sql_rejected")
        self._fallback_consumed = False
        return self._cursor.execute(sql, binds or {})

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchmany(self, size: int | None = None):
        if hasattr(self._cursor, "fetchmany"):
            return self._cursor.fetchmany(size) if size is not None else self._cursor.fetchmany()
        if self._fallback_consumed:
            return []
        self._fallback_consumed = True
        rows = self._cursor.fetchall()
        return rows[:size] if size is not None else rows

    @property
    def arraysize(self) -> int:
        return getattr(self._cursor, "arraysize", 100)

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if hasattr(self._cursor, "arraysize"):
            self._cursor.arraysize = value


def _objects(cur: SelectOnlyCursor) -> set[str]:
    cur.execute(
        "SELECT object_name FROM user_objects WHERE object_type IN ('TABLE','VIEW') "
        "AND object_name IN (" + ",".join(f"'{name}'" for name in REQUIRED_OBJECTS) + ")"
    )
    return {str(row[0]).upper() for row in cur.fetchall()}


def _schema_blockers(conn, cur: SelectOnlyCursor) -> list[str]:
    existing = _objects(cur)
    blockers = [f"missing_object:{name}" for name in REQUIRED_OBJECTS if name not in existing]
    for target in repair.BACKUP_OBJECTS.values():
        if target not in existing:
            continue
        try:
            repair._date_column(target)
        except KeyError:
            blockers.append(f"missing_backup_date_column:{target}")
    source = inspect.getsource(repair.create_backups).upper()
    if "CREATE TABLE" not in source or "BETWEEN :START_DATE" in source:
        blockers.append("backup_ctas_not_oracle_ddl_safe")
    if repair.MANIFEST_TABLE in existing:
        cur.execute(
            f"SELECT backup_tag FROM {repair.MANIFEST_TABLE} "
            "WHERE validation_status='VALIDATED' GROUP BY backup_tag "
            "HAVING COUNT(DISTINCT target_object)=:target_count "
            "ORDER BY MAX(created_at) DESC FETCH FIRST 1 ROWS ONLY",
            {"target_count": len(repair.BACKUP_OBJECTS)},
        )
        row = cur.fetchone()
        if not row:
            blockers.append("validated_restore_shape_evidence_missing")
        else:
            tag = str(row[0])
            cur.execute(
                f"SELECT target_object,backup_object FROM {repair.MANIFEST_TABLE} "
                "WHERE backup_tag=:tag ORDER BY target_object",
                {"tag": tag},
            )
            for target, backup in cur.fetchall():
                if not repair._columns_compatible(conn, str(target), str(backup)):
                    blockers.append(f"restore_shape_mismatch:{target}")
    return sorted(set(blockers))


def _trading_days(cur: SelectOnlyCursor, start: dt.date, end: dt.date) -> list[dt.date]:
    cur.execute(
        "SELECT trade_date FROM SC_IDX_TRADING_DAYS "
        "WHERE trade_date BETWEEN :start_date AND :end_date ORDER BY trade_date",
        {"start_date": start, "end_date": end},
    )
    return [day for row in cur.fetchall() if (day := _as_date(row[0]))]


def _universes(cur: SelectOnlyCursor, end: dt.date) -> dict[dt.date, list[str]]:
    cur.execute(
        "SELECT port_date,ticker FROM ("
        "SELECT port_date,ticker,ROW_NUMBER() OVER (PARTITION BY port_date "
        "ORDER BY port_weight DESC,rank_index) rn FROM TECH11_AI_GOV_ETH_INDEX "
        "WHERE port_date<=:end_date AND port_weight>0 AND ticker IS NOT NULL) "
        "WHERE rn<=25 ORDER BY port_date,rn",
        {"end_date": end},
    )
    result: dict[dt.date, list[str]] = {}
    for port_date, ticker in cur.fetchall():
        day = _as_date(port_date)
        if day:
            result.setdefault(day, []).append(str(ticker).strip().upper())
    return result


def _active_port_date(port_dates: list[dt.date], day: dt.date) -> dt.date | None:
    active = None
    for port_date in port_dates:
        if port_date > day:
            break
        active = port_date
    return active


def _iter_prices_by_day(
    cur: SelectOnlyCursor,
    *,
    start: dt.date,
    end: dt.date,
    tickers: list[str],
    planned: Mapping[dt.date, float] | None = None,
    planned_ticker: str | None = None,
    fetch_size: int = 500,
) -> Iterator[tuple[dt.date, dict[str, tuple[float | None, str]]]]:
    """Stream bounded per-date price maps instead of retaining the full matrix."""
    if not tickers:
        return
    placeholders = ",".join(f":ticker{i}" for i in range(len(tickers)))
    binds: dict[str, object] = {"start_date": start, "end_date": end}
    binds.update({f"ticker{i}": ticker for i, ticker in enumerate(tickers)})
    cur.execute(
        "SELECT trade_date,ticker,canon_adj_close_px,quality FROM SC_IDX_PRICES_CANON "
        "WHERE trade_date BETWEEN :start_date AND :end_date "
        f"AND ticker IN ({placeholders}) ORDER BY trade_date,ticker",
        binds,
    )
    cur.arraysize = fetch_size
    current_day: dt.date | None = None
    current: dict[str, tuple[float | None, str]] = {}
    while True:
        batch = cur.fetchmany(fetch_size)
        if not batch:
            break
        for trade_date, ticker, price, quality in batch:
            day = _as_date(trade_date)
            if day is None:
                continue
            if current_day is not None and day != current_day:
                if planned and planned_ticker and current_day in planned:
                    current[planned_ticker] = (float(planned[current_day]), "PLANNED_REPAIR")
                yield current_day, current
                current = {}
            current_day = day
            current[str(ticker).strip().upper()] = (
                float(price) if price is not None else None,
                str(quality or "").upper(),
            )
    if current_day is not None:
        if planned and planned_ticker and current_day in planned:
            current[planned_ticker] = (float(planned[current_day]), "PLANNED_REPAIR")
        yield current_day, current


def _price_maps_for_trading_days(
    cur: SelectOnlyCursor,
    *,
    trading_days: list[dt.date],
    tickers: list[str],
    planned: Mapping[dt.date, float] | None = None,
    planned_ticker: str | None = None,
) -> Iterator[tuple[dt.date, dict[str, tuple[float | None, str]]]]:
    if not trading_days:
        return
    streamed = iter(
        _iter_prices_by_day(
            cur,
            start=trading_days[0],
            end=trading_days[-1],
            tickers=tickers,
            planned=planned,
            planned_ticker=planned_ticker,
        )
    )
    next_item = next(streamed, None)
    for day in trading_days:
        while next_item is not None and next_item[0] < day:
            next_item = next(streamed, None)
        if next_item is not None and next_item[0] == day:
            price_map = next_item[1]
            next_item = next(streamed, None)
        else:
            price_map = {}
            if planned and planned_ticker and day in planned:
                price_map[planned_ticker] = (float(planned[day]), "PLANNED_REPAIR")
        yield day, price_map


def _confirmed_actions(cur: SelectOnlyCursor, start: dt.date, end: dt.date) -> set[tuple[str, dt.date]]:
    cur.execute(
        "SELECT ticker,effective_date FROM SC_IDX_CORPORATE_ACTIONS "
        "WHERE effective_date BETWEEN :start_date AND :end_date "
        "AND confirmation_status IN ('CONFIRMED','APPLIED')",
        {"start_date": start, "end_date": end},
    )
    return {
        (str(ticker).strip().upper(), day)
        for ticker, value in cur.fetchall()
        if (day := _as_date(value))
    }


def _probe_anchor(ticker: str, day: dt.date) -> float | None:
    from providers.market_data_provider import fetch_eod_prices, fetch_single_day_bar

    rows = fetch_eod_prices([ticker], day.isoformat(), day.isoformat())
    if not rows:
        window = min(5000, max(10, (dt.date.today() - day).days + 10))
        rows = fetch_single_day_bar(ticker, day, window=window)
    for row in rows:
        if str(row.get("ticker") or "").strip().upper() != ticker:
            continue
        if str(row.get("trade_date") or "")[:10] != day.isoformat():
            continue
        value = row.get("adj_close") if row.get("adj_close") is not None else row.get("close")
        if value is not None and float(value) > 0:
            return float(value)
    return None


def _process_blockers() -> list[str]:
    blockers = []
    for unit in ("sc-idx-pipeline.timer", "sc-idx-pipeline.service"):
        result = subprocess.run(
            ["systemctl", "is-active", unit], capture_output=True, text=True, check=False, timeout=5
        )
        if result.stdout.strip() == "active":
            blockers.append(f"active_unit:{unit}")
    result = subprocess.run(
        ["pgrep", "-f", "repair_sc_idx_corporate_actions|calc_index.py --rebuild|run_pipeline.py"],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    ignored_pids: set[str] = set()
    pid = os.getpid()
    while pid > 1 and str(pid) not in ignored_pids:
        ignored_pids.add(str(pid))
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            pid = int(stat.rsplit(")", 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    matches = [pid for pid in result.stdout.split() if pid not in ignored_pids]
    if matches:
        blockers.append("conflicting_sc_idx_process")
    return blockers


def _portfolio_rehearsal(start: dt.date, end: dt.date) -> bool:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools/index_engine/build_portfolio_analytics.py"),
            "--start",
            start.isoformat(),
            "--end",
            end.isoformat(),
            "--dry-run",
            "--low-resource",
            "--skip-preflight",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
        cwd=ROOT,
    )
    return result.returncode == 0


def collect_readiness(
    conn,
    args: argparse.Namespace,
    *,
    anchor_probe: Callable[[str, dt.date], float | bool | None] | None = None,
) -> ReadinessReport:
    cur = SelectOnlyCursor(conn.cursor())
    end = args.end
    if end is None:
        cur.execute("SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS")
        end = _as_date(cur.fetchone()[0])
    if end is None or end < args.start:
        raise RuntimeError("invalid_reconstruction_range")
    report = ReadinessReport(args.start, end)
    report.schema_blockers.extend(_schema_blockers(conn, cur))
    if args.rehearse_portfolio and not _portfolio_rehearsal(args.start, end):
        report.schema_blockers.append("portfolio_dry_run_rehearsal_failed")
    if args.require_quiescent:
        report.process_blockers.extend(_process_blockers())

    trading_days = _trading_days(cur, args.start, end)
    universes = _universes(cur, end)
    port_dates = sorted(universes)
    all_tickers = sorted({ticker for values in universes.values() for ticker in values})
    planned: dict[dt.date, float] = {}
    if args.adjusted_price_csv:
        planned = repair.load_adjusted_prices(args.adjusted_price_csv, args.ticker, args.start, end)
    confirmed = _confirmed_actions(cur, args.start, end)

    previous_port = None
    previous_day = None
    previous_prices: dict[str, tuple[float | None, str]] = {}
    threshold = args.max_abs_constituent_return
    probe = anchor_probe if anchor_probe is not None else (_probe_anchor if args.probe_missing_anchors else None)
    for day, current_prices in _price_maps_for_trading_days(
        cur,
        trading_days=trading_days,
        tickers=all_tickers,
        planned=planned,
        planned_ticker=args.ticker,
    ):
        port_date = _active_port_date(port_dates, day)
        if port_date is None:
            report.missing_holdings_count += 1
            previous_day = day
            previous_prices = current_prices
            continue
        tickers = universes.get(port_date, [])
        if len(tickers) != 25:
            report.missing_holdings_count += 1
        if port_date != previous_port:
            report.rebalance_count += 1
            if previous_day is not None:
                for ticker in tickers:
                    price, quality = previous_prices.get(ticker, (None, ""))
                    if price is None:
                        recoverable = None
                        if probe:
                            try:
                                recovered = probe(ticker, previous_day)
                                recoverable = bool(recovered)
                                if (
                                    recoverable
                                    and isinstance(recovered, (int, float))
                                    and not isinstance(recovered, bool)
                                ):
                                    previous_prices[ticker] = (float(recovered), "PROBED_REAL")
                            except Exception:
                                recoverable = False
                        report.anchors.append(
                            AnchorIssue(day, previous_day, ticker, "MISSING", recoverable)
                        )
                    elif quality in STALE_QUALITIES:
                        report.anchors.append(AnchorIssue(day, previous_day, ticker, "STALE"))
                    elif quality in SUBSTITUTE_QUALITIES:
                        report.anchors.append(AnchorIssue(day, previous_day, ticker, "SUBSTITUTE"))
            previous_port = port_date
        if any(current_prices.get(ticker, (None, ""))[0] is None for ticker in tickers):
            report.partial_source_dates.add(day)
        if previous_day is not None:
            for ticker in tickers:
                p0 = previous_prices.get(ticker, (None, ""))[0]
                p1 = current_prices.get(ticker, (None, ""))[0]
                if p0 is None or p1 is None or p0 <= 0 or p1 <= 0:
                    continue
                return_1d = p1 / p0 - 1.0
                if abs(return_1d) <= threshold:
                    continue
                candidate = detect_split_candidate(
                    ticker=ticker,
                    effective_date=day,
                    previous_price=p0,
                    current_price=p1,
                )
                if candidate and (ticker, day) not in confirmed:
                    classification = "UNRESOLVED_SPLIT_CANDIDATE"
                elif candidate:
                    classification = "CONFIRMED_CORPORATE_ACTION"
                else:
                    classification = "AUDITED_NON_SPLIT_MOVE"
                move = LargeMove(ticker, previous_day, day, return_1d, classification)
                report.large_moves.append(move)
                if classification == "UNRESOLVED_SPLIT_CANDIDATE":
                    report.unresolved_splits.append(move)
        previous_day = day
        previous_prices = current_prices
    conn.rollback()
    return report


def print_report(report: ReadinessReport) -> None:
    print("mode=RECONSTRUCTION_READINESS")
    print("oracle_writes=0")
    print(f"range_start={report.start.isoformat()}")
    print(f"range_end={report.end.isoformat()}")
    print(f"rebalance_count={report.rebalance_count}")
    print(f"missing_exact_anchor_count={report.missing_exact_anchor_count}")
    print(f"unrecoverable_anchor_count={report.unrecoverable_anchor_count}")
    print(f"stale_anchor_count={report.stale_anchor_count}")
    print(f"substitute_anchor_count={report.substitute_anchor_count}")
    print(f"large_move_count={len(report.large_moves)}")
    print(f"unresolved_split_candidate_count={len(report.unresolved_splits)}")
    print(f"missing_holdings_count={report.missing_holdings_count}")
    print(f"partial_source_date_count={len(report.partial_source_dates)}")
    print(f"schema_blocker_count={len(report.schema_blockers)}")
    for issue in sorted(report.anchors, key=lambda item: (item.previous_date, item.ticker)):
        status = "PASS" if issue.recoverable else "FAIL" if issue.recoverable is False else "NOT_CHECKED"
        print(
            f"anchor_issue:ticker={issue.ticker} previous_date={issue.previous_date.isoformat()} "
            f"rebalance_date={issue.rebalance_date.isoformat()} reason={issue.reason} recoverable={status}"
        )
    for move in sorted(report.large_moves, key=lambda item: (item.trade_date, item.ticker)):
        print(
            f"large_move:ticker={move.ticker} previous_date={move.previous_date.isoformat()} "
            f"trade_date={move.trade_date.isoformat()} return_1d={move.return_1d:.8f} "
            f"classification={move.classification}"
        )
    for blocker in report.schema_blockers + report.process_blockers:
        print(f"readiness_blocker={blocker}")
    print(f"overall_status={'PASS' if report.passed else 'FAIL'}")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Read-only TECH100 reconstruction readiness sweep")
    result.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2025, 1, 2))
    result.add_argument("--end", type=dt.date.fromisoformat)
    result.add_argument("--ticker", default="CRWD")
    result.add_argument("--adjusted-price-csv", type=Path)
    result.add_argument("--probe-missing-anchors", action="store_true")
    result.add_argument("--require-quiescent", action="store_true")
    result.add_argument("--rehearse-portfolio", action="store_true")
    result.add_argument(
        "--max-abs-constituent-return",
        type=float,
        default=float(os.getenv("SC_IDX_MAX_ABS_CONSTITUENT_RETURN", "0.20")),
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    load_env_files()
    with configure_reconstruction_connection(get_connection()) as conn:
        report = collect_readiness(conn, args)
    print_report(report)
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())

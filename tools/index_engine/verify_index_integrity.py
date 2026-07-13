#!/usr/bin/env python3
"""Strictly verify TECH100 reconstruction invariants without changing Oracle."""

from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import sys
from pathlib import Path

ROOT = Path(os.getenv("SC_IDX_REPO_ROOT") or Path(__file__).resolve().parents[2])
sys.path[:0] = [str(ROOT), str(ROOT / "app")]

from db_helper import get_connection
from index_engine.oracle_runtime import configure_reconstruction_connection
from index_engine.index_integrity import IntegrityCheck, audit_rebalance, maximum_check
from tools.oracle.env_bootstrap import load_env_files

INDEX_CODE = "TECH100"
CALC_TABLES = (
    "SC_IDX_LEVELS",
    "SC_IDX_CONSTITUENT_DAILY",
    "SC_IDX_CONTRIBUTION_DAILY",
    "SC_IDX_STATS_DAILY",
)
PORTFOLIO_DAILY_TABLES = (
    "SC_IDX_PORTFOLIO_ANALYTICS_DAILY",
    "SC_IDX_PORTFOLIO_POSITION_DAILY",
)


def _as_date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    return value if isinstance(value, dt.date) else None


def _fetch_scalar(cur, sql: str, binds: dict[str, object] | None = None):
    cur.execute(sql, binds or {})
    row = cur.fetchone()
    return row[0] if row else None


def _max_residual_check(
    cur,
    *,
    name: str,
    sql: str,
    binds: dict[str, object],
    tolerance: float,
    has_ticker: bool,
) -> IntegrityCheck:
    cur.execute(sql, binds)
    row = cur.fetchone()
    if not row:
        return IntegrityCheck(name, "NO_ROWS", tolerance, False, detail="required_rows_missing")
    day = _as_date(row[0])
    ticker = str(row[1]) if has_ticker and row[1] is not None else None
    value_index = 2 if has_ticker else 1
    value = float(row[value_index] or 0)
    return maximum_check(name, value, tolerance, date=day, ticker=ticker)


def _rebalance_checks(
    cur,
    *,
    start: dt.date,
    end: dt.date,
    bridge_tolerance: float,
    anchor_tolerance: float,
) -> list[IntegrityCheck]:
    cur.execute(
        "SELECT DISTINCT rebalance_date FROM SC_IDX_HOLDINGS "
        "WHERE index_code=:index_code AND rebalance_date BETWEEN :start_date AND :end_date "
        "ORDER BY rebalance_date",
        {"index_code": INDEX_CODE, "start_date": start, "end_date": end},
    )
    rebalance_dates = [_as_date(row[0]) for row in cur.fetchall()]
    maximum_bridge = (0.0, None)
    maximum_anchor = (0.0, None)
    total_missing = 0
    total_stale = 0
    stale_quality = 0
    for rebalance_date in rebalance_dates:
        previous_date = _as_date(
            _fetch_scalar(
                cur,
                "SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS WHERE trade_date<:rebalance_date",
                {"rebalance_date": rebalance_date},
            )
        )
        if previous_date is None:
            continue
        previous_level = _fetch_scalar(
            cur,
            "SELECT level_tr FROM SC_IDX_LEVELS WHERE index_code=:index_code AND trade_date=:trade_date",
            {"index_code": INDEX_CODE, "trade_date": previous_date},
        )
        previous_divisor = _fetch_scalar(
            cur,
            "SELECT divisor FROM SC_IDX_DIVISOR WHERE index_code=:index_code AND effective_date=("
            "SELECT MAX(effective_date) FROM SC_IDX_DIVISOR WHERE index_code=:index_code "
            "AND effective_date<:rebalance_date)",
            {"index_code": INDEX_CODE, "rebalance_date": rebalance_date},
        )
        new_divisor = _fetch_scalar(
            cur,
            "SELECT divisor FROM SC_IDX_DIVISOR WHERE index_code=:index_code AND effective_date=:rebalance_date",
            {"index_code": INDEX_CODE, "rebalance_date": rebalance_date},
        )
        cur.execute(
            "SELECT ticker,target_weight,shares FROM SC_IDX_HOLDINGS "
            "WHERE index_code=:index_code AND rebalance_date=:rebalance_date",
            {"index_code": INDEX_CODE, "rebalance_date": rebalance_date},
        )
        holdings = cur.fetchall()
        target_weights = {str(ticker): float(weight) for ticker, weight, _ in holdings}
        shares = {str(ticker): float(value) for ticker, _, value in holdings}
        cur.execute(
            "SELECT ticker,canon_adj_close_px FROM SC_IDX_PRICES_CANON "
            "WHERE trade_date=:trade_date AND ticker IN ("
            "SELECT ticker FROM SC_IDX_HOLDINGS WHERE index_code=:index_code "
            "AND rebalance_date=:rebalance_date)",
            {
                "trade_date": previous_date,
                "index_code": INDEX_CODE,
                "rebalance_date": rebalance_date,
            },
        )
        prices = {str(ticker): float(price) for ticker, price in cur.fetchall() if price is not None}
        if previous_level is None or previous_divisor is None or new_divisor is None:
            total_missing += len(shares)
            continue
        audit = audit_rebalance(
            previous_level=float(previous_level),
            previous_divisor=float(previous_divisor),
            new_divisor=float(new_divisor),
            target_weights=target_weights,
            shares=shares,
            exact_previous_prices=prices,
            anchor_relative_tolerance=anchor_tolerance,
        )
        total_missing += audit.missing_price_count
        total_stale += audit.stale_anchor_count
        if abs(audit.bridge_residual) > abs(maximum_bridge[0]):
            maximum_bridge = (audit.bridge_residual, rebalance_date)
        if audit.maximum_anchor_residual > maximum_anchor[0]:
            maximum_anchor = (audit.maximum_anchor_residual, rebalance_date)
        stale_quality += int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(*) FROM SC_IDX_CONSTITUENT_DAILY WHERE trade_date=:rebalance_date "
                "AND UPPER(NVL(price_quality,'UNKNOWN')) IN ('STALE','HISTORICAL','CURRENT')",
                {"rebalance_date": rebalance_date},
            )
            or 0
        )
    return [
        maximum_check(
            "rebalance_bridge_max_residual",
            maximum_bridge[0],
            bridge_tolerance,
            date=maximum_bridge[1],
        ),
        IntegrityCheck("rebalance_missing_exact_prices", total_missing, 0, total_missing == 0),
        IntegrityCheck(
            "rebalance_stale_anchor_count",
            total_stale,
            0,
            total_stale == 0,
            date=maximum_anchor[1],
            detail=f"maximum_relative_residual={maximum_anchor[0]:.12g}",
        ),
        IntegrityCheck("rebalance_substitute_quality_count", stale_quality, 0, stale_quality == 0),
    ]


def collect_checks(conn, args: argparse.Namespace) -> list[IntegrityCheck]:
    cur = conn.cursor()
    range_binds = {"start_date": args.start, "end_date": args.end}
    binds = {**range_binds, "index_code": INDEX_CODE}
    checks = [
        _max_residual_check(
            cur,
            name="contribution_max_residual",
            sql=(
                "SELECT trade_date,residual FROM (SELECT s.trade_date,"
                "s.ret_1d-NVL(c.total_contribution,0) residual FROM SC_IDX_STATS_DAILY s "
                "LEFT JOIN (SELECT trade_date,SUM(contribution) total_contribution "
                "FROM SC_IDX_CONTRIBUTION_DAILY GROUP BY trade_date) c ON c.trade_date=s.trade_date "
                "WHERE s.trade_date BETWEEN :start_date AND :end_date AND s.ret_1d IS NOT NULL "
                "ORDER BY ABS(s.ret_1d-NVL(c.total_contribution,0)) DESC) WHERE ROWNUM=1"
            ),
            binds=range_binds,
            tolerance=args.tolerance,
            has_ticker=False,
        ),
        _max_residual_check(
            cur,
            name="market_value_max_residual",
            sql=(
                "SELECT trade_date,ticker,residual FROM (SELECT trade_date,ticker,"
                "market_value-shares*price_used residual FROM SC_IDX_CONSTITUENT_DAILY "
                "WHERE trade_date BETWEEN :start_date AND :end_date "
                "ORDER BY ABS(market_value-shares*price_used) DESC) WHERE ROWNUM=1"
            ),
            binds=range_binds,
            tolerance=args.tolerance,
            has_ticker=True,
        ),
        _max_residual_check(
            cur,
            name="maximum_unexplained_index_return",
            sql=(
                "SELECT trade_date,ret_1d FROM (SELECT trade_date,ret_1d FROM SC_IDX_STATS_DAILY "
                "WHERE trade_date BETWEEN :start_date AND :end_date AND ret_1d IS NOT NULL "
                "ORDER BY ABS(ret_1d) DESC) WHERE ROWNUM=1"
            ),
            binds=range_binds,
            tolerance=args.max_abs_index_return,
            has_ticker=False,
        ),
    ]
    checks.extend(
        _rebalance_checks(
            cur,
            start=args.start,
            end=args.end,
            bridge_tolerance=args.rebalance_tolerance,
            anchor_tolerance=args.anchor_tolerance,
        )
    )

    split_binds = {"effective_date": args.split_effective_date, "ticker": args.split_ticker}
    previous_date = _as_date(
        _fetch_scalar(
            cur,
            "SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS WHERE trade_date<:effective_date",
            {"effective_date": args.split_effective_date},
        )
    )
    cur.execute(
        "SELECT ret_1d FROM SC_IDX_CONTRIBUTION_DAILY WHERE ticker=:ticker "
        "AND trade_date=:effective_date",
        split_binds,
    )
    row = cur.fetchone()
    split_return = float(row[0]) if row else math.inf
    checks.append(
        maximum_check(
            "split_constituent_return",
            split_return,
            args.max_split_economic_return,
            date=args.split_effective_date,
            ticker=args.split_ticker,
        )
    )
    shares_before = _fetch_scalar(
        cur,
        "SELECT shares FROM SC_IDX_CONSTITUENT_DAILY WHERE ticker=:ticker AND trade_date=:trade_date",
        {"ticker": args.split_ticker, "trade_date": previous_date},
    )
    shares_after = _fetch_scalar(
        cur,
        "SELECT shares FROM SC_IDX_CONSTITUENT_DAILY WHERE ticker=:ticker AND trade_date=:trade_date",
        {"ticker": args.split_ticker, "trade_date": args.split_effective_date},
    )
    share_ratio = float(shares_after) / float(shares_before) if shares_before and shares_after else math.inf
    checks.append(
        maximum_check(
            "split_share_ratio_residual",
            share_ratio - 1.0,
            args.tolerance,
            date=args.split_effective_date,
            ticker=args.split_ticker,
            detail=f"share_ratio={share_ratio:.12g}",
        )
    )

    max_dates: dict[str, dt.date | None] = {}
    for table in CALC_TABLES + PORTFOLIO_DAILY_TABLES:
        max_dates[table] = _as_date(
            _fetch_scalar(
                cur,
                f"SELECT MAX(trade_date) FROM {table} WHERE trade_date BETWEEN :start_date AND :end_date",
                {"start_date": args.start, "end_date": args.end},
            )
        )
    calc_dates = {max_dates[table] for table in CALC_TABLES}
    calc_max = max_dates["SC_IDX_LEVELS"]
    checks.append(
        IntegrityCheck(
            "calc_owned_freshness",
            ",".join(f"{table}:{max_dates[table]}" for table in CALC_TABLES),
            args.end.isoformat(),
            len(calc_dates) == 1 and calc_max == args.end,
        )
    )
    portfolio_ok = all(max_dates[table] == calc_max for table in PORTFOLIO_DAILY_TABLES)
    required_opt_date = _as_date(
        _fetch_scalar(
            cur,
            "SELECT MAX(rebalance_date) FROM SC_IDX_CONSTITUENT_DAILY WHERE trade_date<=:end_date",
            {"end_date": args.end},
        )
    )
    opt_max = _as_date(_fetch_scalar(cur, "SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_OPT_INPUTS"))
    portfolio_ok = portfolio_ok and required_opt_date is not None and opt_max is not None and opt_max >= required_opt_date
    checks.append(
        IntegrityCheck(
            "portfolio_owned_freshness",
            f"daily={','.join(str(max_dates[t]) for t in PORTFOLIO_DAILY_TABLES)};opt={opt_max}",
            f"daily={calc_max};opt>={required_opt_date}",
            portfolio_ok,
        )
    )

    partial_count = int(
        _fetch_scalar(
            cur,
            """
            SELECT COUNT(*) FROM (
              SELECT l.trade_date,
                     (SELECT COUNT(*) FROM SC_IDX_CONSTITUENT_DAILY c WHERE c.trade_date=l.trade_date) constituent_count,
                     (SELECT COUNT(*) FROM SC_IDX_CONTRIBUTION_DAILY d WHERE d.trade_date=l.trade_date) contribution_count,
                     (SELECT COUNT(*) FROM SC_IDX_HOLDINGS h WHERE h.index_code=:index_code
                       AND h.rebalance_date=(SELECT MAX(h2.rebalance_date) FROM SC_IDX_HOLDINGS h2
                         WHERE h2.index_code=:index_code AND h2.rebalance_date<=l.trade_date)) expected_count,
                     MIN(l.trade_date) OVER () first_date
              FROM SC_IDX_LEVELS l
              WHERE l.index_code=:index_code AND l.trade_date BETWEEN :start_date AND :end_date
            ) WHERE constituent_count<>expected_count
               OR (trade_date>first_date AND contribution_count<>expected_count)
               OR (trade_date=first_date AND contribution_count<>0)
            """,
            binds,
        )
        or 0
    )
    checks.append(IntegrityCheck("partial_output_dates", partial_count, 0, partial_count == 0))

    try:
        unresolved = int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(*) FROM SC_IDX_CORPORATE_ACTIONS WHERE effective_date BETWEEN :start_date AND :end_date "
                "AND confirmation_status IN ('PENDING','CONFIRMED') "
                "AND (:processing_run_id IS NULL OR processing_run_id<>:processing_run_id)",
                {
                    "start_date": args.start,
                    "end_date": args.end,
                    "processing_run_id": args.processing_run_id,
                },
            )
            or 0
        )
    except Exception as exc:
        if "ORA-00942" not in str(exc):
            raise
        unresolved = -1
    checks.append(
        IntegrityCheck(
            "unresolved_corporate_actions",
            unresolved,
            0,
            unresolved == 0,
            detail="-1 means corporate-action object is unavailable",
        )
    )
    return checks


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Strictly verify TECH100 reconstruction invariants")
    result.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2025, 1, 2))
    result.add_argument("--end", type=dt.date.fromisoformat, required=True)
    result.add_argument("--tolerance", type=float, default=1e-6)
    result.add_argument("--rebalance-tolerance", type=float, default=1e-6)
    result.add_argument("--anchor-tolerance", type=float, default=1e-8)
    result.add_argument(
        "--max-abs-index-return",
        type=float,
        default=float(os.getenv("SC_IDX_MAX_ABS_INDEX_RETURN", "0.20")),
    )
    result.add_argument("--max-split-economic-return", type=float, default=0.20)
    result.add_argument("--split-ticker", default="CRWD")
    result.add_argument("--split-effective-date", type=dt.date.fromisoformat, default=dt.date(2026, 7, 2))
    result.add_argument("--processing-run-id")
    result.add_argument("--allow-known-contamination", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    load_env_files()
    with configure_reconstruction_connection(get_connection()) as conn:
        checks = collect_checks(conn, args)
    for check in checks:
        context = ""
        if check.date:
            context += f" date={check.date.isoformat()}"
        if check.ticker:
            context += f" ticker={check.ticker}"
        if check.detail:
            context += f" detail={check.detail}"
        print(
            f"{check.name}: value={check.value} tolerance={check.tolerance} "
            f"status={'PASS' if check.passed else 'FAIL'}{context}"
        )
    failed = [check for check in checks if not check.passed]
    print(f"integrity_verification={'PASS' if not failed else 'FAIL'} failed_checks={len(failed)}")
    if failed and not args.allow_known_contamination:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

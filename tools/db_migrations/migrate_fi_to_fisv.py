from __future__ import annotations

import argparse
import datetime as dt
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.index_engine.env_loader import load_default_env
from db_helper import get_connection

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "tools" / "audit" / "output"
REPORT_PATH = OUTPUT_DIR / "fi_to_fisv_migration_report.md"
COLLISIONS_PATH = OUTPUT_DIR / "fi_to_fisv_collisions.csv"

OLD_TICKER = "FI"
NEW_TICKER = "FISV"

TICKER_COLUMNS = [
    "TICKER",
    "SYMBOL",
    "RIC",
    "UNDERLYING",
    "SECURITY_ID",
    "SECURITY",
    "CUSIP",
    "ISIN",
]

KEY_OVERRIDES: dict[str, list[str]] = {
    "SC_IDX_PRICES_RAW": ["TRADE_DATE", "PROVIDER"],
    "SC_IDX_PRICES_CANON": ["TRADE_DATE"],
    "SC_IDX_CONSTITUENT_DAILY": ["TRADE_DATE"],
    "SC_IDX_CONTRIBUTION_DAILY": ["TRADE_DATE"],
    "SC_IDX_HOLDINGS": ["INDEX_CODE", "REBALANCE_DATE"],
    "SC_IDX_IMPUTATIONS": ["INDEX_CODE", "TRADE_DATE"],
    "TECH11_AI_GOV_ETH_INDEX": ["PORT_DATE"],
    "INDEX_COMPANIES": [],
    "INDEX_CONSTITUENTS": ["INDEX_DATE", "INDEX_NAME"],
    "ESG_COMPANIES": [],
    "ESG_CONSTITUENT_PRICES": ["INDEX_CODE", "TRADE_DATE"],
    "AIGES_SCORES": ["ASOF"],
    "ATTRIBUTION": ["DT"],
    "NEWS_ITEM_TICKERS": ["ITEM_TABLE", "ITEM_ID"],
    "NEWS_ITEMS": ["ID"],
    "ESG_BENCHMARKS": ["BENCH_ID"],
}


@dataclass
class TableSpec:
    owner: str
    table: str
    ticker_column: str
    key_columns: list[str]


def _safe_name(name: str) -> str:
    cleaned = "".join(ch for ch in name if ch.isalnum() or ch == "_").upper()
    if cleaned != name.upper():
        raise ValueError(f"unsafe_name:{name}")
    return cleaned


def _select_owner(cur) -> str:
    cur.execute("SELECT USER FROM dual")
    row = cur.fetchone()
    return str(row[0]).strip()


def _fetch_tables_with_ticker_columns(cur, owner: str) -> dict[str, list[str]]:
    placeholders = ",".join([f":c{i}" for i in range(len(TICKER_COLUMNS))])
    sql = (
        "SELECT c.table_name, c.column_name "
        "FROM all_tab_columns c "
        "JOIN all_objects o ON o.owner = c.owner AND o.object_name = c.table_name "
        "WHERE c.owner = :owner "
        "AND o.object_type = 'TABLE' "
        f"AND c.column_name IN ({placeholders}) "
        "ORDER BY c.table_name, c.column_name"
    )
    binds = {"owner": owner}
    for idx, col in enumerate(TICKER_COLUMNS):
        binds[f"c{idx}"] = col
    cur.execute(sql, binds)
    tables: dict[str, list[str]] = {}
    for table_name, column_name in cur.fetchall():
        tables.setdefault(str(table_name), []).append(str(column_name))
    return tables


def _fetch_constraints(cur, owner: str, table_name: str) -> dict[str, list[str]]:
    sql = (
        "SELECT c.constraint_name, c.constraint_type, cc.column_name, cc.position "
        "FROM all_constraints c "
        "JOIN all_cons_columns cc "
        "  ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name "
        "WHERE c.owner = :owner "
        "AND c.table_name = :table_name "
        "AND c.constraint_type IN ('P','U') "
        "ORDER BY c.constraint_name, cc.position"
    )
    cur.execute(sql, {"owner": owner, "table_name": table_name})
    constraints: dict[str, list[str]] = {}
    for cname, ctype, col, pos in cur.fetchall():
        key = f"{ctype}:{cname}"
        constraints.setdefault(key, []).append(str(col))
    return constraints


def _select_key_columns(table: str, constraints: dict[str, list[str]], ticker_col: str) -> list[str]:
    if table in KEY_OVERRIDES:
        return KEY_OVERRIDES[table]
    ticker_cols = {ticker_col}
    for columns in constraints.values():
        if any(col in ticker_cols for col in columns):
            return [col for col in columns if col not in ticker_cols]
    for columns in constraints.values():
        return [col for col in columns if col not in ticker_cols]
    return []


def _fetch_table_columns(cur, owner: str, table: str) -> list[str]:
    cur.execute(
        "SELECT column_name FROM all_tab_columns WHERE owner = :owner AND table_name = :table_name ORDER BY column_id",
        {"owner": owner, "table_name": table},
    )
    return [str(row[0]) for row in cur.fetchall()]


def _count_ticker(cur, owner: str, table: str, ticker_col: str, ticker: str) -> int:
    sql = f"SELECT COUNT(*) FROM {owner}.{table} WHERE {ticker_col} = :ticker"
    cur.execute(sql, {"ticker": ticker})
    row = cur.fetchone()
    return int(row[0] or 0)


def _count_collisions(cur, owner: str, table: str, ticker_col: str, key_cols: list[str]) -> int:
    if not key_cols:
        sql = (
            f"SELECT COUNT(*) FROM ("
            f"SELECT 1 FROM {owner}.{table} "
            f"WHERE {ticker_col} IN (:old_ticker, :new_ticker) "
            f"GROUP BY {ticker_col} "
            f"HAVING COUNT(DISTINCT {ticker_col}) > 1)"
        )
        cur.execute(sql, {"old_ticker": OLD_TICKER, "new_ticker": NEW_TICKER})
        return int(cur.fetchone()[0] or 0)
    key_expr = ", ".join(key_cols)
    sql = (
        f"SELECT COUNT(*) FROM ("
        f"SELECT {key_expr} FROM {owner}.{table} "
        f"WHERE {ticker_col} IN (:old_ticker, :new_ticker) "
        f"GROUP BY {key_expr} "
        f"HAVING COUNT(DISTINCT {ticker_col}) > 1)"
    )
    cur.execute(sql, {"old_ticker": OLD_TICKER, "new_ticker": NEW_TICKER})
    return int(cur.fetchone()[0] or 0)


def _fetch_collision_keys(cur, owner: str, table: str, ticker_col: str, key_cols: list[str]) -> list[tuple]:
    if not key_cols:
        sql = (
            f"SELECT 1 FROM {owner}.{table} "
            f"WHERE {ticker_col} IN (:old_ticker, :new_ticker) "
            f"GROUP BY {ticker_col} "
            f"HAVING COUNT(DISTINCT {ticker_col}) > 1"
        )
        cur.execute(sql, {"old_ticker": OLD_TICKER, "new_ticker": NEW_TICKER})
        return [tuple()] if cur.fetchone() else []
    key_expr = ", ".join(key_cols)
    sql = (
        f"SELECT {key_expr} FROM {owner}.{table} "
        f"WHERE {ticker_col} IN (:old_ticker, :new_ticker) "
        f"GROUP BY {key_expr} "
        f"HAVING COUNT(DISTINCT {ticker_col}) > 1"
    )
    cur.execute(sql, {"old_ticker": OLD_TICKER, "new_ticker": NEW_TICKER})
    return [tuple(row) for row in cur.fetchall()]


def _build_key_filter(key_cols: list[str], key: tuple) -> tuple[str, dict]:
    if not key_cols:
        return "", {}
    clauses = []
    binds: dict[str, Any] = {}
    for idx, col in enumerate(key_cols):
        param = f"k{idx}"
        clauses.append(f"{col} = :{param}")
        binds[param] = key[idx]
    return " AND ".join(clauses), binds


def _fetch_rows_for_key(
    cur,
    owner: str,
    table: str,
    ticker_col: str,
    key_cols: list[str],
    key: tuple,
    columns: list[str],
) -> list[dict]:
    column_expr = ", ".join(columns)
    where_key, binds = _build_key_filter(key_cols, key)
    where_clauses = [f"{ticker_col} IN (:old_ticker, :new_ticker)"]
    binds.update({"old_ticker": OLD_TICKER, "new_ticker": NEW_TICKER})
    if where_key:
        where_clauses.append(where_key)
    sql = (
        f"SELECT ROWID AS RID, {column_expr} "
        f"FROM {owner}.{table} "
        f"WHERE {' AND '.join(where_clauses)}"
    )
    cur.execute(sql, binds)
    rows = []
    col_names = ["RID"] + columns
    for raw in cur.fetchall():
        rows.append({col_names[idx]: raw[idx] for idx in range(len(col_names))})
    return rows


def _as_number(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _as_ts(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, dt.datetime):
        return value.timestamp()
    return 0.0


def _score_row(table: str, row: dict) -> tuple:
    if table == "SC_IDX_PRICES_RAW":
        status_ok = 1 if (row.get("STATUS") or "").upper() == "OK" else 0
        has_price = 1 if row.get("ADJ_CLOSE_PX") or row.get("CLOSE_PX") else 0
        return (status_ok, has_price, _as_ts(row.get("INGESTED_AT")))
    if table == "SC_IDX_PRICES_CANON":
        has_adj = 1 if row.get("CANON_ADJ_CLOSE_PX") is not None else 0
        has_close = 1 if row.get("CANON_CLOSE_PX") is not None else 0
        providers_ok = _as_number(row.get("PROVIDERS_OK"))
        return (has_adj, has_close, providers_ok, _as_ts(row.get("COMPUTED_AT")))
    if table == "SC_IDX_CONSTITUENT_DAILY":
        has_price = 1 if row.get("PRICE_USED") is not None else 0
        has_mv = 1 if row.get("MARKET_VALUE") is not None else 0
        has_weight = 1 if row.get("WEIGHT") is not None else 0
        return (has_price, has_mv, has_weight, _as_ts(row.get("COMPUTED_AT")))
    if table == "SC_IDX_CONTRIBUTION_DAILY":
        has_contrib = 1 if row.get("CONTRIBUTION") is not None else 0
        has_ret = 1 if row.get("RET_1D") is not None else 0
        has_weight = 1 if row.get("WEIGHT_PREV") is not None else 0
        return (has_contrib, has_ret, has_weight, _as_ts(row.get("COMPUTED_AT")))
    if table == "SC_IDX_HOLDINGS":
        has_shares = 1 if row.get("SHARES") is not None else 0
        has_weight = 1 if row.get("TARGET_WEIGHT") is not None else 0
        return (has_shares, has_weight)
    if table == "SC_IDX_IMPUTATIONS":
        has_price = 1 if row.get("IMPUTED_PRICE") is not None else 0
        return (has_price, _as_ts(row.get("CREATED_AT")))
    if table == "TECH11_AI_GOV_ETH_INDEX":
        has_weight = 1 if row.get("PORT_WEIGHT") is not None else 0
        return (has_weight, _as_number(row.get("PORT_WEIGHT")), -_as_number(row.get("RANK_INDEX")))
    if table == "INDEX_COMPANIES":
        has_name = 1 if row.get("NAME") else 0
        has_sector = 1 if row.get("SECTOR") else 0
        has_industry = 1 if row.get("INDUSTRY") else 0
        return (has_name, has_sector, has_industry)
    if table == "INDEX_CONSTITUENTS":
        has_weight = 1 if row.get("WEIGHT_PERCENT") is not None else 0
        has_shares = 1 if row.get("SHARES_HELD") is not None else 0
        return (has_weight, has_shares, _as_number(row.get("WEIGHT_PERCENT")))
    if table == "ESG_COMPANIES":
        has_name = 1 if row.get("NAME") else 0
        has_sector = 1 if row.get("SECTOR") else 0
        has_industry = 1 if row.get("INDUSTRY") else 0
        return (has_name, has_sector, has_industry)
    if table == "ESG_CONSTITUENT_PRICES":
        has_price = 1 if row.get("CLOSE_ADJ") is not None else 0
        has_weight = 1 if row.get("WEIGHT") is not None else 0
        return (has_price, has_weight, _as_number(row.get("CLOSE_ADJ")))
    if table == "AIGES_SCORES":
        has_asof = 1 if row.get("ASOF") is not None else 0
        return (has_asof,)
    if table == "ATTRIBUTION":
        has_dt = 1 if row.get("DT") is not None else 0
        return (has_dt,)
    if table == "NEWS_ITEM_TICKERS":
        has_id = 1 if row.get("ITEM_ID") is not None else 0
        return (has_id,)
    if table == "NEWS_ITEMS":
        has_id = 1 if row.get("ID") is not None else 0
        return (has_id,)
    if table == "ESG_BENCHMARKS":
        has_id = 1 if row.get("BENCH_ID") is not None else 0
        return (has_id,)
    return tuple()


def _choose_best_row(table: str, rows: list[dict]) -> dict:
    best = None
    for row in rows:
        score = _score_row(table, row)
        if best is None:
            best = (score, row)
            continue
        if score > best[0]:
            best = (score, row)
        elif score == best[0]:
            if (row.get("TICKER") or row.get("SYMBOL")) == NEW_TICKER:
                best = (score, row)
    if best is None:
        raise ValueError("no_rows_to_choose")
    return best[1]


def _create_backup_table(cur, owner: str, table: str, ticker_col: str, suffix: str) -> str:
    backup = f"SC_BAK_FI_FISV_{table}_{suffix}".upper()
    sql = (
        f"CREATE TABLE {owner}.{backup} AS "
        f"SELECT * FROM {owner}.{table} "
        f"WHERE {ticker_col} IN ('{OLD_TICKER}', '{NEW_TICKER}')"
    )
    cur.execute(sql)
    return backup


def _delete_by_rowids(cur, owner: str, table: str, rowids: list[str]) -> int:
    if not rowids:
        return 0
    placeholders = ",".join(f":r{i}" for i in range(len(rowids)))
    binds = {f"r{i}": rid for i, rid in enumerate(rowids)}
    sql = f"DELETE FROM {owner}.{table} WHERE ROWID IN ({placeholders})"
    cur.execute(sql, binds)
    return cur.rowcount or 0


def _update_rowid_ticker(cur, owner: str, table: str, rowid: str, ticker_col: str) -> int:
    sql = f"UPDATE {owner}.{table} SET {ticker_col} = :new_ticker WHERE ROWID = :rid"
    cur.execute(sql, {"new_ticker": NEW_TICKER, "rid": rowid})
    return cur.rowcount or 0


def _update_remaining_fi(cur, owner: str, table: str, ticker_col: str) -> int:
    sql = f"UPDATE {owner}.{table} SET {ticker_col} = :new_ticker WHERE {ticker_col} = :old_ticker"
    cur.execute(sql, {"new_ticker": NEW_TICKER, "old_ticker": OLD_TICKER})
    return cur.rowcount or 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate ticker FI to FISV safely.")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    return parser.parse_args()


def _table_specs(cur, owner: str) -> list[TableSpec]:
    tables = _fetch_tables_with_ticker_columns(cur, owner)
    specs: list[TableSpec] = []
    for table, cols in sorted(tables.items()):
        if table.startswith("SC_BAK_FI_FISV_"):
            continue
        constraints = _fetch_constraints(cur, owner, table)
        for ticker_col in cols:
            key_cols = _select_key_columns(table, constraints, ticker_col)
            specs.append(TableSpec(owner=owner, table=table, ticker_column=ticker_col, key_columns=key_cols))
    return specs


def main() -> int:
    args = _parse_args()
    load_default_env()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_lines = [
        "# FI -> FISV migration report",
        "",
        f"mode={'apply' if args.apply else 'dry-run'}",
        f"timestamp_utc={dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "",
    ]

    collisions_rows: list[list[str]] = [
        ["table", "ticker_column", "key", "chosen_ticker", "fi_rowids", "fisv_rowids"]
    ]

    with get_connection() as conn:
        cur = conn.cursor()
        owner = _select_owner(cur)
        report_lines.append(f"oracle_user={owner}")
        report_lines.append("")
        specs = _table_specs(cur, owner)

    for spec in specs:
        table = _safe_name(spec.table)
        ticker_col = _safe_name(spec.ticker_column)
        key_cols = [ _safe_name(col) for col in spec.key_columns ]

        with get_connection() as conn:
            cur = conn.cursor()
            fi_count = _count_ticker(cur, owner, table, ticker_col, OLD_TICKER)
            fisv_count = _count_ticker(cur, owner, table, ticker_col, NEW_TICKER)
            collisions = _count_collisions(cur, owner, table, ticker_col, key_cols)
            if fi_count == 0 and collisions == 0:
                continue
            report_lines.append(f"## {table}.{ticker_col}")
            report_lines.append(f"- FI={fi_count} FISV={fisv_count} collisions={collisions}")

            columns = _fetch_table_columns(cur, owner, table)
            backup_table = None
            updated = 0
            deleted = 0

            if args.apply:
                backup_table = _create_backup_table(cur, owner, table, ticker_col, timestamp)
                report_lines.append(f"- backup_table={backup_table}")

            collision_keys = _fetch_collision_keys(cur, owner, table, ticker_col, key_cols)
            for key in collision_keys:
                rows = _fetch_rows_for_key(cur, owner, table, ticker_col, key_cols, key, columns)
                if not rows:
                    continue
                best = _choose_best_row(table, rows)
                best_rowid = best.get("RID")
                best_ticker = best.get(ticker_col) or best.get("TICKER") or best.get("SYMBOL")

                fi_rowids = [row.get("RID") for row in rows if (row.get(ticker_col) == OLD_TICKER)]
                fisv_rowids = [row.get("RID") for row in rows if (row.get(ticker_col) == NEW_TICKER)]

                collisions_rows.append(
                    [
                        table,
                        ticker_col,
                        "|".join(str(v) for v in key) if key else "<none>",
                        str(best_ticker),
                        ";".join(str(rid) for rid in fi_rowids if rid),
                        ";".join(str(rid) for rid in fisv_rowids if rid),
                    ]
                )

                if args.apply:
                    delete_rowids = [row.get("RID") for row in rows if row.get("RID") != best_rowid]
                    deleted += _delete_by_rowids(cur, owner, table, delete_rowids)
                    if best_ticker == OLD_TICKER:
                        updated += _update_rowid_ticker(cur, owner, table, best_rowid, ticker_col)

            if args.apply:
                updated += _update_remaining_fi(cur, owner, table, ticker_col)
                conn.commit()

            report_lines.append(f"- updated_rows={updated} deleted_rows={deleted}")
            report_lines.append("")

    REPORT_PATH.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    with COLLISIONS_PATH.open("w", encoding="utf-8", newline="") as handle:
        for row in collisions_rows:
            handle.write(",".join(str(value) for value in row) + "\n")

    print(f"report_written={REPORT_PATH}")
    print(f"collisions_written={COLLISIONS_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.oracle.env_bootstrap import load_env_files
from tools.oracle import preflight_oracle
from db_helper import get_connection

OUTPUT_DEFAULT = REPO_ROOT / "tools" / "audit" / "output" / "stuck_2026_01_debug.txt"


def _write_block(handle, title: str, content: str) -> None:
    handle.write("\n")
    handle.write(f"## {title}\n")
    handle.write(content)
    if not content.endswith("\n"):
        handle.write("\n")


def _run_query(sql: str, params: dict | None = None) -> list[tuple]:
    params = params or {}
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(sql, params)
                return cur.fetchall()
        except Exception as exc:
            last_exc = exc
            time.sleep(1 + attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError("oracle_query_failed")


def _format_rows(rows: list[tuple]) -> str:
    if not rows:
        return "(no rows)\n"
    lines = []
    for row in rows:
        lines.append("\t".join(str(item) for item in row))
    return "\n".join(lines) + "\n"


def _discover_index_code() -> str | None:
    rows = _run_query("SELECT DISTINCT index_code FROM SC_IDX_LEVELS")
    codes = [row[0] for row in rows if row and row[0] is not None]
    if not codes:
        return None
    return str(codes[0])


def _next_trading_day(after_date: dt.date) -> dt.date | None:
    rows = _run_query(
        "SELECT trade_date FROM SC_IDX_TRADING_DAYS WHERE trade_date > :d ORDER BY trade_date FETCH FIRST 1 ROWS ONLY",
        {"d": after_date},
    )
    if not rows:
        return None
    value = rows[0][0]
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return None


def _missing_tickers_for_date(trade_date: dt.date) -> list[str]:
    sql = (
        "WITH universe AS ("
        "  SELECT ticker "
        "  FROM ("
        "    SELECT ticker, port_weight, rank_index "
        "    FROM tech11_ai_gov_eth_index "
        "    WHERE port_date = (SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= :trade_date) "
        "    AND port_weight > 0 "
        "    AND ticker IS NOT NULL "
        "    ORDER BY port_weight DESC, rank_index"
        "  ) WHERE ROWNUM <= 25"
        ") "
        "SELECT u.ticker "
        "FROM universe u "
        "LEFT JOIN SC_IDX_PRICES_CANON c "
        "  ON c.trade_date = :trade_date AND c.ticker = u.ticker "
        "WHERE c.canon_adj_close_px IS NULL AND c.canon_close_px IS NULL"
    )
    rows = _run_query(sql, {"trade_date": trade_date})
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _missing_tickers_for_prev_day(universe_date: dt.date, prev_date: dt.date) -> list[str]:
    sql = (
        "WITH universe AS ("
        "  SELECT ticker "
        "  FROM ("
        "    SELECT ticker, port_weight, rank_index "
        "    FROM tech11_ai_gov_eth_index "
        "    WHERE port_date = (SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= :universe_date) "
        "    AND port_weight > 0 "
        "    AND ticker IS NOT NULL "
        "    ORDER BY port_weight DESC, rank_index"
        "  ) WHERE ROWNUM <= 25"
        ") "
        "SELECT u.ticker "
        "FROM universe u "
        "LEFT JOIN SC_IDX_PRICES_CANON c "
        "  ON c.trade_date = :prev_date AND c.ticker = u.ticker "
        "WHERE c.canon_adj_close_px IS NULL AND c.canon_close_px IS NULL"
    )
    rows = _run_query(sql, {"universe_date": universe_date, "prev_date": prev_date})
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _missing_adj_close_tickers(trade_date: dt.date) -> list[str]:
    sql = (
        "WITH universe AS ("
        "  SELECT ticker "
        "  FROM ("
        "    SELECT ticker, port_weight, rank_index "
        "    FROM tech11_ai_gov_eth_index "
        "    WHERE port_date = (SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= :trade_date) "
        "    AND port_weight > 0 "
        "    AND ticker IS NOT NULL "
        "    ORDER BY port_weight DESC, rank_index"
        "  ) WHERE ROWNUM <= 25"
        ") "
        "SELECT u.ticker "
        "FROM universe u "
        "LEFT JOIN SC_IDX_PRICES_CANON c "
        "  ON c.trade_date = :trade_date AND c.ticker = u.ticker "
        "WHERE c.canon_adj_close_px IS NULL"
    )
    rows = _run_query(sql, {"trade_date": trade_date})
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _missing_adj_close_for_prev_day(universe_date: dt.date, prev_date: dt.date) -> list[str]:
    sql = (
        "WITH universe AS ("
        "  SELECT ticker "
        "  FROM ("
        "    SELECT ticker, port_weight, rank_index "
        "    FROM tech11_ai_gov_eth_index "
        "    WHERE port_date = (SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= :universe_date) "
        "    AND port_weight > 0 "
        "    AND ticker IS NOT NULL "
        "    ORDER BY port_weight DESC, rank_index"
        "  ) WHERE ROWNUM <= 25"
        ") "
        "SELECT u.ticker "
        "FROM universe u "
        "LEFT JOIN SC_IDX_PRICES_CANON c "
        "  ON c.trade_date = :prev_date AND c.ticker = u.ticker "
        "WHERE c.canon_adj_close_px IS NULL"
    )
    rows = _run_query(sql, {"universe_date": universe_date, "prev_date": prev_date})
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _missing_adj_close_counts(dates: list[dt.date]) -> list[tuple[dt.date, int]]:
    results = []
    for trade_date in dates:
        missing = _missing_adj_close_tickers(trade_date)
        results.append((trade_date, len(missing)))
    return results


def main() -> int:
    load_env_files()
    preflight = preflight_oracle.main()
    if preflight != 0:
        return preflight

    output_path = OUTPUT_DEFAULT
    if len(sys.argv) > 1:
        output_path = Path(sys.argv[1])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write("\n# stuck_2026_01_debug\n")
        handle.write(f"timestamp_utc={dt.datetime.utcnow().isoformat()}\n")

        index_code = _discover_index_code()
        _write_block(handle, "index_code", f"{index_code}\n")

        # 1) Trading calendar
        sql_calendar = (
            "SELECT trade_date FROM SC_IDX_TRADING_DAYS "
            "WHERE trade_date BETWEEN DATE '2025-12-29' AND DATE '2026-01-10' "
            "ORDER BY trade_date"
        )
        rows = _run_query(sql_calendar)
        _write_block(handle, "trading_calendar", _format_rows(rows))

        # 2) Prices
        sql_canon = (
            "SELECT trade_date, COUNT(*) cnt FROM SC_IDX_PRICES_CANON "
            "WHERE trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10' "
            "GROUP BY trade_date ORDER BY trade_date"
        )
        rows = _run_query(sql_canon)
        _write_block(handle, "canon_counts", _format_rows(rows))

        sql_raw = (
            "SELECT trade_date, COUNT(*) cnt FROM SC_IDX_PRICES_RAW "
            "WHERE trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10' "
            "GROUP BY trade_date ORDER BY trade_date"
        )
        rows = _run_query(sql_raw)
        _write_block(handle, "raw_counts", _format_rows(rows))

        # Missing tickers for first date after 2026-01-02
        next_day = _next_trading_day(dt.date(2026, 1, 2))
        if next_day:
            missing = _missing_tickers_for_date(next_day)
            content = f"next_trading_day={next_day.isoformat()}\nmissing_count={len(missing)}\n"
            if missing:
                content += "missing_tickers=" + ",".join(missing) + "\n"
            _write_block(handle, "missing_tickers_next_day", content)
            prev_day = dt.date(2026, 1, 2)
            missing_prev = _missing_tickers_for_prev_day(next_day, prev_day)
            content_prev = (
                f"universe_date={next_day.isoformat()}\n"
                f"prev_date={prev_day.isoformat()}\n"
                f"missing_count={len(missing_prev)}\n"
            )
            if missing_prev:
                content_prev += "missing_tickers=" + ",".join(missing_prev) + "\n"
            _write_block(handle, "missing_tickers_prev_day", content_prev)
            missing_adj_prev = _missing_adj_close_for_prev_day(next_day, prev_day)
            content_adj_prev = (
                f"universe_date={next_day.isoformat()}\n"
                f"prev_date={prev_day.isoformat()}\n"
                f"missing_adj_close_count={len(missing_adj_prev)}\n"
            )
            if missing_adj_prev:
                content_adj_prev += "missing_adj_close_tickers=" + ",".join(missing_adj_prev) + "\n"
            _write_block(handle, "missing_adj_close_prev_day", content_adj_prev)
            missing_adj = _missing_adj_close_tickers(next_day)
            content_adj = (
                f"trade_date={next_day.isoformat()}\n"
                f"missing_adj_close_count={len(missing_adj)}\n"
            )
            if missing_adj:
                content_adj += "missing_adj_close_tickers=" + ",".join(missing_adj) + "\n"
            _write_block(handle, "missing_adj_close_next_day", content_adj)

            days_window = [d for d in _run_query(
                "SELECT trade_date FROM SC_IDX_TRADING_DAYS "
                "WHERE trade_date BETWEEN DATE '2026-01-02' AND DATE '2026-01-10' "
                "ORDER BY trade_date"
            )]
            dates = []
            for (value,) in days_window:
                if isinstance(value, dt.datetime):
                    value = value.date()
                if isinstance(value, dt.date):
                    dates.append(value)
            missing_counts = _missing_adj_close_counts(dates)
            lines = ["trade_date\tmissing_adj_close_count"]
            for trade_date, count in missing_counts:
                lines.append(f"{trade_date.isoformat()}\t{count}")
            _write_block(handle, "missing_adj_close_by_day", "\n".join(lines) + "\n")
        else:
            _write_block(handle, "missing_tickers_next_day", "next_trading_day=none\n")

        # 3) Index outputs
        sql_levels = (
            "SELECT trade_date, level_tr FROM SC_IDX_LEVELS "
            "WHERE index_code=:index_code AND trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10' "
            "ORDER BY trade_date"
        )
        try:
            rows = _run_query(sql_levels, {"index_code": index_code})
            _write_block(handle, "levels", _format_rows(rows))
        except Exception as exc:
            _write_block(handle, "levels_error", f"{exc}\n")

        sql_stats_requested = (
            "SELECT trade_date, level_tr, ret_1d, ret_5d, ret_20d, vol_20d, n_constituents, n_imputed "
            "FROM SC_IDX_STATS_DAILY "
            "WHERE index_code=:index_code AND trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10' "
            "ORDER BY trade_date"
        )
        try:
            rows = _run_query(sql_stats_requested, {"index_code": index_code})
            _write_block(handle, "stats_requested", _format_rows(rows))
        except Exception as exc:
            _write_block(handle, "stats_requested_error", f"{exc}\n")
            sql_stats = (
                "SELECT trade_date, level_tr, ret_1d, ret_5d, ret_20d, vol_20d, n_constituents, n_imputed "
                "FROM SC_IDX_STATS_DAILY "
                "WHERE trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10' "
                "ORDER BY trade_date"
            )
            rows = _run_query(sql_stats)
            _write_block(handle, "stats_actual", _format_rows(rows))

        sql_contrib = (
            "SELECT trade_date, COUNT(*) cnt FROM SC_IDX_CONTRIBUTION_DAILY "
            "WHERE trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10' "
            "GROUP BY trade_date ORDER BY trade_date"
        )
        rows = _run_query(sql_contrib)
        _write_block(handle, "contribution_counts", _format_rows(rows))

        # 4) Job runs / errors
        sql_runs_requested = (
            "SELECT started_at, ended_at, job_name, status, provider, error, run_id "
            "FROM SC_IDX_JOB_RUNS "
            "WHERE started_at >= TIMESTAMP '2026-01-01 00:00:00' "
            "ORDER BY started_at DESC"
        )
        try:
            rows = _run_query(sql_runs_requested)
            _write_block(handle, "job_runs_requested", _format_rows(rows))
        except Exception as exc:
            _write_block(handle, "job_runs_requested_error", f"{exc}\n")
            sql_runs = (
                "SELECT started_at, ended_at, job_name, status, provider, error_msg, run_id "
                "FROM SC_IDX_JOB_RUNS "
                "WHERE started_at >= TIMESTAMP '2026-01-01 00:00:00' "
                "ORDER BY started_at DESC"
            )
            rows = _run_query(sql_runs)
            _write_block(handle, "job_runs_actual", _format_rows(rows))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

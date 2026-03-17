from __future__ import annotations

import argparse
import datetime as _dt
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.index_engine.env_loader import load_default_env
from tools.oracle.env_bootstrap import load_env_files
from tools.oracle import preflight_oracle

from index_engine import db_portfolio_analytics as db
from index_engine.portfolio_analytics_v1 import build_portfolio_outputs


def _parse_date(value: str | None) -> _dt.date | None:
    if not value:
        return None
    return _dt.date.fromisoformat(value)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TECH100 portfolio analytics tables")
    parser.add_argument("--start", help="Optional write window start YYYY-MM-DD")
    parser.add_argument("--end", help="Optional write window end YYYY-MM-DD")
    parser.add_argument("--apply-ddl", action="store_true", help="Apply the portfolio analytics DDL first")
    parser.add_argument("--dry-run", action="store_true", help="Compute and summarize without writing")
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip Oracle preflight (only for tests or when preflight already ran)",
    )
    return parser.parse_args(argv)


def _filter_rows(
    rows: list[dict[str, object]],
    *,
    start_date: _dt.date,
    end_date: _dt.date,
) -> list[dict[str, object]]:
    filtered: list[dict[str, object]] = []
    for row in rows:
        trade_date = row.get("trade_date")
        if not isinstance(trade_date, _dt.date):
            continue
        if start_date <= trade_date <= end_date:
            filtered.append(row)
    return filtered


def _summarize_outputs(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    analytics_rows: list[dict[str, object]],
    position_rows: list[dict[str, object]],
    optimizer_rows: list[dict[str, object]],
) -> str:
    models = sorted(
        {
            str(row.get("model_code"))
            for row in analytics_rows
            if row.get("model_code")
        }
    )
    latest_date = None
    if analytics_rows:
        latest_date = max(
            row["trade_date"]
            for row in analytics_rows
            if isinstance(row.get("trade_date"), _dt.date)
        )
    lines = [
        f"write_window={start_date.isoformat()}..{end_date.isoformat()}",
        f"analytics_rows={len(analytics_rows)}",
        f"position_rows={len(position_rows)}",
        f"optimizer_rows={len(optimizer_rows)}",
        f"models={','.join(models)}",
        f"latest_trade_date={latest_date.isoformat() if latest_date else 'none'}",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_env_files()
    load_default_env()

    if not args.skip_preflight and preflight_oracle.main() != 0:
        return 2

    if args.apply_ddl:
        db.apply_ddl()

    bounds = db.fetch_trade_date_bounds()
    min_date, max_date = bounds
    if min_date is None or max_date is None:
        print("portfolio_analytics: no_official_trade_dates", flush=True)
        return 1

    start_date = _parse_date(args.start) or min_date
    end_date = _parse_date(args.end) or max_date
    if end_date < start_date:
        raise ValueError("end_before_start")

    official_daily_rows = db.fetch_official_daily_rows()
    official_position_rows = db.fetch_official_position_rows()
    metadata_rows = db.fetch_metadata_rows()
    price_rows = db.fetch_price_rows(start_date=min_date, end_date=max_date)

    outputs = build_portfolio_outputs(
        official_daily_rows=official_daily_rows,
        official_position_rows=official_position_rows,
        metadata_rows=metadata_rows,
        price_rows=price_rows,
    )

    analytics_rows = _filter_rows(outputs["analytics"], start_date=start_date, end_date=end_date)
    position_rows = _filter_rows(outputs["positions"], start_date=start_date, end_date=end_date)
    optimizer_rows = _filter_rows(outputs["optimizer_inputs"], start_date=start_date, end_date=end_date)

    summary = _summarize_outputs(
        start_date=start_date,
        end_date=end_date,
        analytics_rows=analytics_rows,
        position_rows=position_rows,
        optimizer_rows=optimizer_rows,
    )
    print(summary, flush=True)

    if args.dry_run:
        return 0

    db.persist_outputs(
        start_date=start_date,
        end_date=end_date,
        analytics_rows=analytics_rows,
        position_rows=position_rows,
        optimizer_rows=optimizer_rows,
        constraint_rows=outputs["constraints"],
    )
    print("portfolio_analytics: refresh_complete", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

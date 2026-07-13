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
        "--low-resource",
        action="store_true",
        help="Persist one model at a time using manifest-backed bounded batches",
    )
    parser.add_argument("--backup-tag", help="Validated reconstruction backup tag")
    parser.add_argument("--run-id", help="Validated reconstruction run identifier")
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
    position_row_count: int | None = None,
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
        f"position_rows={position_row_count if position_row_count is not None else len(position_rows)}",
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

    if args.low_resource and not args.dry_run and not (args.backup_tag and args.run_id):
        raise ValueError("low_resource_writes_require_manifest_coordinates")

    streamed_analytics: list[dict[str, object]] = []
    streamed_position_count = 0

    def persist_model(_spec, analytics, positions) -> None:
        nonlocal streamed_position_count
        analytics = _filter_rows(analytics, start_date=start_date, end_date=end_date)
        positions = _filter_rows(positions, start_date=start_date, end_date=end_date)
        streamed_analytics.extend(analytics)
        streamed_position_count += len(positions)
        if not args.dry_run:
            analytics_count, position_count = db.persist_model_output_batch(
                analytics_rows=analytics,
                position_rows=positions,
            )
            print(
                f"portfolio_model_persist:model={_spec.code} analytics_rows={analytics_count} "
                f"position_rows={position_count}",
                flush=True,
            )

    if args.low_resource and not args.dry_run:
        from db_helper import get_connection
        from tools.db_migrations import repair_sc_idx_corporate_actions as repair

        with get_connection() as conn:
            repair.validate_backup_set(
                conn,
                args.backup_tag,
                start_date,
                end_date,
                expected_run_id=args.run_id,
            )
        db.reset_output_window(start_date=start_date, end_date=end_date)

    outputs = build_portfolio_outputs(
        official_daily_rows=official_daily_rows,
        official_position_rows=official_position_rows,
        metadata_rows=metadata_rows,
        price_rows=price_rows,
        model_output_callback=persist_model if args.low_resource else None,
    )

    analytics_rows = (
        streamed_analytics
        if args.low_resource
        else _filter_rows(outputs["analytics"], start_date=start_date, end_date=end_date)
    )
    position_rows = (
        []
        if args.low_resource
        else _filter_rows(outputs["positions"], start_date=start_date, end_date=end_date)
    )
    optimizer_rows = _filter_rows(outputs["optimizer_inputs"], start_date=start_date, end_date=end_date)

    summary = _summarize_outputs(
        start_date=start_date,
        end_date=end_date,
        analytics_rows=analytics_rows,
        position_rows=position_rows,
        optimizer_rows=optimizer_rows,
        position_row_count=streamed_position_count if args.low_resource else None,
    )
    print(summary, flush=True)

    if args.dry_run:
        return 0

    if args.low_resource:
        optimizer_count = db.persist_optimizer_and_constraints(
            optimizer_rows=optimizer_rows,
            constraint_rows=outputs["constraints"],
        )
        print(f"portfolio_optimizer_persist:rows={optimizer_count}", flush=True)
    else:
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

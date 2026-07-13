#!/usr/bin/env python3
"""Print the current secrets-free reconstruction status once."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "app")]

from index_engine.reconstruction_status import read_reconstruction_status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read SC_IDX reconstruction status once")
    parser.add_argument(
        "--status-file",
        type=Path,
        default=Path(
            os.getenv(
                "SC_IDX_RECONSTRUCTION_STATUS_FILE",
                "/var/lib/sustainacore/sc_idx/reconstruction_status.json",
            )
        ),
    )
    args = parser.parse_args(argv)
    if not args.status_file.exists():
        print("status=UNAVAILABLE")
        return 2
    payload = read_reconstruction_status(args.status_file)
    for key in (
        "run_id",
        "backup_tag",
        "revision",
        "repair_start",
        "repair_end",
        "stage",
        "stage_started_at",
        "last_completed_date",
        "rows_processed",
        "model_code",
        "completed_model_count",
        "analytics_rows_committed",
        "position_rows_committed",
        "optimizer_rows_committed",
        "status",
        "failure_class",
        "rollback_status",
        "updated_at",
    ):
        value = payload.get(key)
        print(f"{key}={'' if value is None else value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

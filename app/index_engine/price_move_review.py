"""Read immutable, operator-reviewed observations on exceptional returns only."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
from pathlib import Path

PACKAGED = Path(__file__).with_name("price_move_reviews")
PRIVATE = Path("/etc/sustainacore/price_move_reviews")


def matching_review(ticker: str, previous: dt.date, current: dt.date,
                    p0: float, p1: float) -> str | None:
    if not re.fullmatch(r"[A-Z0-9.-]{1,20}", ticker):
        return None
    name = f"{current.isoformat()}_{ticker}.json"
    for directory in (PACKAGED, PRIVATE):
        try:
            with (directory / name).open("rb") as stream:
                raw = stream.read(16385)
        except FileNotFoundError:
            continue
        if len(raw) > 16384:
            return None
        try:
            record = json.loads(raw)
            valid = (
                record["ticker"] == ticker and record["previous_trade_date"] == previous.isoformat()
                and record["trade_date"] == current.isoformat()
                and record["basis"] == "adjusted_regular_session_close"
                and record["corporate_actions"] == "checked_clear"
                and record["independent_source_confirmed"] is True
                and all(isinstance(record[key], str) and record[key].strip()
                        for key in ("security", "exchange", "currency", "reviewer",
                                    "reviewed_at", "price_evidence", "action_evidence"))
                and all(type(p) in (float, int) for p in record["canonical_prices"])
                and record["canonical_prices"] == [p0, p1]
                and len(record["independent_adjusted_prices"]) == 2
                and all(type(b) in (float, int) and math.isclose(a, b, abs_tol=0.000005, rel_tol=0)
                        for a, b in zip((p0, p1), record["independent_adjusted_prices"]))
            )
        except (ValueError, TypeError, KeyError, OverflowError):
            return None
        if valid:
            return hashlib.sha256(raw).hexdigest()
        return None
    return None

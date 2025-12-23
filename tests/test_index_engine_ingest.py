import datetime as _dt
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from tools.index_engine.ingest_prices import compute_canonical_rows, _build_raw_rows_from_provider


def test_compute_canonical_uses_provider_close_when_adj_missing():
    trade_date = _dt.date(2025, 1, 2)
    raw_rows = [
        {
            "ticker": "ABC",
            "trade_date": trade_date,
            "provider": "TWELVEDATA",
            "close_px": 12.34,
            "adj_close_px": None,
            "status": "OK",
        }
    ]

    canon_rows = compute_canonical_rows(raw_rows)

    assert len(canon_rows) == 1
    row = canon_rows[0]
    assert row["canon_adj_close_px"] == 12.34
    assert row["canon_close_px"] == 12.34
    assert row["chosen_provider"] == "TWELVEDATA"
    assert row["quality"] == "LOW"
    assert row["providers_ok"] == 1


def test_build_raw_rows_skips_null_close():
    rows = [
        {"ticker": "AAA", "trade_date": "2025-01-02", "close": None},
        {"ticker": "BBB", "trade_date": "2025-01-02", "close": 12.34},
    ]
    raw_rows = _build_raw_rows_from_provider(rows)
    assert len(raw_rows) == 2
    error_row = [row for row in raw_rows if row["ticker"] == "AAA"][0]
    ok_row = [row for row in raw_rows if row["ticker"] == "BBB"][0]
    assert error_row["status"] == "ERROR"
    assert ok_row["status"] == "OK"

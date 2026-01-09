from tools.index_engine import ingest_prices
from app.index_engine import db as engine_db


def test_split_tickers_normalizes_fi():
    assert ingest_prices._split_tickers("FI") == ["FISV"]
    assert ingest_prices._split_tickers("fi") == ["FISV"]


def test_build_raw_rows_normalizes_fi():
    rows = ingest_prices._build_raw_rows_from_provider(
        [
            {
                "trade_date": "2026-01-08",
                "ticker": "FI",
                "close": 10.0,
                "adj_close": 10.0,
            }
        ]
    )
    assert rows[0]["ticker"] == "FISV"


def test_db_normalize_ticker():
    assert engine_db.normalize_ticker("fi") == "FISV"

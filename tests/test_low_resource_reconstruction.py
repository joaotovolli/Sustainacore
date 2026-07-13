import datetime as dt
import gc
import tracemalloc

from app.index_engine.portfolio_analytics_v1 import (
    MetadataRow,
    OfficialDailyRow,
    OfficialPositionRow,
    PriceRow,
    build_portfolio_outputs,
)


def _fixture(day_count=100, ticker_count=10):
    start = dt.date(2025, 1, 2)
    days = [start + dt.timedelta(days=index) for index in range(day_count)]
    tickers = [f"T{index:02d}" for index in range(ticker_count)]
    daily = [OfficialDailyRow(day, 1000.0 + index) for index, day in enumerate(days)]
    positions = [
        OfficialPositionRow(day, days[0], ticker, 1.0 / ticker_count, "REAL", 0.0, 0.0)
        for day in days
        for ticker in tickers
    ]
    metadata = [MetadataRow(days[0], ticker, ticker, "Technology", 80.0) for ticker in tickers]
    prices = [
        PriceRow(day, ticker, 100.0 + day_index * 0.01 + ticker_index)
        for day_index, day in enumerate(days)
        for ticker_index, ticker in enumerate(tickers)
    ]
    return daily, positions, metadata, prices


def _peak_bytes(*, stream):
    daily, positions, metadata, prices = _fixture()
    counts = {"analytics": 0, "positions": 0}

    def sink(_spec, analytics, model_positions):
        counts["analytics"] += len(analytics)
        counts["positions"] += len(model_positions)

    gc.collect()
    tracemalloc.start()
    outputs = build_portfolio_outputs(
        official_daily_rows=daily,
        official_position_rows=positions,
        metadata_rows=metadata,
        price_rows=prices,
        model_output_callback=sink if stream else None,
    )
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    if stream:
        assert outputs["analytics"] == []
        assert outputs["positions"] == []
        assert counts == {"analytics": 600, "positions": 6000}
    return peak


def test_streamed_model_outputs_reduce_peak_python_allocation():
    legacy_peak = _peak_bytes(stream=False)
    streamed_peak = _peak_bytes(stream=True)
    assert streamed_peak < legacy_peak * 0.70

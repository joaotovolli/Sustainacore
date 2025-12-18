import datetime as _dt

from tools.index_engine.run_daily import compute_eligible_end_date


def test_compute_eligible_end_date_skips_same_day():
    provider_latest = _dt.date(2025, 12, 18)
    today = _dt.date(2025, 12, 19)
    trading_days = [
        _dt.date(2025, 12, 17),
        _dt.date(2025, 12, 18),
        _dt.date(2025, 12, 19),
    ]
    eligible = compute_eligible_end_date(provider_latest=provider_latest, today_utc=today, trading_days=trading_days)
    assert eligible == _dt.date(2025, 12, 18)


def test_compute_eligible_end_date_respects_trading_calendar():
    provider_latest = _dt.date(2025, 12, 20)
    today = _dt.date(2025, 12, 21)
    trading_days = [
        _dt.date(2025, 12, 17),
        _dt.date(2025, 12, 18),
        _dt.date(2025, 12, 19),
    ]
    eligible = compute_eligible_end_date(provider_latest=provider_latest, today_utc=today, trading_days=trading_days)
    assert eligible == _dt.date(2025, 12, 19)

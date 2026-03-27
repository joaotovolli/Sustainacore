import datetime as _dt

from tools.index_engine.run_daily import compute_eligible_end_date, derive_expected_target_date


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


def test_derive_expected_target_date_extends_weekday_gap():
    expected, source, effective_days, synthetic_days = derive_expected_target_date(
        provider_latest=_dt.date(2026, 3, 26),
        today_utc=_dt.date(2026, 3, 27),
        trading_days=[_dt.date(2026, 3, 25)],
        allow_weekday_fallback=True,
        max_gap_days=2,
    )

    assert expected == _dt.date(2026, 3, 26)
    assert source == "weekday_fallback"
    assert effective_days[-1] == _dt.date(2026, 3, 26)
    assert synthetic_days == [_dt.date(2026, 3, 26)]

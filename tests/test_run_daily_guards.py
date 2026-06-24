import datetime as _dt

from tools.index_engine.run_daily import (
    compute_eligible_end_date,
    derive_expected_target_date,
    normalize_provider_usage,
    provider_usage_budget,
)


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


def test_provider_usage_normalization_separates_daily_and_minute_capacity():
    fields = normalize_provider_usage(
        {
            "daily_current_usage": 2,
            "daily_plan_limit": 800,
            "minute_current_usage": 1,
            "minute_plan_limit": 8,
        },
        configured_daily_limit=800,
    )

    assert fields["daily_used"] == 2
    assert fields["daily_limit"] == 800
    assert fields["daily_remaining"] == 798
    assert fields["minute_used"] == 1
    assert fields["minute_limit"] == 8

    remaining, max_calls, reason = provider_usage_budget(
        daily_used=fields["daily_used"],
        daily_limit=fields["daily_limit"],
        safety_buffer=100,
        per_run_limit=50,
        daily_budget=775,
    )

    assert remaining == 798
    assert max_calls == 50
    assert reason is None


def test_provider_usage_normalization_does_not_treat_minute_limit_as_daily_quota():
    fields = normalize_provider_usage(
        {"current_usage": 1, "plan_limit": 8},
        configured_daily_limit=800,
    )

    assert fields["daily_used"] is None
    assert fields["daily_limit"] is None
    assert fields["minute_used"] == 1
    assert fields["minute_limit"] == 8

    remaining, max_calls, reason = provider_usage_budget(
        daily_used=fields["daily_used"],
        daily_limit=fields["daily_limit"],
        safety_buffer=100,
        per_run_limit=50,
        daily_budget=775,
    )

    assert remaining is None
    assert max_calls == 0
    assert reason == "provider_usage_unavailable"


def test_provider_usage_budget_blocks_when_daily_remaining_below_buffer():
    remaining, max_calls, reason = provider_usage_budget(
        daily_used=725,
        daily_limit=800,
        safety_buffer=100,
        per_run_limit=50,
        daily_budget=775,
    )

    assert remaining == 75
    assert max_calls == 0
    assert reason == "quota_safety_buffer"

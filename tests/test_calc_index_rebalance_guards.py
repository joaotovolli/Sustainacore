import datetime as dt

import pytest

from app.index_engine.index_calc_v1 import compute_holdings_at_rebalance
from tools.index_engine import calc_index as ci


def test_rebalance_prior_price_missing_fails_closed() -> None:
    with pytest.raises(ci.IndexValidationError, match="rebalance_prior_price_missing"):
        ci.validate_rebalance_prior_prices(
            rebalance_date=dt.date(2026, 7, 1),
            prev_date=dt.date(2026, 6, 30),
            tickers=["AAA", "BBB"],
            prices_prev={"AAA": {"price": 100.0, "quality": "REAL"}},
        )


def test_missing_exact_rebalance_anchor_is_backfilled_once(monkeypatch) -> None:
    day = dt.date(2025, 12, 31)
    responses = [
        {"AAA": {"price": 100.0, "quality": "REAL"}},
        {
            "AAA": {"price": 100.0, "quality": "REAL"},
            "BBB": {"price": 200.0, "quality": "REAL"},
        },
    ]
    calls = []
    monkeypatch.setattr(ci.db, "fetch_prices", lambda *_args, **_kwargs: responses.pop(0))
    monkeypatch.setattr(
        ci,
        "_attempt_missing_backfill",
        lambda **kwargs: calls.append(kwargs) or (1, {}),
    )

    prices = ci.fetch_rebalance_prior_prices(
        prev_date=day, tickers=["AAA", "BBB"], allow_close=False
    )
    ci.validate_rebalance_prior_prices(
        rebalance_date=dt.date(2026, 1, 2),
        prev_date=day,
        tickers=["AAA", "BBB"],
        prices_prev=prices,
    )

    assert calls == [{"trade_date": day, "tickers": ["BBB"], "max_provider_calls": 1}]


def test_rebalance_anchor_still_fails_closed_after_backfill(monkeypatch) -> None:
    day = dt.date(2025, 12, 31)
    monkeypatch.setattr(ci.db, "fetch_prices", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(ci, "_attempt_missing_backfill", lambda **_kwargs: (0, {}))
    prices = ci.fetch_rebalance_prior_prices(prev_date=day, tickers=["AAA"], allow_close=False)
    with pytest.raises(ci.IndexValidationError, match="rebalance_prior_price_missing"):
        ci.validate_rebalance_prior_prices(
            rebalance_date=dt.date(2026, 1, 2),
            prev_date=day,
            tickers=["AAA"],
            prices_prev=prices,
        )


def test_rebalance_anchor_retry_is_limited_to_strict_rebuild() -> None:
    assert ci.should_retry_rebalance_anchor(rebuild=True, strict=True)
    assert not ci.should_retry_rebalance_anchor(rebuild=False, strict=True)
    assert not ci.should_retry_rebalance_anchor(rebuild=True, strict=False)
    assert not ci.should_retry_rebalance_anchor(rebuild=False, strict=False)


def test_stale_historical_anchor_is_rejected_for_rebalance() -> None:
    with pytest.raises(ci.IndexValidationError, match="rebalance_prior_price_stale_anchor"):
        ci.validate_rebalance_prior_prices(
            rebalance_date=dt.date(2026, 7, 1),
            prev_date=dt.date(2026, 6, 30),
            tickers=["AAA"],
            prices_prev={"AAA": {"price": 100.0, "quality": "HISTORICAL"}},
        )


def test_split_price_basis_mismatch_fails_return_sanity() -> None:
    with pytest.raises(ci.IndexValidationError, match="suspicious_price_return"):
        ci.validate_price_return_sanity(
            prev_date=dt.date(2026, 6, 30),
            trade_date=dt.date(2026, 7, 1),
            tickers=["CRWD"],
            prices_prev={"CRWD": 190.785},
            prices_now={"CRWD": 772.74},
            max_abs_return=0.20,
        )


def test_normal_rebalance_continuity_passes() -> None:
    prev_level = 1397.334068
    prev_prices = {"AAA": 100.0, "BBB": 200.0}
    shares, divisor = compute_holdings_at_rebalance(
        tickers=["AAA", "BBB"],
        prices_prev=prev_prices,
        level_prev=prev_level,
        divisor_prev=1.0,
    )

    ci.validate_rebalance_continuity(
        rebalance_date=dt.date(2026, 7, 1),
        prev_date=dt.date(2026, 6, 30),
        prev_level=prev_level,
        shares=shares,
        prices_prev=prev_prices,
        divisor=divisor,
        abs_tol=1e-6,
        rel_tol=1e-8,
    )


def test_non_rebalance_daily_move_below_threshold_passes() -> None:
    ci.validate_price_return_sanity(
        prev_date=dt.date(2026, 7, 1),
        trade_date=dt.date(2026, 7, 2),
        tickers=["AAA", "BBB"],
        prices_prev={"AAA": 100.0, "BBB": 200.0},
        prices_now={"AAA": 101.0, "BBB": 198.0},
        max_abs_return=0.20,
    )


def test_validation_failure_does_not_publish_or_delete_rows(monkeypatch, capsys) -> None:
    prev_trade = dt.date(2026, 6, 30)
    trade_date = dt.date(2026, 7, 1)
    calls: list[str] = []
    finish_calls: list[dict[str, object]] = []

    monkeypatch.setattr(ci, "load_default_env", lambda: None)
    monkeypatch.setattr(ci, "start_run", lambda *args, **kwargs: "run-1")

    def finish_run(run_id, *, status, error):
        finish_calls.append({"run_id": run_id, "status": status, "error": error})

    monkeypatch.setattr(ci, "finish_run", finish_run)
    monkeypatch.setattr(ci.db, "fetch_trading_days", lambda start, end: [trade_date])
    monkeypatch.setattr(ci.db, "fetch_calc_completion_max_date", lambda: prev_trade)
    monkeypatch.setattr(ci.engine_db, "fetch_max_canon_trade_date", lambda: trade_date)
    monkeypatch.setattr(ci.db, "fetch_last_level_before", lambda start: (prev_trade, 1397.334068))
    monkeypatch.setattr(ci.db, "fetch_constituent_shares", lambda date: {"OLD": 1.0})
    monkeypatch.setattr(ci.db, "fetch_latest_rebalance_date", lambda start: None)
    monkeypatch.setattr(ci.db, "fetch_holdings_for_rebalance", lambda date: {})
    monkeypatch.setattr(ci.db, "fetch_divisor_for_date", lambda date: None)
    monkeypatch.setattr(ci.db, "fetch_level_for_date", lambda date: None)

    def fetch_universe(as_of_date):
        if as_of_date == prev_trade:
            return dt.date(2026, 4, 1), ["OLD"]
        return trade_date, ["AAA", "BBB"]

    monkeypatch.setattr(ci.db, "fetch_universe", fetch_universe)

    def fetch_prices(as_of_date, tickers, *, allow_close):
        requested = set(tickers)
        if as_of_date == prev_trade and requested == {"OLD"}:
            return {"OLD": {"price": 10.0, "quality": "REAL"}}
        if as_of_date == prev_trade:
            return {"AAA": {"price": 100.0, "quality": "REAL"}}
        if as_of_date == trade_date:
            return {
                "AAA": {"price": 101.0, "quality": "REAL"},
                "BBB": {"price": 99.0, "quality": "REAL"},
            }
        return {}

    monkeypatch.setattr(ci.db, "fetch_prices", fetch_prices)
    monkeypatch.setattr(ci, "_attempt_missing_backfill", lambda **_kwargs: (0, {}))

    def record_write(name):
        def inner(*args, **kwargs):
            calls.append(name)

        return inner

    for name in (
        "delete_index_range",
        "delete_holdings_divisor",
        "upsert_holdings",
        "upsert_divisor",
        "upsert_constituent_daily",
        "upsert_levels",
        "upsert_contribution_daily",
        "upsert_stats_daily",
    ):
        monkeypatch.setattr(ci.db, name, record_write(name))

    code = ci.main(
        [
            "--start",
            trade_date.isoformat(),
            "--end",
            trade_date.isoformat(),
            "--rebuild",
            "--strict",
            "--no-preflight-self-heal",
            "--no-diagnose-missing-sql",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert calls == []
    assert finish_calls[-1]["status"] == "ERROR"
    assert "rebalance_prior_price_missing" in str(finish_calls[-1]["error"])
    assert "rebalance_prior_price_missing" in captured.err

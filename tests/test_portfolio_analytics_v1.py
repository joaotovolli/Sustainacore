import datetime as dt

import pytest

from app.index_engine.portfolio_analytics_v1 import (
    DEFAULT_MODEL_SPECS,
    MetadataRow,
    OfficialDailyRow,
    OfficialPositionRow,
    PriceRow,
    build_model_target_weights,
    build_portfolio_outputs,
    compute_factor_history,
)


def test_build_model_target_weights_supports_equal_and_governance_tilt():
    benchmark = {"AAA": 0.60, "BBB": 0.40}
    governance = {"AAA": 90.0, "BBB": 60.0}
    momentum = {"AAA": 0.10, "BBB": 0.02}
    low_vol = {"AAA": 0.25, "BBB": 0.40}

    equal = build_model_target_weights(
        model_code="TECH100_EQ",
        benchmark_weights=benchmark,
        governance_scores=governance,
        momentum_scores=momentum,
        low_vol_scores=low_vol,
    )
    assert equal == {"AAA": 0.5, "BBB": 0.5}

    tilted = build_model_target_weights(
        model_code="TECH100_GOV",
        benchmark_weights=benchmark,
        governance_scores=governance,
        momentum_scores=momentum,
        low_vol_scores=low_vol,
    )
    assert tilted["AAA"] > benchmark["AAA"]
    assert tilted["BBB"] < benchmark["BBB"]
    assert pytest.approx(sum(tilted.values())) == 1.0


def test_compute_factor_history_uses_trailing_windows():
    trade_days = [
        dt.date(2026, 1, 2),
        dt.date(2026, 1, 5),
        dt.date(2026, 1, 6),
        dt.date(2026, 1, 7),
    ]
    price_by_date = {
        trade_days[0]: {"AAA": 100.0, "BBB": 100.0},
        trade_days[1]: {"AAA": 110.0, "BBB": 95.0},
        trade_days[2]: {"AAA": 121.0, "BBB": 90.0},
        trade_days[3]: {"AAA": 133.1, "BBB": 91.8},
    }

    history = compute_factor_history(
        trade_days,
        price_by_date,
        {"AAA", "BBB"},
        momentum_window=2,
        low_vol_window=2,
    )

    assert history[trade_days[2]]["AAA"].momentum_20d == pytest.approx(0.21)
    assert history[trade_days[2]]["BBB"].momentum_20d == pytest.approx(-0.10)
    assert history[trade_days[2]]["AAA"].low_vol_60d is not None


def test_build_portfolio_outputs_generates_rows_for_all_models():
    trade_days = [
        dt.date(2026, 1, 2),
        dt.date(2026, 1, 5),
        dt.date(2026, 1, 6),
        dt.date(2026, 1, 7),
    ]
    official_daily = [
        OfficialDailyRow(trade_date=trade_days[0], level_tr=100.0),
        OfficialDailyRow(trade_date=trade_days[1], level_tr=101.0, ret_1d=0.01),
        OfficialDailyRow(trade_date=trade_days[2], level_tr=102.0, ret_1d=0.0099),
        OfficialDailyRow(trade_date=trade_days[3], level_tr=103.0, ret_1d=0.0098),
    ]
    official_positions = [
        OfficialPositionRow(trade_date=trade_days[0], rebalance_date=trade_days[0], ticker="AAA", weight=0.60),
        OfficialPositionRow(trade_date=trade_days[0], rebalance_date=trade_days[0], ticker="BBB", weight=0.40),
        OfficialPositionRow(
            trade_date=trade_days[1],
            rebalance_date=trade_days[0],
            ticker="AAA",
            weight=0.61,
            contribution_1d=0.012,
            ret_1d=0.02,
        ),
        OfficialPositionRow(
            trade_date=trade_days[1],
            rebalance_date=trade_days[0],
            ticker="BBB",
            weight=0.39,
            contribution_1d=-0.002,
            ret_1d=-0.005,
        ),
        OfficialPositionRow(trade_date=trade_days[2], rebalance_date=trade_days[2], ticker="AAA", weight=0.50),
        OfficialPositionRow(trade_date=trade_days[2], rebalance_date=trade_days[2], ticker="BBB", weight=0.50),
        OfficialPositionRow(
            trade_date=trade_days[3],
            rebalance_date=trade_days[2],
            ticker="AAA",
            weight=0.52,
            contribution_1d=0.008,
            ret_1d=0.015,
        ),
        OfficialPositionRow(
            trade_date=trade_days[3],
            rebalance_date=trade_days[2],
            ticker="BBB",
            weight=0.48,
            contribution_1d=0.001,
            ret_1d=0.002,
        ),
    ]
    metadata_rows = [
        MetadataRow(
            port_date=trade_days[0],
            ticker="AAA",
            company_name="Alpha",
            sector="Software",
            governance_score=92.0,
            transparency=95.0,
        ),
        MetadataRow(
            port_date=trade_days[0],
            ticker="BBB",
            company_name="Beta",
            sector="Semiconductors",
            governance_score=58.0,
            transparency=60.0,
        ),
        MetadataRow(
            port_date=trade_days[2],
            ticker="AAA",
            company_name="Alpha",
            sector="Software",
            governance_score=90.0,
            transparency=90.0,
        ),
        MetadataRow(
            port_date=trade_days[2],
            ticker="BBB",
            company_name="Beta",
            sector="Semiconductors",
            governance_score=62.0,
            transparency=65.0,
        ),
    ]
    price_rows = [
        PriceRow(trade_date=trade_days[0], ticker="AAA", price=100.0),
        PriceRow(trade_date=trade_days[0], ticker="BBB", price=100.0),
        PriceRow(trade_date=trade_days[1], ticker="AAA", price=102.0),
        PriceRow(trade_date=trade_days[1], ticker="BBB", price=99.0),
        PriceRow(trade_date=trade_days[2], ticker="AAA", price=103.0),
        PriceRow(trade_date=trade_days[2], ticker="BBB", price=101.0),
        PriceRow(trade_date=trade_days[3], ticker="AAA", price=104.0),
        PriceRow(trade_date=trade_days[3], ticker="BBB", price=102.0),
    ]

    outputs = build_portfolio_outputs(
        official_daily_rows=official_daily,
        official_position_rows=official_positions,
        metadata_rows=metadata_rows,
        price_rows=price_rows,
    )

    assert len(outputs["analytics"]) == len(DEFAULT_MODEL_SPECS) * len(trade_days)
    assert len(outputs["optimizer_inputs"]) == 4

    gov_rows = [
        row
        for row in outputs["positions"]
        if row["model_code"] == "TECH100_GOV" and row["trade_date"] == trade_days[0]
    ]
    aaa = next(row for row in gov_rows if row["ticker"] == "AAA")
    bbb = next(row for row in gov_rows if row["ticker"] == "BBB")
    assert aaa["model_weight"] > 0.60
    assert bbb["model_weight"] < 0.40

    eq_rows = [
        row
        for row in outputs["positions"]
        if row["model_code"] == "TECH100_EQ" and row["trade_date"] == trade_days[0]
    ]
    assert {row["ticker"]: row["model_weight"] for row in eq_rows} == {"AAA": 0.5, "BBB": 0.5}

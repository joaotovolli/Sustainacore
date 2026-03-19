import datetime as dt

import tools.index_engine.pipeline_health as pipeline_health


def test_collect_health_snapshot_reports_portfolio_gap(monkeypatch):
    def fake_fetch_scalar(sql, binds=None):
        if sql == "SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS":
            return dt.date(2026, 3, 18)
        if sql == "SELECT MAX(trade_date) FROM SC_IDX_PRICES_CANON":
            return dt.date(2026, 3, 18)
        if sql == "SELECT COUNT(*) FROM SC_IDX_PRICES_CANON WHERE trade_date = :trade_date":
            return 25
        if sql == "SELECT level_tr FROM SC_IDX_LEVELS WHERE index_code = 'TECH100' AND trade_date = :trade_date":
            return 1015.728415
        if sql == "SELECT MAX(trade_date) FROM SC_IDX_STATS_DAILY":
            return dt.date(2026, 3, 18)
        if sql == "SELECT ret_1d FROM SC_IDX_STATS_DAILY WHERE trade_date = :trade_date":
            return -0.0012
        if sql == "SELECT MAX(trade_date) FROM SC_IDX_CONTRIBUTION_DAILY":
            return dt.date(2026, 3, 18)
        if sql == "SELECT COUNT(*) FROM SC_IDX_CONTRIBUTION_DAILY WHERE trade_date = :trade_date":
            return 25
        if sql == "SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY":
            return dt.date(2026, 3, 16)
        if (
            sql
            == "SELECT COUNT(DISTINCT model_code) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY "
            "WHERE trade_date = :trade_date"
        ):
            return 6
        if sql == "SELECT MIN(trade_date) FROM SC_IDX_TRADING_DAYS WHERE trade_date > :trade_date":
            return dt.date(2026, 3, 19)
        if (
            sql == "SELECT COUNT(*) FROM SC_IDX_JOB_RUNS "
            "WHERE started_at >= SYSTIMESTAMP - INTERVAL '1' DAY "
            "AND error_msg LIKE '%ORA-%'"
        ):
            return 0
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(pipeline_health, "_fetch_scalar", fake_fetch_scalar)
    monkeypatch.setattr(
        pipeline_health,
        "_fetch_one",
        lambda sql, binds=None: (dt.date(2026, 3, 18),),
    )

    health = pipeline_health.collect_health_snapshot(
        stage_durations={"portfolio_analytics": 1.25},
        last_error=None,
    )

    assert health["levels_max_date"] == "2026-03-18"
    assert health["portfolio_max_date"] == "2026-03-16"
    assert health["portfolio_expected_date"] == "2026-03-18"
    assert health["portfolio_gap_days"] == 2
    assert health["portfolio_in_sync"] is False


def test_format_health_summary_includes_portfolio_sync_fields():
    summary = pipeline_health.format_health_summary(
        {
            "calendar_max_date": "2026-03-18",
            "canon_max_date": "2026-03-18",
            "canon_count_latest_day": 25,
            "levels_max_date": "2026-03-18",
            "level_latest": 1015.728415,
            "stats_max_date": "2026-03-18",
            "ret_1d_latest": -0.0012,
            "contrib_max_date": "2026-03-18",
            "contrib_count_latest_day": 25,
            "portfolio_max_date": "2026-03-18",
            "portfolio_model_count_latest_day": 6,
            "portfolio_expected_date": "2026-03-18",
            "portfolio_gap_days": 0,
            "portfolio_in_sync": True,
            "next_missing_trading_day": "2026-03-19",
            "oracle_error_counts_24h": 0,
            "last_error": None,
            "stage_durations_sec": {"portfolio_analytics": 1.25},
        }
    )

    assert "portfolio_expected_date=2026-03-18" in summary
    assert "portfolio_gap_days=0" in summary
    assert "portfolio_in_sync=True" in summary

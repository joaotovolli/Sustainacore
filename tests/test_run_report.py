import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.run_report import build_pipeline_run_summary


def test_build_pipeline_run_summary_marks_success_stale_when_stats_before_lag():
    summary = build_pipeline_run_summary(
        run_id="run-stale",
        terminal_status="success",
        started_at="2026-04-02T05:30:24+00:00",
        stage_results={
            "determine_target_dates": {"status": "OK", "counts": {}, "attempts": 1},
            "calc_index": {"status": "SKIP", "counts": {}, "attempts": 1},
            "portfolio_analytics": {"status": "SKIP", "counts": {}, "attempts": 1},
        },
        context={
            "ended_at": "2026-04-02T05:30:29+00:00",
            "expected_target_date": "2026-04-01",
            "expected_target_source": "calendar",
            "max_canon_before": "2026-04-01",
            "max_level_before": "2026-04-01",
            "max_stats_before": "2026-03-31",
            "max_portfolio_before": "2026-04-01",
            "max_portfolio_position_before": "2026-04-01",
            "repo_root": "/repo",
            "repo_head": "abc1234",
        },
        warnings=[],
        status_reason=None,
        root_cause=None,
        remediation=None,
    )

    assert summary["overall_health"] == "Stale"
    assert summary["freshness"]["stats_max_date"] == "2026-03-31"
    assert summary["freshness"]["health"]["verdict"] == "stale"
    assert "stats_behind_levels" in summary["freshness"]["health"]["stale_signals"]


def test_build_pipeline_run_summary_does_not_set_failed_stage_for_clean_skip():
    summary = build_pipeline_run_summary(
        run_id="run-skip",
        terminal_status="clean_skip",
        started_at="2026-05-29T18:56:49+00:00",
        stage_results={
            "determine_target_dates": {"status": "SKIP", "counts": {}, "attempts": 1},
            "generate_run_report": {"status": "OK", "counts": {}, "attempts": 1},
        },
        context={
            "ended_at": "2026-05-29T18:58:51+00:00",
            "expected_target_date": "2026-05-28",
            "expected_target_source": "provider_not_ready",
            "max_canon_before": "2026-05-27",
            "max_level_before": "2026-05-27",
            "max_stats_before": "2026-05-27",
            "max_portfolio_before": "2026-05-27",
            "max_portfolio_position_before": "2026-05-27",
            "provider_ready": False,
            "repo_root": "/repo",
            "repo_head": "abc1234",
        },
        warnings=[],
        status_reason="provider_not_ready",
        root_cause=None,
        remediation="Wait for the market data provider to publish the latest EOD bars, then rerun.",
    )

    assert summary["overall_health"] == "Skipped"
    assert summary["failed_stage"] is None
    assert summary["freshness"]["health"]["verdict"] == "provider_not_ready"


def test_degraded_target_date_warning_keeps_aligned_data_fresh():
    summary = build_pipeline_run_summary(
        run_id="run-degraded-fresh",
        terminal_status="success_with_degradation",
        started_at="2026-05-31T13:14:59+00:00",
        stage_results={
            "determine_target_dates": {"status": "DEGRADED", "counts": {}, "attempts": 1},
            "calc_index": {"status": "OK", "counts": {}, "attempts": 1},
            "portfolio_analytics": {"status": "OK", "counts": {}, "attempts": 1},
            "generate_run_report": {"status": "OK", "counts": {}, "attempts": 1},
        },
        context={
            "ended_at": "2026-05-31T13:15:22+00:00",
            "expected_target_date": "2026-05-29",
            "expected_target_source": "weekday_fallback",
            "max_canon_after_ingest": "2026-05-29",
            "levels_max_after": "2026-05-29",
            "stats_max_after": "2026-05-29",
            "portfolio_max_after": "2026-05-29",
            "portfolio_position_max_after": "2026-05-29",
            "repo_root": "/repo",
            "repo_head": "abc1234",
        },
        warnings=["trading_days_weekday_fallback:start=2026-05-29 end=2026-05-29 count=1"],
        status_reason="determine_target_dates",
        root_cause=None,
        remediation=None,
    )

    assert summary["terminal_status"] == "success_with_degradation"
    assert summary["freshness"]["health"]["verdict"] == "fresh"
    assert summary["freshness"]["health"]["reason"] == "aligned_with_expected"

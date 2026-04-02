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

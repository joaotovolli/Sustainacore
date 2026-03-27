import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.run_report import build_pipeline_daily_summary, format_pipeline_daily_report
import tools.index_engine.daily_telemetry_report as daily_telemetry_report


def _summary(terminal_status: str = "success_with_degradation") -> dict:
    return {
        "run_id": "run-123",
        "terminal_status": terminal_status,
        "overall_health": "Degraded" if terminal_status == "success_with_degradation" else terminal_status.title(),
        "started_at": "2026-01-08T00:00:00+00:00",
        "ended_at": "2026-01-08T00:05:00+00:00",
        "duration_sec": 300.0,
        "latest_successful_trade_date": "2026-01-07",
        "status_reason": "imputation_or_replacement" if terminal_status == "success_with_degradation" else "provider_not_ready",
        "root_cause": None if terminal_status != "failed" else "calc_index",
        "remediation": "Review the latest report.",
        "warnings": ["daily_budget_partial"] if terminal_status == "success_with_degradation" else [],
        "counts": {
            "raw_upserts": 25,
            "canon_upserts": 25,
            "total_imputed": 2 if terminal_status == "success_with_degradation" else 0,
            "levels_rows": 1,
            "stats_rows": 1,
            "portfolio_analytics_rows": 6,
            "portfolio_position_rows": 18,
        },
        "provider_readiness": {
            "candidate_end_date": "2026-01-07",
            "ready_end_date": "2026-01-07" if terminal_status != "clean_skip" else None,
            "provider_usage_current": 1,
            "provider_usage_limit": 8,
            "provider_usage_remaining": 7,
            "calls_used_today": 10,
        },
        "artifact_paths": {
            "latest_report_json_path": "/tmp/latest_report.json",
            "latest_report_text_path": "/tmp/latest_report.txt",
            "telemetry_path": "/tmp/latest_telemetry.json",
        },
        "alert_decision": {
            "decision": "sent" if terminal_status == "failed" else "skipped",
            "email_sent": terminal_status == "failed",
            "deduplicated": False,
            "delivery": {
                "ready": terminal_status == "failed",
                "delivery_state": "sent" if terminal_status == "failed" else "not_attempted",
                "message_id": "<msg-123@sustainacore.org>" if terminal_status == "failed" else None,
                "mail_to_count": 1 if terminal_status == "failed" else 0,
                "missing_env": [],
            },
        },
        "stage_results": {
            "preflight_oracle": {
                "status": "OK",
                "duration_sec": 0.1,
                "attempts": 1,
                "warnings": [],
                "counts": {},
            },
            "determine_target_dates": {
                "status": "DEGRADED" if terminal_status == "success_with_degradation" else "SKIP",
                "duration_sec": 0.1,
                "attempts": 1,
                "warnings": ["daily_budget_partial"] if terminal_status == "success_with_degradation" else [],
                "counts": {},
            },
            "portfolio_analytics": {
                "status": "OK",
                "duration_sec": 0.2,
                "attempts": 1,
                "warnings": [],
                "counts": {"portfolio_analytics_rows": 6, "portfolio_position_rows": 18},
            },
        },
    }


def _telemetry() -> dict:
    return {
        "run_id": "run-123",
        "terminal_status": "success_with_degradation",
        "status_reason": "imputation_or_replacement",
        "root_cause": None,
        "remediation": "Review the latest report.",
        "stage_results": _summary()["stage_results"],
        "warnings": ["daily_budget_partial"],
        "alert": {"decision": "skipped", "email_sent": False},
        "freshness": {
            "canon_max_date": "2026-01-07",
            "levels_max_date": "2026-01-07",
            "stats_max_date": "2026-01-07",
            "portfolio_max_date": "2026-01-07",
        },
    }


def test_daily_summary_flags_stale_portfolio():
    report = build_pipeline_daily_summary(
        latest_summary=_summary(),
        latest_telemetry=_telemetry(),
        health_snapshot={
            "calendar_max_date": "2026-01-08",
            "canon_max_date": "2026-01-07",
            "levels_max_date": "2026-01-07",
            "stats_max_date": "2026-01-07",
            "portfolio_analytics_max_date": "2026-01-05",
            "portfolio_position_max_date": "2026-01-05",
            "repo_root": "/repo",
            "repo_head": "abc1234",
        },
    )

    assert report["headline"]["overall_health"] == "Stale"
    assert report["headline"]["portfolio_in_sync"] is False
    assert report["headline"]["repo_head"] == "abc1234"
    assert report["freshness"]["alignment"]["verdict"] == "out_of_sync"
    assert report["freshness"]["health"]["verdict"] == "stale"
    assert "portfolio_analytics_behind_levels" in report["freshness"]["health"]["stale_signals"]


def test_daily_report_renders_clean_skip():
    summary = _summary("clean_skip")
    telemetry = _telemetry()
    telemetry["terminal_status"] = "clean_skip"
    report = build_pipeline_daily_summary(
        latest_summary=summary,
        latest_telemetry=telemetry,
        health_snapshot={"calendar_max_date": "2026-01-08"},
    )

    text = format_pipeline_daily_report(report)

    assert "overall_health: Skipped" in text
    assert "terminal_status: clean_skip" in text
    assert "Section 3: Stage-by-stage outcome" in text


def test_daily_report_renders_failed_alert_details():
    summary = _summary("failed")
    telemetry = _telemetry()
    telemetry["terminal_status"] = "failed"
    telemetry["root_cause"] = "calc_index"
    report = build_pipeline_daily_summary(
        latest_summary=summary,
        latest_telemetry=telemetry,
        health_snapshot={
            "calendar_max_date": "2026-01-08",
            "canon_max_date": "2026-01-07",
            "levels_max_date": "2026-01-07",
            "stats_max_date": "2026-01-07",
            "portfolio_analytics_max_date": "2026-01-06",
            "portfolio_position_max_date": "2026-01-06",
        },
    )

    text = format_pipeline_daily_report(report)

    assert "overall_health: Failed" in text
    assert "root_cause: calc_index" in text
    assert "smtp_delivery_state: sent" in text
    assert "smtp_message_id: <msg-123@sustainacore.org>" in text
    assert "alignment_verdict: partially_aligned" in text


def test_daily_report_renders_degraded_success_metrics():
    report = build_pipeline_daily_summary(
        latest_summary=_summary(),
        latest_telemetry=_telemetry(),
        health_snapshot={
            "calendar_max_date": "2026-01-08",
            "canon_max_date": "2026-01-07",
            "levels_max_date": "2026-01-07",
            "stats_max_date": "2026-01-07",
            "portfolio_analytics_max_date": "2026-01-07",
            "portfolio_position_max_date": "2026-01-07",
        },
    )

    text = format_pipeline_daily_report(report)

    assert "overall_health: Degraded" in text
    assert "imputed_rows: 2" in text
    assert "alignment_verdict: aligned" in text
    assert "portfolio_in_sync: True" in text


def test_daily_report_script_writes_artifacts_without_db(tmp_path, monkeypatch, capsys):
    report_json = tmp_path / "report.json"
    telemetry_json = tmp_path / "telemetry.json"
    outdir = tmp_path / "out"
    report_json.write_text(json.dumps(_summary()), encoding="utf-8")
    telemetry_json.write_text(json.dumps(_telemetry()), encoding="utf-8")
    monkeypatch.setenv("SMTP_PASS", "supersecret")

    rc = daily_telemetry_report.main(
        [
            "--report-json",
            str(report_json),
            "--telemetry-json",
            str(telemetry_json),
            "--output-dir",
            str(outdir),
            "--skip-db",
            "--dry-run",
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert (outdir / "sc_idx_daily_report_latest.json").exists()
    assert (outdir / "sc_idx_daily_report_latest.txt").exists()
    assert "supersecret" not in captured.out
    assert "supersecret" not in (outdir / "sc_idx_daily_report_latest.txt").read_text(encoding="utf-8")

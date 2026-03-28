import datetime as dt
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
import sys
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.orchestration import (
    PipelineArgs,
    PipelineGraphState,
    SCIdxPipelineRuntime,
    SmokePipelineRuntime,
    _decide_alerts,
    build_pipeline_graph,
    run_pipeline,
)
import tools.index_engine.run_pipeline as pipeline_cli


@dataclass
class _Record:
    status: str
    details: str | None = None
    started_at: dt.datetime | None = None
    ended_at: dt.datetime | None = None


class _FakeStore:
    def __init__(self, stage_records=None):
        self.stage_records = stage_records or {}

    def create_run_id(self):
        return "run-test"

    def fetch_resume_run_id(self, run_date=None):
        return "run-test"

    def fetch_stage_statuses(self, run_id):
        return self.stage_records

    def record_stage_start(self, run_id, stage_name, details=None):
        now = dt.datetime(2026, 1, 7, tzinfo=dt.timezone.utc)
        self.stage_records[stage_name] = _Record(
            status="STARTED",
            details=details,
            started_at=now,
            ended_at=None,
        )

    def record_stage_end(self, run_id, stage_name, status, details=None):
        now = dt.datetime(2026, 1, 7, tzinfo=dt.timezone.utc)
        self.stage_records[stage_name] = _Record(
            status=status,
            details=details,
            started_at=now,
            ended_at=now,
        )


def _state(**overrides) -> PipelineGraphState:
    state: PipelineGraphState = {
        "run_id": "run-test",
        "run_date": "2026-01-07",
        "started_at": "2026-01-07T00:00:00+00:00",
        "resume": False,
        "restart": True,
        "smoke": True,
        "skip_ingest": False,
        "smoke_scenario": "success",
        "warnings": [],
        "stage_results": {},
        "context": {},
        "report": {},
        "alert": {},
        "telemetry": {},
        "terminal_status": None,
    }
    state.update(overrides)
    return state


def test_smoke_success_writes_report_and_terminal_state(tmp_path):
    exit_code, state = run_pipeline(
        PipelineArgs(smoke=True, smoke_scenario="success", report_dir=tmp_path, restart=True)
    )

    report_paths = (state.get("context") or {}).get("report_paths") or {}
    assert exit_code == 0
    assert state["terminal_status"] == "success"
    assert state["report"]["ended_at"] is not None
    assert state["telemetry"]["ended_at"] is not None
    assert state["report"]["duration_sec"] is not None
    assert state["report"]["failed_stage"] is None
    assert Path(report_paths["json_path"]).exists()
    assert Path(report_paths["text_path"]).exists()


def test_smoke_terminal_scenarios(tmp_path):
    scenarios = {
        "clean_skip": (0, "clean_skip"),
        "failed": (1, "failed"),
        "blocked": (2, "blocked"),
        "degraded": (0, "success_with_degradation"),
    }

    for scenario, expected in scenarios.items():
        exit_code, state = run_pipeline(
            PipelineArgs(smoke=True, smoke_scenario=scenario, report_dir=tmp_path / scenario, restart=True)
        )
        assert exit_code == expected[0]
        assert state["terminal_status"] == expected[1]


def test_graph_retries_transient_stage_once_then_recovers(tmp_path):
    class RetryRuntime(SmokePipelineRuntime):
        def __init__(self):
            super().__init__(
                smoke_scenario="success",
                report_dir=tmp_path,
                state_store=_FakeStore(),
            )
            self.completeness_attempts = 0

        def completeness_check(self, state):
            self.completeness_attempts += 1
            if self.completeness_attempts == 1:
                return {
                    "status": "FAILED",
                    "detail": "transient_oracle",
                    "error": "ORA-12545: Connect failed",
                    "error_token": "oracle_transient",
                    "retryable": True,
                }
            return {"status": "OK", "detail": "recovered"}

    runtime = RetryRuntime()
    graph = build_pipeline_graph(runtime)
    final_state = graph.invoke(_state())

    assert runtime.completeness_attempts == 2
    assert final_state["terminal_status"] == "success"
    assert final_state["stage_results"]["completeness_check"]["status"] == "OK"


def test_resume_skips_completed_stage(tmp_path):
    stage_payload = {
        "stage": "preflight_oracle",
        "status": "OK",
        "detail": "oracle_user=RESUMED",
        "detail_json": {"context": {"oracle_user": "RESUMED"}, "counts": {}, "warnings": []},
        "attempts": 1,
        "started_at": "2026-01-07T00:00:00+00:00",
        "ended_at": "2026-01-07T00:00:00+00:00",
        "duration_sec": 0.0,
        "counts": {},
        "warnings": [],
    }
    store = _FakeStore(
        {
            "preflight_oracle": _Record(
                status="OK",
                details=json.dumps(stage_payload),
                started_at=dt.datetime(2026, 1, 7, tzinfo=dt.timezone.utc),
                ended_at=dt.datetime(2026, 1, 7, tzinfo=dt.timezone.utc),
            )
        }
    )

    class ResumeRuntime(SmokePipelineRuntime):
        def __init__(self):
            super().__init__(smoke_scenario="success", report_dir=tmp_path, state_store=store)
            self.preflight_calls = 0

        def preflight_oracle(self, state):
            self.preflight_calls += 1
            return super().preflight_oracle(state)

    runtime = ResumeRuntime()
    graph = build_pipeline_graph(runtime)
    final_state = graph.invoke(_state(resume=True, restart=False))

    assert runtime.preflight_calls == 0
    assert final_state["stage_results"]["preflight_oracle"]["detail"] == "oracle_user=RESUMED"
    assert final_state["terminal_status"] == "success"


def test_graph_runs_portfolio_stage_before_report(tmp_path):
    class PortfolioRuntime(SmokePipelineRuntime):
        def __init__(self):
            super().__init__(
                smoke_scenario="success",
                report_dir=tmp_path,
                state_store=_FakeStore(),
            )
            self.portfolio_calls = 0

        def portfolio_analytics(self, state):
            self.portfolio_calls += 1
            return {
                "status": "OK",
                "detail": "portfolio_advanced_to=2026-01-07",
                "counts": {
                    "portfolio_analytics_rows": 6,
                    "portfolio_position_rows": 18,
                    "portfolio_optimizer_rows": 18,
                },
                "context": {"portfolio_max_after": dt.date(2026, 1, 7)},
            }

    runtime = PortfolioRuntime()
    graph = build_pipeline_graph(runtime)
    final_state = graph.invoke(_state())

    assert runtime.portfolio_calls == 1
    assert final_state["stage_results"]["portfolio_analytics"]["status"] == "OK"
    assert final_state["report"]["counts"]["portfolio_analytics_rows"] == 6


def test_cli_honors_skip_ingest_env(monkeypatch):
    captured = {}

    def _fake_run_pipeline(args):
        captured["args"] = args
        return 0, {"terminal_status": "success"}

    monkeypatch.setenv("SC_IDX_PIPELINE_SKIP_INGEST", "1")
    monkeypatch.setattr(pipeline_cli, "run_pipeline", _fake_run_pipeline)

    exit_code = pipeline_cli.main([])

    assert exit_code == 0
    assert captured["args"].skip_ingest is True


def test_determine_target_dates_uses_defaults_for_blank_budget_env(monkeypatch, tmp_path):
    trading_days = [dt.date(2026, 3, 25), dt.date(2026, 3, 26)]
    provider = SimpleNamespace(fetch_api_usage=lambda: {"current_usage": 1, "plan_limit": 8})
    trading_days_module = SimpleNamespace(update_trading_days_with_retry=lambda **kwargs: (True, None))
    fake_run_daily = SimpleNamespace(
        PROBE_SYMBOL_ENV="SC_IDX_PROBE_SYMBOL",
        DAILY_LIMIT_ENV="SC_IDX_MARKET_DATA_DAILY_LIMIT",
        DAILY_BUFFER_ENV="SC_IDX_MARKET_DATA_DAILY_BUFFER",
        BUFFER_ENV="SC_IDX_MARKET_DATA_CREDIT_BUFFER",
        DEFAULT_DAILY_LIMIT=800,
        DEFAULT_BUFFER=25,
        TRADING_DAYS_RETRY_ENV="SC_IDX_TRADING_DAYS_RETRY_ATTEMPTS",
        TRADING_DAYS_RETRY_BASE_ENV="SC_IDX_TRADING_DAYS_RETRY_BASE_SEC",
        _load_provider_module=lambda: provider,
        _load_trading_days_module=lambda: trading_days_module,
        _compute_daily_budget=lambda daily_limit, daily_buffer, calls_used_today: (
            daily_limit - calls_used_today,
            max(0, daily_limit - calls_used_today - daily_buffer),
        ),
        _resolve_end_date=lambda provider, probe_symbol, today_utc: (
            dt.date(2026, 3, 26),
            trading_days,
            dt.date(2026, 3, 26),
        ),
        derive_expected_target_date=lambda provider_latest, today_utc, trading_days, allow_weekday_fallback: (
            dt.date(2026, 3, 26),
            "provider_guard",
            trading_days,
            [],
        ),
        select_next_missing_trading_day=lambda trading_days, max_canon_trade_date=None: dt.date(2026, 3, 26),
        compute_eligible_end_date=lambda provider_latest, today_utc, trading_days: dt.date(2026, 3, 26),
    )
    runtime = SCIdxPipelineRuntime(
        report_dir=tmp_path,
        telemetry_dir=tmp_path / "telemetry",
        state_store=_FakeStore(),
    )

    monkeypatch.setattr(runtime, "_load_run_daily_module", lambda: fake_run_daily)
    monkeypatch.setattr("index_engine.orchestration.fetch_calls_used_today", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr("index_engine.orchestration.engine_db.fetch_max_canon_trade_date", lambda: dt.date(2026, 3, 25))
    monkeypatch.setattr("index_engine.orchestration.db_index_calc.fetch_max_level_date", lambda: dt.date(2026, 3, 25))
    monkeypatch.setattr(
        "index_engine.orchestration.db_portfolio_analytics.fetch_portfolio_analytics_max_date",
        lambda: dt.date(2026, 3, 25),
    )
    monkeypatch.setenv("SC_IDX_MARKET_DATA_DAILY_LIMIT", "")
    monkeypatch.setenv("SC_IDX_MARKET_DATA_DAILY_BUFFER", "")
    monkeypatch.setenv("SC_IDX_MARKET_DATA_CREDIT_BUFFER", "")
    monkeypatch.setenv("SC_IDX_TRADING_DAYS_RETRY_ATTEMPTS", "")
    monkeypatch.setenv("SC_IDX_TRADING_DAYS_RETRY_BASE_SEC", "")

    result = runtime.determine_target_dates(_state(smoke=False))

    assert result["status"] == "OK"
    assert result["context"]["daily_limit"] == 800
    assert result["context"]["daily_buffer"] == 25
    assert result["context"]["max_provider_calls"] == 775


def test_calc_index_main_accepts_optional_argv():
    from tools.index_engine import calc_index

    params = inspect.signature(calc_index.main).parameters

    assert list(params) == ["argv"]
    assert params["argv"].default is None


def test_failed_alert_send_failure_does_not_mark_gate(monkeypatch, tmp_path):
    calls = {"marked": 0}

    def _fake_gate(*args, **kwargs):
        return {"should_send": True, "reason": "first_send", "state": {}, "detail_hash": "abc"}

    def _fake_mark(*args, **kwargs):
        calls["marked"] += 1

    def _fake_send(*args, **kwargs):
        return {
            "ok": False,
            "attempted": True,
            "ready": False,
            "delivery_state": "missing_env",
            "missing_env": ["MAIL_TO"],
        }

    monkeypatch.setattr("index_engine.orchestration.evaluate_alert_gate", _fake_gate)
    monkeypatch.setattr("index_engine.orchestration.mark_alert_sent", _fake_mark)
    monkeypatch.setattr("index_engine.orchestration.send_email_result", _fake_send)
    monkeypatch.setattr(
        "index_engine.orchestration.smtp_configuration_status",
        lambda: {"ready": False, "missing_env": ["MAIL_TO"], "delivery_state": "missing_env"},
    )

    runtime = SmokePipelineRuntime(smoke_scenario="failed", report_dir=tmp_path, state_store=_FakeStore())
    state = _state(
        smoke=False,
        terminal_status="failed",
        status_reason="calc_index",
        remediation="rerun calc",
        warnings=[],
        context={
            "ended_at": "2026-01-07T00:05:00+00:00",
            "levels_max_after": "2026-01-07",
            "stats_max_after": "2026-01-07",
            "portfolio_max_after": "2026-01-07",
            "candidate_end_date": "2026-01-07",
            "ready_end_date": "2026-01-07",
            "calc_start_date": "2026-01-07",
            "calc_end_date": "2026-01-07",
        },
        stage_results={
            "calc_index": {
                "stage": "calc_index",
                "status": "FAILED",
                "detail": "calc_failed",
                "counts": {},
                "warnings": [],
                "attempts": 1,
                "duration_sec": 1.2,
            }
        },
    )

    final_state = _decide_alerts(state, runtime)

    assert final_state["alert"]["decision"] == "send_failed"
    assert final_state["alert"]["email_sent"] is False
    assert calls["marked"] == 0
    assert final_state["report"]["alert_decision"]["decision"] == "send_failed"


def test_failed_alert_suppressed_when_already_sent(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "index_engine.orchestration.evaluate_alert_gate",
        lambda *args, **kwargs: {
            "should_send": False,
            "reason": "already_sent_today",
            "state": {"last_sent_utc_date": "2026-01-07"},
            "detail_hash": "abc",
        },
    )
    monkeypatch.setattr(
        "index_engine.orchestration.smtp_configuration_status",
        lambda: {"ready": True, "missing_env": [], "delivery_state": "not_attempted"},
    )

    runtime = SmokePipelineRuntime(smoke_scenario="failed", report_dir=tmp_path, state_store=_FakeStore())
    state = _state(
        smoke=False,
        terminal_status="failed",
        status_reason="calc_index",
        context={"ended_at": "2026-01-07T00:05:00+00:00"},
        stage_results={
            "calc_index": {
                "stage": "calc_index",
                "status": "FAILED",
                "detail": "calc_failed",
                "counts": {},
                "warnings": [],
                "attempts": 1,
                "duration_sec": 1.2,
            }
        },
    )

    final_state = _decide_alerts(state, runtime)

    assert final_state["alert"]["decision"] == "suppressed"
    assert final_state["alert"]["deduplicated"] is True
    assert final_state["report"]["alert_decision"]["gate"]["reason"] == "already_sent_today"


def test_blocked_alert_sends_and_marks_gate(monkeypatch, tmp_path):
    calls = {"marked": 0, "sent": 0}

    monkeypatch.setattr(
        "index_engine.orchestration.evaluate_alert_gate",
        lambda *args, **kwargs: {
            "should_send": True,
            "reason": "first_send",
            "state": {},
            "detail_hash": "abc",
        },
    )

    def _fake_mark(*args, **kwargs):
        calls["marked"] += 1

    def _fake_send(*args, **kwargs):
        calls["sent"] += 1
        return {
            "ok": True,
            "attempted": True,
            "ready": True,
            "delivery_state": "sent",
            "message_id": "<msg-123@sustainacore.org>",
            "mail_to_count": 2,
            "missing_env": [],
        }

    monkeypatch.setattr("index_engine.orchestration.mark_alert_sent", _fake_mark)
    monkeypatch.setattr("index_engine.orchestration.send_email_result", _fake_send)
    monkeypatch.setattr(
        "index_engine.orchestration.smtp_configuration_status",
        lambda: {"ready": True, "missing_env": [], "delivery_state": "not_attempted"},
    )

    runtime = SmokePipelineRuntime(smoke_scenario="blocked", report_dir=tmp_path, state_store=_FakeStore())
    state = _state(
        smoke=False,
        terminal_status="blocked",
        status_reason="acquire_lock",
        context={"ended_at": "2026-01-07T00:05:00+00:00"},
        stage_results={
            "acquire_lock": {
                "stage": "acquire_lock",
                "status": "BLOCKED",
                "detail": "lock_busy",
                "counts": {},
                "warnings": [],
                "attempts": 1,
                "duration_sec": 0.2,
            }
        },
    )

    final_state = _decide_alerts(state, runtime)

    assert calls["sent"] == 1
    assert calls["marked"] == 1
    assert final_state["alert"]["decision"] == "sent"
    assert final_state["alert"]["email_sent"] is True
    assert final_state["report"]["alert_decision"]["delivery"]["message_id"] == "<msg-123@sustainacore.org>"


def test_degraded_alert_requires_opt_in(monkeypatch, tmp_path):
    calls = {"sent": 0}

    monkeypatch.delenv("SC_IDX_EMAIL_ON_DEGRADED", raising=False)
    monkeypatch.setattr(
        "index_engine.orchestration.send_email_result",
        lambda *args, **kwargs: calls.__setitem__("sent", calls["sent"] + 1),
    )

    runtime = SmokePipelineRuntime(smoke_scenario="degraded", report_dir=tmp_path, state_store=_FakeStore())
    state = _state(
        smoke=False,
        terminal_status="success_with_degradation",
        status_reason="imputation_or_replacement",
        context={"ended_at": "2026-01-07T00:05:00+00:00"},
        stage_results={
            "imputation_or_replacement": {
                "stage": "imputation_or_replacement",
                "status": "DEGRADED",
                "detail": "imputation_used",
                "counts": {"total_imputed": 2},
                "warnings": ["imputed_rows"],
                "attempts": 1,
                "duration_sec": 0.4,
            }
        },
    )

    final_state = _decide_alerts(state, runtime)

    assert calls["sent"] == 0
    assert final_state["alert"]["decision"] == "skipped"


def test_stale_clean_skip_sends_alert(monkeypatch, tmp_path):
    calls = {"marked": 0, "sent": 0}

    monkeypatch.setattr(
        "index_engine.orchestration.evaluate_alert_gate",
        lambda *args, **kwargs: {
            "should_send": True,
            "reason": "first_send",
            "state": {},
            "detail_hash": "abc",
        },
    )
    monkeypatch.setattr(
        "index_engine.orchestration._fetch_recent_terminal_history",
        lambda limit=5: ["clean_skip"],
    )
    monkeypatch.setattr(
        "index_engine.orchestration.mark_alert_sent",
        lambda *args, **kwargs: calls.__setitem__("marked", calls["marked"] + 1),
    )
    monkeypatch.setattr(
        "index_engine.orchestration.send_email_result",
        lambda *args, **kwargs: {
            "ok": True,
            "attempted": True,
            "ready": True,
            "delivery_state": "sent",
            "message_id": "<msg-stale@sustainacore.org>",
            "mail_to_count": 1,
            "missing_env": [],
        },
    )
    monkeypatch.setattr(
        "index_engine.orchestration.smtp_configuration_status",
        lambda: {"ready": True, "missing_env": [], "delivery_state": "not_attempted"},
    )

    runtime = SmokePipelineRuntime(smoke_scenario="clean_skip", report_dir=tmp_path, state_store=_FakeStore())
    state = _state(
        smoke=False,
        terminal_status="clean_skip",
        status_reason="up_to_date",
        context={
            "ended_at": "2026-01-08T00:05:00+00:00",
            "expected_target_date": "2026-01-08",
            "expected_target_source": "provider_guard_estimate",
            "max_level_before": "2026-01-07",
            "stats_max_after": "2026-01-07",
            "portfolio_max_after": "2026-01-07",
            "portfolio_position_max_after": "2026-01-07",
        },
        stage_results={
            "determine_target_dates": {
                "stage": "determine_target_dates",
                "status": "SKIP",
                "detail": "up_to_date",
                "counts": {},
                "warnings": [],
                "attempts": 1,
                "duration_sec": 0.1,
            }
        },
    )

    final_state = _decide_alerts(state, runtime)

    assert calls["marked"] == 1
    assert final_state["alert"]["alert_name"] == "sc_idx_pipeline_stale"
    assert final_state["alert"]["decision"] == "sent"
    assert final_state["alert"]["email_sent"] is True
    assert final_state["report"]["overall_health"] == "Stale"
    assert final_state["report"]["alert_decision"]["trigger_reason"] == "latest_complete_lagging_expected"


def test_repeated_degraded_sends_alert_without_opt_in(monkeypatch, tmp_path):
    calls = {"marked": 0}

    monkeypatch.delenv("SC_IDX_EMAIL_ON_DEGRADED", raising=False)
    monkeypatch.setattr(
        "index_engine.orchestration.evaluate_alert_gate",
        lambda *args, **kwargs: {
            "should_send": True,
            "reason": "first_send",
            "state": {},
            "detail_hash": "abc",
        },
    )
    monkeypatch.setattr(
        "index_engine.orchestration._fetch_recent_terminal_history",
        lambda limit=5: ["success_with_degradation"],
    )
    monkeypatch.setattr(
        "index_engine.orchestration.mark_alert_sent",
        lambda *args, **kwargs: calls.__setitem__("marked", calls["marked"] + 1),
    )
    monkeypatch.setattr(
        "index_engine.orchestration.send_email_result",
        lambda *args, **kwargs: {
            "ok": True,
            "attempted": True,
            "ready": True,
            "delivery_state": "sent",
            "message_id": "<msg-repeat@sustainacore.org>",
            "mail_to_count": 1,
            "missing_env": [],
        },
    )
    monkeypatch.setattr(
        "index_engine.orchestration.smtp_configuration_status",
        lambda: {"ready": True, "missing_env": [], "delivery_state": "not_attempted"},
    )

    runtime = SmokePipelineRuntime(smoke_scenario="degraded", report_dir=tmp_path, state_store=_FakeStore())
    state = _state(
        smoke=False,
        terminal_status="success_with_degradation",
        status_reason="imputation_or_replacement",
        context={
            "ended_at": "2026-01-07T00:05:00+00:00",
            "expected_target_date": "2026-01-07",
            "expected_target_source": "readiness_probe",
            "levels_max_after": "2026-01-07",
            "stats_max_after": "2026-01-07",
            "portfolio_max_after": "2026-01-07",
            "portfolio_position_max_after": "2026-01-07",
        },
        stage_results={
            "imputation_or_replacement": {
                "stage": "imputation_or_replacement",
                "status": "DEGRADED",
                "detail": "imputation_used",
                "counts": {"total_imputed": 2},
                "warnings": ["imputed_rows"],
                "attempts": 1,
                "duration_sec": 0.4,
            }
        },
    )

    final_state = _decide_alerts(state, runtime)

    assert calls["marked"] == 1
    assert final_state["alert"]["alert_name"] == "sc_idx_pipeline_repeated_degraded"
    assert final_state["alert"]["trigger_reason"] == "repeated_degraded:2"
    assert final_state["alert"]["decision"] == "sent"


def test_clean_skip_alerts_are_not_sent(monkeypatch, tmp_path):
    calls = {"sent": 0}

    monkeypatch.setattr(
        "index_engine.orchestration.send_email_result",
        lambda *args, **kwargs: calls.__setitem__("sent", calls["sent"] + 1),
    )

    runtime = SmokePipelineRuntime(smoke_scenario="clean_skip", report_dir=tmp_path, state_store=_FakeStore())
    state = _state(
        smoke=False,
        terminal_status="clean_skip",
        status_reason="provider_not_ready",
        context={"ended_at": "2026-01-07T00:05:00+00:00"},
        stage_results={
            "readiness_probe": {
                "stage": "readiness_probe",
                "status": "SKIP",
                "detail": "provider_not_ready",
                "counts": {},
                "warnings": [],
                "attempts": 1,
                "duration_sec": 0.1,
            }
        },
    )

    final_state = _decide_alerts(state, runtime)

    assert calls["sent"] == 0
    assert final_state["alert"]["decision"] == "skipped"

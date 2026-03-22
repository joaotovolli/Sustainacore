import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.orchestration import (
    PipelineArgs,
    PipelineGraphState,
    SmokePipelineRuntime,
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

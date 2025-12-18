import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.index_engine.run_pipeline as pipeline


def test_run_stage_handles_noarg_main(monkeypatch):
    original_argv = sys.argv[:]
    seen = {}

    def noarg_main():
        seen["argv"] = sys.argv[:]
        return 0

    pipeline._run_stage("noarg", noarg_main, ["--foo", "bar"])
    assert seen["argv"][1:] == ["--foo", "bar"]
    assert sys.argv == original_argv


def test_run_stage_handles_argv_main():
    captured = []

    def argv_main(argv):
        captured.extend(argv)
        return 0

    pipeline._run_stage("withargv", argv_main, ["a", "b"])
    assert captured == ["a", "b"]


def test_pipeline_skip_ingest(monkeypatch):
    calls = []

    def _record(name):
        def _impl(args=None):
            calls.append(name)
            return 0

        return _impl

    monkeypatch.setenv("SC_IDX_PIPELINE_SKIP_INGEST", "1")
    monkeypatch.setattr(pipeline, "load_default_env", lambda: None)
    # Use real _run_stage to exercise branching.
    class _FakeIngest:
        def main(self, argv=None):
            calls.append("ingest_should_skip")
            return 0

    class _FakeNoArg:
        def __init__(self, name):
            self.main = _record(name)

    monkeypatch.setitem(sys.modules, "tools.index_engine.run_daily", _FakeIngest())
    monkeypatch.setitem(sys.modules, "tools.index_engine.check_price_completeness", _FakeNoArg("completeness"))
    monkeypatch.setitem(sys.modules, "tools.index_engine.impute_missing_prices", _FakeNoArg("impute"))
    monkeypatch.setitem(sys.modules, "tools.index_engine.calc_index", _FakeNoArg("index_calc"))

    pipeline.main()
    assert "ingest_should_skip" not in calls
    assert calls == ["completeness", "impute", "index_calc"]

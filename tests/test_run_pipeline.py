import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.index_engine.run_pipeline as pipeline


def test_run_pipeline_order(monkeypatch):
    calls = []

    def _stage(name):
        def _impl(args):
            calls.append(name)
            return 0

        return _impl

    monkeypatch.setattr(pipeline, "load_default_env", lambda: None)
    monkeypatch.setattr(pipeline, "_run_stage", lambda n, f, a: f(a))
    monkeypatch.setattr(pipeline, "sys", pipeline.sys)

    class _FakeMod:
        def __init__(self, name):
            self.main = _stage(name)

    monkeypatch.setitem(sys.modules, "tools.index_engine.run_daily", _FakeMod("ingest"))
    monkeypatch.setitem(sys.modules, "tools.index_engine.check_price_completeness", _FakeMod("completeness"))
    monkeypatch.setitem(sys.modules, "tools.index_engine.impute_missing_prices", _FakeMod("impute"))
    monkeypatch.setitem(sys.modules, "tools.index_engine.calc_index", _FakeMod("index_calc"))

    pipeline.main()
    assert calls == ["ingest", "completeness", "impute", "index_calc"]

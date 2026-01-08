import datetime as dt
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import tools.index_engine.run_pipeline as pipeline


class _Record:
    def __init__(self, status: str):
        now = dt.datetime(2026, 1, 7, tzinfo=dt.timezone.utc)
        self.status = status
        self.started_at = now
        self.ended_at = now
        self.details = None


class _FakeStore:
    def __init__(self, *, resume_run_id=None, stage_records=None):
        self.run_id = "run-1"
        self.resume_run_id = resume_run_id
        self.stage_records = stage_records or {}
        self.started = {}

    def create_run_id(self):
        return self.run_id

    def fetch_resume_run_id(self, run_date=None):
        return self.resume_run_id

    def fetch_stage_statuses(self, run_id):
        return self.stage_records

    def record_stage_start(self, run_id, stage_name, details=None):
        self.started[stage_name] = details

    def record_stage_end(self, run_id, stage_name, status, details=None):
        self.stage_records[stage_name] = _Record(status)


def _patch_health(monkeypatch):
    monkeypatch.setattr(pipeline, "collect_health_snapshot", lambda stage_durations, last_error: {})
    monkeypatch.setattr(pipeline, "format_health_summary", lambda health: "summary")
    monkeypatch.setattr(pipeline, "write_health_artifact", lambda health: None)
    monkeypatch.setattr(pipeline, "start_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "finish_run", lambda *args, **kwargs: None)


def test_invoke_stage_handles_noarg_main(monkeypatch):
    original_argv = sys.argv[:]
    seen = {}

    def noarg_main():
        seen["argv"] = sys.argv[:]
        return 0

    code, detail = pipeline._invoke_stage("noarg", noarg_main, ["--foo", "bar"])
    assert code == 0
    assert detail is None
    assert seen["argv"][1:] == ["--foo", "bar"]
    assert sys.argv == original_argv


def test_invoke_stage_handles_argv_main():
    captured = []

    def argv_main(argv):
        captured.extend(argv)
        return 0

    code, _detail = pipeline._invoke_stage("withargv", argv_main, ["a", "b"])
    assert code == 0
    assert captured == ["a", "b"]


def test_pipeline_skip_ingest(monkeypatch):
    calls = []
    store = _FakeStore()
    _patch_health(monkeypatch)

    monkeypatch.setenv("SC_IDX_PIPELINE_SKIP_INGEST", "1")
    monkeypatch.setattr(pipeline, "PipelineStateStore", lambda pipeline_name=None: store)

    update_mod = types.ModuleType("tools.index_engine.update_trading_days")
    update_mod.update_trading_days = lambda auto_extend=False: calls.append("update_trading_days")
    monkeypatch.setitem(sys.modules, "tools.index_engine.update_trading_days", update_mod)

    run_daily_mod = types.ModuleType("tools.index_engine.run_daily")
    run_daily_mod.main = lambda argv=None: calls.append("ingest") or 0
    monkeypatch.setitem(sys.modules, "tools.index_engine.run_daily", run_daily_mod)

    check_mod = types.ModuleType("tools.index_engine.check_price_completeness")
    check_mod.run_check = lambda **kwargs: calls.append("completeness") or {}
    monkeypatch.setitem(sys.modules, "tools.index_engine.check_price_completeness", check_mod)

    ingest_mod = types.ModuleType("tools.index_engine.ingest_prices")
    ingest_mod._run_backfill_missing = lambda args: calls.append("backfill_missing")
    monkeypatch.setitem(sys.modules, "tools.index_engine.ingest_prices", ingest_mod)

    calc_mod = types.ModuleType("tools.index_engine.calc_index")
    calc_mod.main = lambda argv=None: calls.append("calc_index") or 0
    monkeypatch.setitem(sys.modules, "tools.index_engine.calc_index", calc_mod)

    impute_mod = types.ModuleType("tools.index_engine.impute_missing_prices")
    impute_mod.main = lambda argv=None: calls.append("impute") or 0
    monkeypatch.setitem(sys.modules, "tools.index_engine.impute_missing_prices", impute_mod)

    target_day = dt.date(2026, 1, 7)
    level_state = {"value": dt.date(2026, 1, 6)}

    monkeypatch.setattr(pipeline.engine_db, "fetch_latest_trading_day", lambda: target_day)
    monkeypatch.setattr(pipeline.engine_db, "fetch_trading_days", lambda start, end: [level_state["value"], target_day])
    monkeypatch.setattr(pipeline.engine_db, "fetch_max_canon_trade_date", lambda: target_day)
    monkeypatch.setattr(pipeline.engine_db, "fetch_missing_real_for_trade_date", lambda date: [])
    monkeypatch.setattr(pipeline.engine_db, "fetch_max_imputation_date", lambda: None)

    def _fetch_max_level_date():
        return level_state["value"]

    def _calc_main(argv=None):
        calls.append("calc_index")
        level_state["value"] = target_day
        return 0

    calc_mod.main = _calc_main
    monkeypatch.setitem(sys.modules, "tools.index_engine.calc_index", calc_mod)
    monkeypatch.setattr(pipeline.db_index_calc, "fetch_max_level_date", _fetch_max_level_date)

    exit_code = pipeline.main([])
    assert exit_code == 0
    assert "ingest" not in calls
    assert calls == ["update_trading_days", "completeness", "calc_index", "impute"]


def test_pipeline_advances_next_missing_day(monkeypatch):
    calls = []
    store = _FakeStore()
    _patch_health(monkeypatch)

    monkeypatch.setattr(pipeline, "PipelineStateStore", lambda pipeline_name=None: store)

    update_mod = types.ModuleType("tools.index_engine.update_trading_days")
    update_mod.update_trading_days = lambda auto_extend=False: None
    monkeypatch.setitem(sys.modules, "tools.index_engine.update_trading_days", update_mod)

    run_daily_mod = types.ModuleType("tools.index_engine.run_daily")
    run_daily_mod.main = lambda argv=None: 0
    monkeypatch.setitem(sys.modules, "tools.index_engine.run_daily", run_daily_mod)

    check_mod = types.ModuleType("tools.index_engine.check_price_completeness")
    check_mod.run_check = lambda **kwargs: None
    monkeypatch.setitem(sys.modules, "tools.index_engine.check_price_completeness", check_mod)

    ingest_mod = types.ModuleType("tools.index_engine.ingest_prices")
    ingest_mod._run_backfill_missing = lambda args: None
    monkeypatch.setitem(sys.modules, "tools.index_engine.ingest_prices", ingest_mod)

    calc_args = {}
    calc_mod = types.ModuleType("tools.index_engine.calc_index")

    def _calc_main(argv=None):
        calc_args["argv"] = list(argv or [])
        return 0

    calc_mod.main = _calc_main
    monkeypatch.setitem(sys.modules, "tools.index_engine.calc_index", calc_mod)

    impute_mod = types.ModuleType("tools.index_engine.impute_missing_prices")
    impute_mod.main = lambda argv=None: 0
    monkeypatch.setitem(sys.modules, "tools.index_engine.impute_missing_prices", impute_mod)

    level_date = dt.date(2026, 1, 6)
    target_day = dt.date(2026, 1, 7)

    monkeypatch.setattr(pipeline.engine_db, "fetch_latest_trading_day", lambda: target_day)
    monkeypatch.setattr(pipeline.engine_db, "fetch_trading_days", lambda start, end: [level_date, target_day])
    monkeypatch.setattr(pipeline.engine_db, "fetch_max_canon_trade_date", lambda: target_day)
    monkeypatch.setattr(pipeline.engine_db, "fetch_missing_real_for_trade_date", lambda date: [])
    monkeypatch.setattr(pipeline.engine_db, "fetch_max_imputation_date", lambda: None)

    level_state = {"value": level_date}

    def _fetch_max_level_date():
        return level_state["value"]

    def _calc_main_with_update(argv=None):
        level_state["value"] = target_day
        calc_args["argv"] = list(argv or [])
        return 0

    calc_mod.main = _calc_main_with_update
    monkeypatch.setitem(sys.modules, "tools.index_engine.calc_index", calc_mod)
    monkeypatch.setattr(pipeline.db_index_calc, "fetch_max_level_date", _fetch_max_level_date)

    exit_code = pipeline.main([])
    assert exit_code == 0
    assert calc_args["argv"][:4] == ["--start", "2026-01-07", "--end", "2026-01-07"]


def test_pipeline_resume_skips_ok_stages(monkeypatch):
    calls = []
    stage_records = {
        "update_trading_days": _Record("OK"),
        "ingest_prices": _Record("OK"),
    }
    store = _FakeStore(resume_run_id="run-1", stage_records=stage_records)
    _patch_health(monkeypatch)

    monkeypatch.setattr(pipeline, "PipelineStateStore", lambda pipeline_name=None: store)

    update_mod = types.ModuleType("tools.index_engine.update_trading_days")
    update_mod.update_trading_days = lambda auto_extend=False: calls.append("update_trading_days")
    monkeypatch.setitem(sys.modules, "tools.index_engine.update_trading_days", update_mod)

    run_daily_mod = types.ModuleType("tools.index_engine.run_daily")
    run_daily_mod.main = lambda argv=None: calls.append("ingest") or 0
    monkeypatch.setitem(sys.modules, "tools.index_engine.run_daily", run_daily_mod)

    check_mod = types.ModuleType("tools.index_engine.check_price_completeness")
    check_mod.run_check = lambda **kwargs: calls.append("completeness") or None
    monkeypatch.setitem(sys.modules, "tools.index_engine.check_price_completeness", check_mod)

    ingest_mod = types.ModuleType("tools.index_engine.ingest_prices")
    ingest_mod._run_backfill_missing = lambda args: None
    monkeypatch.setitem(sys.modules, "tools.index_engine.ingest_prices", ingest_mod)

    calc_mod = types.ModuleType("tools.index_engine.calc_index")
    impute_mod = types.ModuleType("tools.index_engine.impute_missing_prices")
    impute_mod.main = lambda argv=None: calls.append("impute") or 0
    monkeypatch.setitem(sys.modules, "tools.index_engine.impute_missing_prices", impute_mod)

    level_state = {"value": dt.date(2026, 1, 6)}
    target_day = dt.date(2026, 1, 7)

    monkeypatch.setattr(pipeline.engine_db, "fetch_latest_trading_day", lambda: target_day)
    monkeypatch.setattr(pipeline.engine_db, "fetch_trading_days", lambda start, end: [level_state["value"], target_day])
    monkeypatch.setattr(pipeline.engine_db, "fetch_max_canon_trade_date", lambda: target_day)
    monkeypatch.setattr(pipeline.engine_db, "fetch_missing_real_for_trade_date", lambda date: [])
    monkeypatch.setattr(pipeline.engine_db, "fetch_max_imputation_date", lambda: None)

    def _fetch_max_level_date():
        return level_state["value"]

    def _calc_main(argv=None):
        calls.append("calc_index")
        level_state["value"] = target_day
        return 0

    calc_mod.main = _calc_main
    monkeypatch.setitem(sys.modules, "tools.index_engine.calc_index", calc_mod)
    monkeypatch.setattr(pipeline.db_index_calc, "fetch_max_level_date", _fetch_max_level_date)

    exit_code = pipeline.main([])
    assert exit_code == 0
    assert "update_trading_days" not in calls
    assert "ingest" not in calls
    assert "completeness" in calls

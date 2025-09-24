from importlib import util
from pathlib import Path
import sys
import types

_repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo_root))


def _load_module():
    names = ["app", "app.rag", "app.rag.gemini_cli"]
    original = {name: sys.modules.get(name) for name in names}
    try:
        app_pkg = types.ModuleType("app")
        app_pkg.__path__ = [str(_repo_root / "app")]
        sys.modules["app"] = app_pkg

        rag_pkg = types.ModuleType("app.rag")
        rag_pkg.__path__ = [str(_repo_root / "app" / "rag")]
        sys.modules["app.rag"] = rag_pkg

        gemini_cli_spec = util.spec_from_file_location(
            "app.rag.gemini_cli", _repo_root / "app" / "rag" / "gemini_cli.py"
        )
        assert gemini_cli_spec and gemini_cli_spec.loader
        gemini_cli_module = util.module_from_spec(gemini_cli_spec)
        sys.modules["app.rag.gemini_cli"] = gemini_cli_module
        gemini_cli_spec.loader.exec_module(gemini_cli_module)

        module_path = _repo_root / "app" / "retrieval" / "gemini_gateway.py"
        spec = util.spec_from_file_location("app.retrieval.gemini_gateway", module_path)
        assert spec and spec.loader
        module = util.module_from_spec(spec)
        sys.modules["app.retrieval.gemini_gateway"] = module
        spec.loader.exec_module(module)
        return module
    finally:
        for name, saved in original.items():
            if saved is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = saved


_module = _load_module()
_clean_answer_text = getattr(_module, "_clean_answer_text")
_dedup_sources = getattr(_module, "_dedup_sources")


def test_clean_answer_text_removes_debug_blocks():
    raw = """Here’s the best supported answer from the retrieved sources.

Why this answer:
- Source 1: Something

Main insight with detail [CITE_ID].

Sources:
- Source 1
- Source 2
"""

    cleaned = _clean_answer_text(raw)

    assert "Here’s the best supported answer" not in cleaned
    assert "Why this answer" not in cleaned
    assert cleaned.startswith("Main insight")


def test_clean_answer_text_preserves_content():
    raw = "Insightful line [FCA].\n\nSecond line."  # no debug
    cleaned = _clean_answer_text(raw)

    assert cleaned == raw


def test_dedup_sources_keeps_last_occurrence():
    sources = [
        "First Title — Publisher (2024)",
        "Duplicate — Org",  # should be dropped in favour of the later entry
        "Second Title — Publisher (2023)",
        "duplicate — org",  # same as above but later and lower case
    ]

    deduped = _dedup_sources(sources, limit=10)

    assert "First Title — Publisher (2024)" in deduped
    assert "Second Title — Publisher (2023)" in deduped
    assert "duplicate — org" in deduped
    assert "Duplicate — Org" not in deduped
    assert deduped.index("duplicate — org") > deduped.index("Second Title — Publisher (2023)")


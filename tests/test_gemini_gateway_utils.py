import io
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
_build_sources_from_facts = getattr(_module, "_build_sources_from_facts")
GeminiGateway = getattr(_module, "GeminiGateway")


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


def test_clean_answer_text_returns_empty_when_only_debug_sections():
    raw = "\n".join(
        [
            "Here’s the best supported answer from the retrieved sources.",
            "Why this answer:",
            "- Source 1: Detail",
            "Sources:",
            "- Source 1",
            "- Source 2",
        ]
    )

    cleaned = _clean_answer_text(raw)

    assert cleaned == ""


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


def test_build_sources_from_facts_prefers_cited_and_latest_entries():
    answer = "The TECH100 tracks AI leaders [TECH100_METHOD]. Microsoft remains a member [MSFT_PROFILE]."
    facts = [
        {
            "citation_id": "MSFT_PROFILE",
            "title": "Microsoft Corporation",
            "source_name": "TECH100",
            "date": "2024-03-01",
            "url": "https://example.com/microsoft?utm_source=newsletter",
        },
        {
            "citation_id": "TECH100_METHOD",
            "title": "TECH100 Methodology",
            "source_name": "SustainaCore",
            "date": "2024-01-15",
            "url": "https://example.com/methodology",
        },
        {
            "citation_id": "MSFT_PROFILE",
            "title": "Microsoft Corporation (Updated)",
            "source_name": "SustainaCore",
            "date": "2024-04-01",
            "url": "https://example.com/microsoft",
        },
    ]

    sources = _build_sources_from_facts(answer, facts, limit=6)

    assert sources[0] == "TECH100 Methodology — SustainaCore (2024-01-15)"
    assert sources[1] == "Microsoft Corporation (Updated) — SustainaCore (2024-04-01)"
    assert len(sources) == 2


def test_build_sources_from_facts_falls_back_to_uncited_facts():
    answer = "No citations in this answer."
    facts = [
        {"citation_id": "FACT_A", "title": "Fact A", "source_name": "Org", "date": "2024", "url": "https://a"},
        {"citation_id": "FACT_B", "title": "Fact B", "source_name": "Org", "date": "2023", "url": "https://b"},
    ]

    sources = _build_sources_from_facts(answer, facts, limit=3)

    assert sources == ["Fact A — Org (2024)", "Fact B — Org (2023)"]


def test_compose_answer_parses_code_block_json(monkeypatch):
    gateway = GeminiGateway()

    code_block = """```json\n{\n  \"answer\": \"Yes, Microsoft is in the TECH100 Index.\",\n  \"sources\": [\"Source A\", \"Source B\"]\n}\n```"""

    def _fake_call_json(_prompt, *, model):  # pylint: disable=unused-argument
        return {
            "candidates": [
                {
                    "content": {"parts": [{"text": code_block}], "role": "model"},
                    "finishReason": "STOP",
                    "index": 0,
                }
            ],
            "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 5},
        }

    gateway._call_json = _fake_call_json  # type: ignore[attr-defined]
    monkeypatch.setenv("ASK2_SYNTH_FALLBACK", "0")
    monkeypatch.setattr(_module, "open", lambda *_args, **_kwargs: io.StringIO(), raising=False)

    retriever_payload = {"facts": [], "context_note": ""}
    plan = {"filters": {}, "query_variants": [], "k": 4, "raw": {}}

    result = gateway.compose_answer("Is MSFT in TECH100?", retriever_payload, plan, hop_count=1)

    assert result["answer"] == "Yes, Microsoft is in the TECH100 Index."
    assert result["sources"] == ["Source A", "Source B"]


def test_compose_answer_synth_fallback(monkeypatch):
    gateway = GeminiGateway()

    gateway._call_json = lambda *_args, **_kwargs: {}  # type: ignore[attr-defined]
    monkeypatch.setenv("ASK2_SYNTH_FALLBACK", "1")
    monkeypatch.setattr(_module, "open", lambda *_args, **_kwargs: io.StringIO(), raising=False)

    facts = [
        {
            "title": "Doc A",
            "snippet": "Doc A summary goes here with plenty of detail.",
            "source_name": "Sustainacore",
        },
        {
            "title": "Doc B",
            "snippet": "Doc B insight to include.",
            "source_name": "Sustainacore",
        },
    ]

    retriever_payload = {"facts": facts, "context_note": ""}
    plan = {"filters": {}, "query_variants": [], "k": 4, "raw": {}}

    result = gateway.compose_answer("Tell me about Doc A", retriever_payload, plan, hop_count=1)

    assert result["answer"].startswith("Here’s a brief summary from SustainaCore sources")
    assert "Doc A" in result["answer"]

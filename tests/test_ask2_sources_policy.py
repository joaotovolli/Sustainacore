import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.rag import routing  # noqa: E402


def _vector_hits():
    return [
        {
            "title": "Tech100 Rank",
            "url": "https://example.com/rank",
            "snippet": "Microsoft ranks first in TECH100.",
            "score": 0.86,
        },
        {
            "title": "TECH100 Methodology",
            "url": "https://example.com/methodology",
            "snippet": "The index balances ESG pillars.",
            "score": 0.82,
        },
    ]


def test_route_strips_inline_sources():
    def _vector_fn(_query, _k):
        return _vector_hits()

    def _gemini_fn(_prompt, *_args, **_kwargs):
        return """Microsoft is in the TECH100 index.\nWhy this answer:\n- Matched to TECH100 documentation\nSources:\n- Source 1: TECH100 Rank"""

    result = routing.route_ask2(
        "Is Microsoft in the TECH100 Index?",
        k=4,
        vector_fn=_vector_fn,
        gemini_fn=_gemini_fn,
    )

    answer = result["answer"]
    assert "Why this answer" not in answer
    assert "Sources:" not in answer
    assert result["sources"]
    assert result["meta"]["routing"] == "high_conf"
    assert result["meta"]["similarity_floor_mode"] == routing.SIMILARITY_FLOOR_MODE

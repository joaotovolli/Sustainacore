import json

from tools.research_generator.run_generator import _build_details


def test_generation_meta_in_details():
    bundle = {"report_type": "REBALANCE", "window": {"start": "2025-01-01", "end": "2025-01-02"}}
    draft = {"table_caption": "table", "chart_caption": "chart", "template_mode": True}
    meta = {
        "duration_seconds": 120,
        "profile_selected": "MEDIUM",
        "usage_before": {"available": False, "reason": "usage_unavailable"},
        "usage_after": {"available": False, "reason": "usage_unavailable"},
        "usage_delta": {"available": False, "reason": "usage_unavailable"},
        "stop_reason": "usage_unavailable",
    }
    details = _build_details(bundle, draft, "chart.png", generation_meta=meta)
    payload = json.loads(details)
    assert "generation_meta" in payload
    assert payload["generation_meta"]["profile_selected"] == "MEDIUM"


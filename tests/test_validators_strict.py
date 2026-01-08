from tools.research_generator.validators import quality_gate_strict


def test_docx_no_forbidden_phrases():
    bundle = {
        "docx_tables": [{"title": "Core vs Rest summary", "rows": [{"Metric": "A", "Core": 1, "Rest": 2}]}],
        "docx_charts": [{"caption": "Figure 1. Core vs Rest"}],
        "metric_pool": [{"name": "core.aiges.mean", "value": 1}] * 100,
        "profile": {"max_candidate_metrics": 1, "max_charts": 1, "max_tables": 1},
    }
    draft = {
        "paragraphs": ["Figure 1 shows what it shows. Table 1 lists core vs rest 1 2 3 4 5 6."],
    }
    ok, issues = quality_gate_strict(bundle, draft)
    assert not ok
    assert any("forbidden_phrase" in issue for issue in issues)


def test_core_vs_rest_required():
    bundle = {
        "docx_tables": [{"title": "Other table", "rows": [{"Metric": "A", "Core": 1, "Rest": 2}]}],
        "docx_charts": [{"caption": "Figure 1. Core vs Rest"}],
        "metric_pool": [{"name": "core.aiges.mean", "value": 1}] * 100,
        "profile": {"max_candidate_metrics": 1, "max_charts": 1, "max_tables": 1},
    }
    draft = {
        "paragraphs": ["Figure 1 shows results. Table 1 lists metrics 1 2 3 4 5 6 with core and rest."],
    }
    ok, issues = quality_gate_strict(bundle, draft)
    assert not ok
    assert "missing_core_vs_rest_table" in issues


def test_sanitizer_numeric_spacing_gate():
    bundle = {
        "docx_tables": [{"title": "Core vs Rest summary", "rows": [{"Metric": "A", "Core": 1, "Rest": 2}]}],
        "docx_charts": [{"caption": "Figure 1. Core vs Rest"}],
        "metric_pool": [{"name": "core.aiges.mean", "value": 1}] * 100,
        "profile": {"max_candidate_metrics": 1, "max_charts": 1, "max_tables": 1},
    }
    draft = {
        "paragraphs": ["Figure 1 shows 79. 84 vs 70. 12. Table 1 lists metrics 1 2 3 4 5 6 with core and rest."],
    }
    ok, issues = quality_gate_strict(bundle, draft)
    assert not ok
    assert "spaced_decimal" in issues

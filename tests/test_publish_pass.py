from tools.research_generator.publish_pass import linearize_draft


def test_linearize_strips_filler_and_inserts_markers():
    outline = [
        {"type": "paragraph", "text": "Figure 1 provides the detailed breakdown for this section."},
        {"type": "figure", "id": 1},
        {"type": "paragraph", "text": "Figure 1 shows core vs coverage."},
        {"type": "table", "id": 1},
        {"type": "paragraph", "text": "Table 1 highlights turnover."},
    ]
    charts = [{"caption": "Core vs coverage"}]
    tables = [{"title": "Summary"}]
    text = linearize_draft(
        headline="Test Headline",
        paragraphs=["One", "Two"],
        outline=outline,
        charts=charts,
        tables=tables,
    )
    assert "provides the detailed breakdown" not in text.lower()
    assert "[INSERT FIGURE 1" in text
    assert "[INSERT TABLE 1" in text


def test_pipeline_rewrite_loop_on_blocker():
    from tools.research_generator.agent_pipeline import _has_blocker

    issues = [
        {"severity": "WARN", "issue": "ok"},
        {"severity": "BLOCKER", "issue": "bad"},
    ]
    assert _has_blocker(issues)

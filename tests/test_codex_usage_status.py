from tools.research_generator.codex_usage import _parse_status_text


def test_parse_status_text_success():
    text = (
        "Context window: [#####-----] 39% left\n"
        "5h limit: [####------] 74% left (2h remaining)\n"
        "Weekly limit: [########--] 92% left (6d remaining)\n"
    )
    payload = _parse_status_text(text)
    assert payload["context_window"]["left_pct"] == 39
    assert payload["five_hour"]["remaining_pct"] == 74
    assert payload["five_hour"]["used_pct"] == 26
    assert payload["weekly"]["remaining_pct"] == 92
    assert payload["weekly"]["used_pct"] == 8


def test_parse_status_text_failure():
    assert _parse_status_text("no status here") is None

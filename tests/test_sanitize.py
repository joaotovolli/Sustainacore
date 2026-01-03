from tools.research_generator.sanitize import sanitize_text_blocks


def test_sanitizer_numeric_spacing():
    text = ["Value is 79. 84 and change +31. 33 in the sample."]
    cleaned = sanitize_text_blocks(text)
    assert "79.84" in cleaned[0]
    assert "+31.33" in cleaned[0]

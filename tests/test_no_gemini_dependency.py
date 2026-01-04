from pathlib import Path


def test_no_gemini_dependency():
    run_gen = Path("tools/research_generator/run_generator.py").read_text()
    assert "gemini_cli" not in run_gen

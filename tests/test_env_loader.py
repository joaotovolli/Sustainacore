import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from tools.index_engine.env_loader import load_env_file


def test_env_loader_parses_key_value_and_ignores_comments(tmp_path, monkeypatch):
    path = tmp_path / "env"
    path.write_text(
        "\n".join(
            [
                "# comment",
                "export FOO=bar",
                "BLANK=",
                "SPACED = nope",
                "QUOTED='hello'",
                "DOUBLE=\"world\"",
                "INVALIDLINE",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("QUOTED", raising=False)
    monkeypatch.setenv("DOUBLE", "already")

    load_env_file(path)

    assert os.environ["FOO"] == "bar"
    assert os.environ["QUOTED"] == "hello"
    assert os.environ["DOUBLE"] == "already"

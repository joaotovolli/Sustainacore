import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app


@pytest.mark.parametrize("filters", [None, {"docset": "demo"}])
def test_top_k_by_vector_filters_shim(monkeypatch, filters):
    helper = getattr(app, "_top_k_by_vector", None)
    if not callable(helper):
        pytest.skip("vector helper unavailable")

    sentinel = object()
    captured = {}

    def stub(vec, k, *args):
        captured["vec"] = vec
        captured["k"] = k
        captured["args"] = args
        return [sentinel]

    monkeypatch.setattr(app, "_orig_tkbv", stub, raising=False)

    kwargs = {}
    if filters is not None:
        kwargs["filters"] = filters

    result = app._top_k_by_vector(b"vector", 3, **kwargs)
    assert result == [sentinel]
    assert captured["vec"] == b"vector"
    assert captured["k"] == 3
    assert captured["args"] == ()

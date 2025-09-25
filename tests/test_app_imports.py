"""Regression tests for importing the legacy Flask facade."""

import importlib
import sys


def test_app_module_imports_cleanly(monkeypatch):
    """Ensure importing ``app`` doesn't raise and wires the ask2 router."""

    # Drop any cached module so we exercise the top-level imports each time.
    monkeypatch.syspath_prepend("/workspace/Sustainacore")
    sys.modules.pop("app", None)

    module = importlib.import_module("app")

    # The module should expose the Flask ``app`` and the legacy router helper.
    assert hasattr(module, "app")
    assert hasattr(module, "_call_route_ask2_facade")

    route = getattr(module, "_route_ask2", None)
    assert callable(route), "legacy ask2 router should be available"

    shaped, status = module._call_route_ask2_facade("hi", 2)
    assert status == 200
    assert "routing" in shaped["meta"]
    assert shaped["meta"]["routing"] in {"smalltalk", "gemini_first"}

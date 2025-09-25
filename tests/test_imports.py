import importlib
import pytest


@pytest.mark.parametrize("name", ["app", "app.retrieval.app"])
def test_imports_do_not_crash(name):
    importlib.import_module(name)

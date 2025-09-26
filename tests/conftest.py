import importlib.util
import pathlib
import sys

import pytest


ROOT = pathlib.Path(__file__).resolve().parents[1]


def _ensure_app_module():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    app_spec = importlib.util.spec_from_file_location("app", str(ROOT / "app.py"))
    app_module = importlib.util.module_from_spec(app_spec)
    app_spec.loader.exec_module(app_module)
    sys.modules.setdefault("app", app_module)
    return app_module


def _load_facade_module():
    _ensure_app_module()
    facade_path = ROOT / ".ask_facade" / "wsgi_ask_facade.py"
    facade_spec = importlib.util.spec_from_file_location("ask_facade_wsgi", str(facade_path))
    facade_module = importlib.util.module_from_spec(facade_spec)
    facade_spec.loader.exec_module(facade_module)
    sys.modules.setdefault("ask_facade_wsgi", facade_module)
    return facade_module


@pytest.fixture(scope="session")
def facade_module():
    return _load_facade_module()


@pytest.fixture(scope="session")
def facade_app(facade_module):
    app = getattr(facade_module, "base_app", None)
    assert app is not None, "Facade Flask app should be available"
    return app


@pytest.fixture
def facade_client(facade_app):
    with facade_app.test_client() as client:
        yield client

import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

app_spec = importlib.util.spec_from_file_location("app", str(ROOT / "app.py"))
app_module = importlib.util.module_from_spec(app_spec)
app_spec.loader.exec_module(app_module)
sys.modules.setdefault("app", app_module)

facade_path = ROOT / ".ask_facade" / "wsgi_ask_facade.py"
facade_spec = importlib.util.spec_from_file_location("ask_facade_wsgi", str(facade_path))
facade_module = importlib.util.module_from_spec(facade_spec)
facade_spec.loader.exec_module(facade_module)

app = getattr(facade_module, "base_app", None)


def test_metrics_positive_uptime():
    assert app is not None, "Facade Flask app should be available"
    with app.test_client() as client:
        response = client.get("/metrics")
        assert response.status_code == 200
        payload = response.get_json()
        assert isinstance(payload, dict)
        uptime = payload.get("uptime")
        assert isinstance(uptime, (int, float))
        assert uptime > 0.0

"""WSGI facade that forwards directly to the Flask application."""

from __future__ import annotations

from importlib import import_module
import time
from flask import jsonify

app_mod = import_module("app")
app = getattr(app_mod, "app", None)

if app is None:
    raise RuntimeError("Flask application not found in app module")

_MODULE_START_TS = time.time()


def _ask2_passthrough():
    view = getattr(app_mod, "ask2", None)
    if callable(view):
        return view()
    return jsonify({"answer": "", "sources": [], "contexts": [], "meta": {"error": "ask2_missing"}}), 503


if "ask2" not in app.view_functions:
    app.add_url_rule("/ask2", endpoint="ask2", view_func=_ask2_passthrough, methods=["GET", "POST"])


def _register_if_missing(rule: str, endpoint: str, view_func):
    if endpoint in app.view_functions:
        return
    app.add_url_rule(rule, endpoint=endpoint, view_func=view_func, methods=["GET"])


def _healthz_facade():
    return jsonify({"ok": True})


def _metrics_facade():
    uptime = max(time.time() - _MODULE_START_TS, 0.0)
    return jsonify({"uptime": float(uptime)})


_register_if_missing("/healthz", "healthz_facade", _healthz_facade)
_register_if_missing("/metrics", "metrics_facade", _metrics_facade)

__all__ = ["app"]

base_app = app

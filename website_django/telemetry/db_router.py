from __future__ import annotations

from django.conf import settings


class TelemetryRouter:
    def _telemetry_alias(self) -> str:
        return getattr(settings, "TELEMETRY_DB_ALIAS", "default")

    def db_for_read(self, model, **hints):
        if model._meta.app_label == "telemetry":
            return self._telemetry_alias()
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == "telemetry":
            return self._telemetry_alias()
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        telemetry_alias = self._telemetry_alias()
        if app_label == "telemetry":
            return db == telemetry_alias
        if db == telemetry_alias and telemetry_alias != "default":
            return False
        return None

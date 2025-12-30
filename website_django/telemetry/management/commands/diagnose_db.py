from __future__ import annotations

import json
import os
import signal
import uuid
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations.recorder import MigrationRecorder
from django.utils.timezone import now

from telemetry.models import WebEvent


class Command(BaseCommand):
    help = "Print active database settings and telemetry migration/table status."

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            default=getattr(settings, "TELEMETRY_DB_ALIAS", DEFAULT_DB_ALIAS),
            help="Database alias to inspect (default: TELEMETRY_DB_ALIAS).",
        )
        parser.add_argument(
            "--fail-on-sqlite",
            action="store_true",
            help="Exit with error if the active engine is sqlite3.",
        )
        parser.add_argument(
            "--verify-insert",
            action="store_true",
            help="Insert a diagnostic event row and confirm readback.",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=25,
            help="Abort diagnostics after N seconds (default: 25).",
        )

    @staticmethod
    def _mask_value(value: str | None) -> str | None:
        if not value:
            return None
        if len(value) <= 4:
            return "***"
        return f"{value[:2]}***{value[-2:]}"

    def handle(self, *args, **options):
        timeout_seconds = options.get("timeout", 0)

        def _timeout_handler(signum, frame):
            raise TimeoutError("diagnose_db timeout")

        if timeout_seconds and hasattr(signal, "signal"):
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(timeout_seconds)

        alias = options["database"]
        if alias not in connections:
            self.stdout.write(self.style.ERROR(f"Database alias '{alias}' is not configured."))
            return

        conn = connections[alias]
        settings_dict = conn.settings_dict
        safe_settings = {
            "ENGINE": settings_dict.get("ENGINE"),
            "NAME": settings_dict.get("NAME"),
            "HOST": settings_dict.get("HOST"),
            "USER": settings_dict.get("USER"),
            "PORT": settings_dict.get("PORT"),
        }

        self.stdout.write("Telemetry DB diagnosis")
        self.stdout.write(f"production_mode: {getattr(settings, 'PRODUCTION_MODE', False)}")
        self.stdout.write(f"alias: {alias}")
        for key, value in safe_settings.items():
            if value:
                if key in {"USER"}:
                    value = self._mask_value(str(value))
                self.stdout.write(f"{key.lower()}: {value}")

        engine = settings_dict.get("ENGINE") or ""
        if options["fail_on_sqlite"] and "sqlite3" in engine:
            raise CommandError("Active DB engine is sqlite3; Oracle is required for production.")

        env_files = [
            "/etc/sustainacore.env",
            "/etc/sustainacore/db.env",
            "/etc/sysconfig/sustainacore-django.env",
        ]
        self.stdout.write("env_files:")
        for path in env_files:
            status = "missing"
            if os.path.exists(path):
                status = "readable" if os.access(path, os.R_OK) else "present"
            self.stdout.write(f"  - {path}: {status}")

        env_summary = {
            "DB_USER": self._mask_value(os.environ.get("DB_USER")),
            "DB_PASSWORD": "***" if os.environ.get("DB_PASSWORD") or os.environ.get("DB_PASS") else None,
            "DB_DSN": self._mask_value(os.environ.get("DB_DSN")),
            "ORACLE_USER": self._mask_value(os.environ.get("ORACLE_USER")),
            "ORACLE_PASSWORD": "***" if os.environ.get("ORACLE_PASSWORD") else None,
            "ORACLE_DSN": self._mask_value(os.environ.get("ORACLE_DSN")),
            "ORACLE_CONNECT_STRING": self._mask_value(os.environ.get("ORACLE_CONNECT_STRING")),
            "TNS_ADMIN": os.environ.get("TNS_ADMIN"),
            "ORACLE_CLIENT_LIB_DIR": os.environ.get("ORACLE_CLIENT_LIB_DIR"),
        }
        self.stdout.write("oracle_env:")
        for key, value in env_summary.items():
            if value:
                self.stdout.write(f"  - {key}: {value}")

        table_names = set()
        expected = {"W_WEB_CONSENT", "W_WEB_EVENT", "W_WEB_SESSION"}
        if "oracle" in engine:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT table_name FROM user_tables WHERE table_name IN (:a, :b, :c)",
                        {"a": "W_WEB_CONSENT", "b": "W_WEB_EVENT", "c": "W_WEB_SESSION"},
                    )
                    table_names = {row[0] for row in cursor.fetchall()}
            except TimeoutError as exc:
                raise CommandError(str(exc)) from exc
            except Exception as exc:
                self.stdout.write(
                    self.style.WARNING(f"oracle_table_query_failed: {exc.__class__.__name__}")
                )
        if not table_names:
            table_names = set(conn.introspection.table_names())
        missing = sorted(expected - table_names)
        present = sorted(expected & table_names)
        self.stdout.write(f"tables_present: {', '.join(present) if present else 'none'}")
        if missing:
            self.stdout.write(self.style.WARNING(f"tables_missing: {', '.join(missing)}"))

        recorder = MigrationRecorder(conn)
        applied = recorder.applied_migrations()
        telemetry_migrations = sorted(
            name for app, name in applied if app == "telemetry"
        )
        if telemetry_migrations:
            self.stdout.write(f"telemetry_migrations: {', '.join(telemetry_migrations)}")
        else:
            self.stdout.write(self.style.WARNING("telemetry_migrations: none"))

        if missing or not telemetry_migrations:
            self.stdout.write(
                self.style.WARNING(
                    "If migrations failed, confirm the Oracle user has CREATE TABLE and QUOTA privileges."
                )
            )

        if options["verify_insert"]:
            if missing:
                raise CommandError("Missing telemetry tables; cannot verify insert.")
            diag_id = uuid.uuid4()
            payload = {
                "diag": True,
                "ts": (now() + timedelta(seconds=0)).isoformat(),
            }
            try:
                WebEvent.objects.using(alias).create(
                    event_id=diag_id,
                    event_ts=now(),
                    consent_analytics_effective="N",
                    event_type="diagnose",
                    path="/diagnose",
                    payload_json=json.dumps(payload),
                )
            except TimeoutError as exc:
                raise CommandError(str(exc)) from exc
            except Exception as exc:
                raise CommandError(f"diagnostic_insert_failed: {exc.__class__.__name__}") from exc

            exists = WebEvent.objects.using(alias).filter(event_id=diag_id).exists()
            if not exists:
                raise CommandError("diagnostic_readback_failed: inserted row not found.")
            self.stdout.write("diagnostic_insert_readback: ok")

        if timeout_seconds:
            signal.alarm(0)

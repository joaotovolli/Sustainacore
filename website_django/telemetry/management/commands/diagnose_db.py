from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations.recorder import MigrationRecorder


class Command(BaseCommand):
    help = "Print active database settings and telemetry migration/table status."

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            default=getattr(settings, "TELEMETRY_DB_ALIAS", DEFAULT_DB_ALIAS),
            help="Database alias to inspect (default: TELEMETRY_DB_ALIAS).",
        )

    def handle(self, *args, **options):
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
        self.stdout.write(f"alias: {alias}")
        for key, value in safe_settings.items():
            if value:
                self.stdout.write(f"{key.lower()}: {value}")

        table_names = set(conn.introspection.table_names())
        expected = {"W_WEB_CONSENT", "W_WEB_EVENT", "W_WEB_SESSION"}
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

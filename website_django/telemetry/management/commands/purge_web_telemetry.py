from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import now

from telemetry.models import WebEvent, WebSession


class Command(BaseCommand):
    help = "Purge web telemetry events older than N days."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=settings.TELEMETRY_RETENTION_DAYS,
            help="Number of days to retain web telemetry events (default from settings).",
        )
        parser.add_argument(
            "--sessions",
            action="store_true",
            help="Also purge web session rows older than N days based on last_seen_ts.",
        )

    def handle(self, *args, **options):
        days = int(options["days"])
        cutoff = now() - timedelta(days=days)
        deleted_events, _ = WebEvent.objects.filter(event_ts__lt=cutoff).delete()
        message = f"Deleted {deleted_events} web events older than {days} days."

        if options["sessions"]:
            deleted_sessions, _ = WebSession.objects.filter(last_seen_ts__lt=cutoff).delete()
            message = f"{message} Deleted {deleted_sessions} web sessions."

        self.stdout.write(self.style.SUCCESS(message))

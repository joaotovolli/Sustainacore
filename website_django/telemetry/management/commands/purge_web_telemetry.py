from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import now

from telemetry.models import WebConsent, WebEvent, WebEventDaily, WebSession


class Command(BaseCommand):
    help = "Purge web telemetry raw events and optional long-lived tables."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=None,
            help="Deprecated alias for --raw-days.",
        )
        parser.add_argument(
            "--raw-days",
            type=int,
            default=settings.TELEMETRY_RAW_RETENTION_DAYS,
            help="Number of days to retain raw web telemetry events.",
        )
        parser.add_argument(
            "--aggregate-days",
            type=int,
            default=settings.TELEMETRY_AGGREGATE_RETENTION_DAYS,
            help="Number of days to retain daily aggregate rows.",
        )
        parser.add_argument(
            "--session-days",
            type=int,
            default=settings.TELEMETRY_SESSION_RETENTION_DAYS,
            help="Number of days to retain web session rows.",
        )
        parser.add_argument(
            "--consent-days",
            type=int,
            default=settings.TELEMETRY_CONSENT_RETENTION_DAYS,
            help="Number of days to retain consent rows.",
        )
        parser.add_argument(
            "--sessions",
            action="store_true",
            help="Also purge web session rows based on --session-days.",
        )
        parser.add_argument(
            "--consents",
            action="store_true",
            help="Also purge consent rows based on --consent-days.",
        )
        parser.add_argument(
            "--aggregates",
            action="store_true",
            help="Also purge daily aggregate rows based on --aggregate-days.",
        )

    def handle(self, *args, **options):
        raw_days = int(options["days"] or options["raw_days"])
        raw_cutoff = now() - timedelta(days=raw_days)
        db_alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")
        deleted_events, _ = WebEvent.objects.using(db_alias).filter(event_ts__lt=raw_cutoff).delete()
        messages = [f"Deleted {deleted_events} raw web events older than {raw_days} days."]

        if options["sessions"]:
            session_cutoff = now() - timedelta(days=int(options["session_days"]))
            deleted_sessions, _ = (
                WebSession.objects.using(db_alias).filter(last_seen_ts__lt=session_cutoff).delete()
            )
            messages.append(
                f"Deleted {deleted_sessions} web sessions older than {int(options['session_days'])} days."
            )

        if options["consents"]:
            consent_cutoff = now() - timedelta(days=int(options["consent_days"]))
            deleted_consents, _ = (
                WebConsent.objects.using(db_alias).filter(created_ts__lt=consent_cutoff).delete()
            )
            messages.append(
                f"Deleted {deleted_consents} consent rows older than {int(options['consent_days'])} days."
            )

        if options["aggregates"]:
            aggregate_cutoff = now().date() - timedelta(days=int(options["aggregate_days"]))
            deleted_aggregates, _ = (
                WebEventDaily.objects.using(db_alias).filter(bucket_date__lt=aggregate_cutoff).delete()
            )
            messages.append(
                "Deleted "
                f"{deleted_aggregates} daily aggregate rows older than {int(options['aggregate_days'])} days."
            )

        self.stdout.write(self.style.SUCCESS(" ".join(messages)))

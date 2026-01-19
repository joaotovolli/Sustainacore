from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import now

from telemetry.models import WebAsk2Conversation


class Command(BaseCommand):
    help = "Purge Ask2 conversations and messages older than N days."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=settings.TELEMETRY_RETENTION_DAYS,
            help="Number of days to retain Ask2 conversations (default from settings).",
        )

    def handle(self, *args, **options):
        days = int(options["days"])
        cutoff = now() - timedelta(days=days)
        db_alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")
        deleted_rows, _ = (
            WebAsk2Conversation.objects.using(db_alias)
            .filter(last_message_at__lt=cutoff)
            .delete()
        )
        message = f"Deleted {deleted_rows} Ask2 rows older than {days} days."
        self.stdout.write(self.style.SUCCESS(message))

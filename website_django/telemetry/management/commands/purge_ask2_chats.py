from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import now

from telemetry.models import WebAsk2Conversation, WebAsk2Message


class Command(BaseCommand):
    help = "Purge Ask2 conversation and message records older than N days."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=180,
            help="Delete rows older than this many days (default: 180).",
        )

    def handle(self, *args, **options):
        days = int(options["days"])
        cutoff = now() - timedelta(days=days)
        db_alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")

        deleted_messages, _ = WebAsk2Message.objects.using(db_alias).filter(
            created_at__lt=cutoff
        ).delete()
        deleted_conversations, _ = WebAsk2Conversation.objects.using(db_alias).filter(
            last_message_at__lt=cutoff
        ).delete()

        self.stdout.write(
            f"deleted_messages={deleted_messages} deleted_conversations={deleted_conversations}"
        )

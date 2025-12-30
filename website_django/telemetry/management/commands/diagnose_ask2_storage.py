from __future__ import annotations

import uuid
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils.timezone import now

from telemetry.models import WebAsk2Conversation, WebAsk2Message


class Command(BaseCommand):
    help = "Verify Ask2 conversation/message storage in the telemetry database."

    def add_arguments(self, parser):
        parser.add_argument(
            "--fail-on-sqlite",
            action="store_true",
            help="Exit with error if the active engine is sqlite3.",
        )
        parser.add_argument(
            "--verify-insert",
            action="store_true",
            help="Insert a diagnostic conversation/message pair and confirm readback.",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=60,
            help="Abort diagnostics after N seconds (default: 60).",
        )

    def handle(self, *args, **options):
        db_alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")
        engine = settings.DATABASES.get(db_alias, {}).get("ENGINE", "")
        self.stdout.write(f"alias: {db_alias}")
        self.stdout.write(f"engine: {engine}")
        if options["fail_on_sqlite"] and "sqlite3" in engine:
            raise CommandError("Active DB engine is sqlite3; Oracle is required for production.")

        if options["verify_insert"]:
            conversation_id = uuid.uuid4()
            now_ts = now()
            WebAsk2Conversation.objects.using(db_alias).create(
                conversation_id=conversation_id,
                created_at=now_ts,
                last_message_at=now_ts,
                path_first="/diagnose/ask2",
            )
            WebAsk2Message.objects.using(db_alias).create(
                conversation_id=conversation_id,
                created_at=now_ts,
                role="user",
                content="diagnostic_prompt",
                content_len=len("diagnostic_prompt"),
                status="ok",
            )
            WebAsk2Message.objects.using(db_alias).create(
                conversation_id=conversation_id,
                created_at=now_ts,
                role="assistant",
                content="diagnostic_reply",
                content_len=len("diagnostic_reply"),
                status="ok",
            )
            msg_count = WebAsk2Message.objects.using(db_alias).filter(
                conversation_id=conversation_id
            ).count()
            if msg_count != 2:
                raise CommandError("diagnostic_readback_failed: message count mismatch")
            self.stdout.write("diagnostic_insert_readback: ok")
            WebAsk2Message.objects.using(db_alias).filter(
                conversation_id=conversation_id
            ).delete()
            WebAsk2Conversation.objects.using(db_alias).filter(
                conversation_id=conversation_id
            ).delete()

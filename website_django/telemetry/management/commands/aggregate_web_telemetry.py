from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from telemetry.models import WebEvent, WebEventDaily
from telemetry.utils import status_class, stable_token


@dataclass
class _AggregateBucket:
    event_count: int = 0
    total_response_ms: int = 0
    max_response_ms: int | None = None
    session_keys: set[str] = field(default_factory=set)
    user_ids: set[int] = field(default_factory=set)
    ip_hashes: set[str] = field(default_factory=set)

    def add(self, row: dict) -> None:
        self.event_count += 1
        response_ms = row.get("response_ms")
        if response_ms is not None:
            value = int(response_ms)
            self.total_response_ms += value
            self.max_response_ms = value if self.max_response_ms is None else max(self.max_response_ms, value)
        session_key = row.get("session_key")
        if session_key:
            self.session_keys.add(str(session_key))
        user_id = row.get("user_id")
        if user_id is not None:
            self.user_ids.add(int(user_id))
        ip_hash = row.get("ip_hash")
        if ip_hash:
            self.ip_hashes.add(str(ip_hash))


class Command(BaseCommand):
    help = "Roll raw web telemetry into compact daily aggregate rows."

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            dest="bucket_date",
            help="UTC date to aggregate in YYYY-MM-DD format (default: yesterday).",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=1,
            help="Number of UTC days to aggregate ending at --date or yesterday.",
        )

    def handle(self, *args, **options):
        days = max(1, int(options["days"]))
        end_day = date.fromisoformat(options["bucket_date"]) if options.get("bucket_date") else (
            datetime.now(timezone.utc).date() - timedelta(days=1)
        )
        start_day = end_day - timedelta(days=days - 1)
        db_alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")
        day = start_day
        while day <= end_day:
            created = self._aggregate_day(db_alias, day)
            self.stdout.write(f"{day.isoformat()}: created {created} aggregate rows")
            day += timedelta(days=1)

    def _aggregate_day(self, alias: str, bucket_day: date) -> int:
        start_ts = datetime.combine(bucket_day, time.min, tzinfo=timezone.utc)
        end_ts = start_ts + timedelta(days=1)
        rows = list(
            WebEvent.objects.using(alias)
            .filter(event_ts__gte=start_ts, event_ts__lt=end_ts)
            .values(
                "event_type",
                "event_name",
                "path",
                "referrer_host",
                "referrer",
                "country_code",
                "consent_analytics_effective",
                "is_bot",
                "status_code",
                "response_ms",
                "session_key",
                "user_id",
                "ip_hash",
            )
            .iterator(chunk_size=1000)
        )

        buckets: dict[tuple, _AggregateBucket] = defaultdict(_AggregateBucket)
        for row in rows:
            path = row.get("path") or "/"
            referrer_value = row.get("referrer_host") or row.get("referrer")
            key = (
                row.get("event_type") or "",
                row.get("event_name") or "",
                path,
                stable_token(path) or "",
                referrer_value or "",
                row.get("country_code") or "",
                row.get("consent_analytics_effective") or "N",
                row.get("is_bot") or "N",
                status_class(row.get("status_code")) or "",
            )
            buckets[key].add(row)

        with transaction.atomic(using=alias):
            WebEventDaily.objects.using(alias).filter(bucket_date=bucket_day).delete()
            payload = []
            for key, bucket in buckets.items():
                (
                    event_type,
                    event_name,
                    path,
                    path_hash,
                    referrer_value,
                    country_code,
                    consent_effective,
                    is_bot,
                    status_bucket,
                ) = key
                payload.append(
                    WebEventDaily(
                        bucket_date=bucket_day,
                        event_type=event_type,
                        event_name=event_name or None,
                        path=path,
                        path_hash=path_hash,
                        referrer_host=referrer_value or None,
                        country_code=country_code or None,
                        consent_analytics_effective=consent_effective,
                        is_bot=is_bot,
                        status_class=status_bucket or None,
                        event_count=bucket.event_count,
                        unique_sessions=len(bucket.session_keys),
                        unique_users=len(bucket.user_ids),
                        unique_visitors=len(bucket.ip_hashes),
                        total_response_ms=bucket.total_response_ms,
                        max_response_ms=bucket.max_response_ms,
                    )
                )
            WebEventDaily.objects.using(alias).bulk_create(payload, batch_size=500)
        return len(payload)

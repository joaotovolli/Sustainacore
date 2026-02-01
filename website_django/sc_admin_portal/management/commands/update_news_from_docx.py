from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from sc_admin_portal.docx_import import build_news_body_from_docx
from sc_admin_portal.news_storage import (
    NewsStorageError,
    create_news_asset,
    update_news_post_body,
)


class Command(BaseCommand):
    help = "Update an existing news post body from a DOCX file."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--file", required=True, help="Path to the DOCX file.")
        parser.add_argument(
            "--news-id",
            required=True,
            help="News item id (e.g., NEWS_ITEMS:41 or 41).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse the DOCX but do not update Oracle.",
        )

    def handle(self, *args, **options) -> None:
        file_path = options["file"]
        raw_news_id = (options["news_id"] or "").strip()
        dry_run = options["dry_run"]

        if ":" in raw_news_id:
            raw_news_id = raw_news_id.split(":", 1)[-1]
        try:
            news_id = int(raw_news_id)
        except ValueError as exc:
            raise CommandError("Invalid news id.") from exc

        stats: dict[str, int] = {}

        def upload_asset(file_name: str | None, mime_type: str | None, file_bytes: bytes) -> int:
            if dry_run:
                return 0
            return create_news_asset(
                news_id=news_id,
                file_name=file_name,
                mime_type=mime_type,
                file_bytes=file_bytes,
            )

        headline, body_html = build_news_body_from_docx(
            file_path,
            asset_uploader=upload_asset,
            stats=stats,
        )

        images_found = stats.get("images_found", 0)
        images_uploaded = stats.get("images_uploaded", 0)
        self.stdout.write(
            f"Images found: {images_found} | Uploaded: {images_uploaded} | Storage: news_assets"
        )

        if not body_html:
            raise CommandError("No body content found in DOCX.")
        if images_found and not images_uploaded:
            raise CommandError("DOCX contains images but none could be extracted.")

        if dry_run:
            self.stdout.write(headline)
            self.stdout.write(body_html)
            return

        try:
            result = update_news_post_body(news_id=news_id, body_html=body_html)
        except NewsStorageError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(f"Updated NEWS_ITEMS:{news_id}"))
        self.stdout.write(self.style.SUCCESS(f"Assets linked: {len(result.get('asset_ids', []))}"))

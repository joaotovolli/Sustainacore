from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from sc_admin_portal.docx_import import build_news_body_from_docx
from sc_admin_portal.news_storage import create_news_asset, create_news_post


class Command(BaseCommand):
    help = "Publish a news post from a DOCX file."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--file", required=True, help="Path to the DOCX file.")
        parser.add_argument("--tags", default="", help="Comma-separated tags.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse the DOCX but do not publish to Oracle.",
        )

    def handle(self, *args, **options) -> None:
        file_path = options["file"]
        tags = options["tags"] or ""
        dry_run = options["dry_run"]

        def upload_asset(file_name: str | None, mime_type: str | None, file_bytes: bytes) -> int:
            return create_news_asset(
                news_id=None,
                file_name=file_name,
                mime_type=mime_type,
                file_bytes=file_bytes,
            )

        headline, body_html = build_news_body_from_docx(file_path, asset_uploader=upload_asset)

        if not headline:
            raise CommandError("No headline found in DOCX.")
        if not body_html:
            raise CommandError("No body content found in DOCX.")

        if dry_run:
            self.stdout.write(headline)
            self.stdout.write(body_html)
            return

        news_item = create_news_post(headline=headline, tags=tags, body_html=body_html)
        self.stdout.write(self.style.SUCCESS(f"Published {news_item['id']}"))
        self.stdout.write(self.style.SUCCESS(news_item["url"]))

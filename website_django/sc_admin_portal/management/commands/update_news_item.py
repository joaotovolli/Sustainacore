from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from sc_admin_portal.docx_import import build_news_body_from_docx
from sc_admin_portal.news_storage import (
    NewsStorageError,
    create_news_asset,
    update_news_item,
)


class Command(BaseCommand):
    help = "Update an existing news item title/body (DOCX or HTML)."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--news-id", required=True, help="News item id (e.g., NEWS_ITEMS:44 or 44).")
        parser.add_argument("--title", help="Optional new headline.")
        parser.add_argument("--body-file", help="Path to an HTML file to replace body_html.")
        parser.add_argument("--docx", help="Path to a DOCX file to replace body_html.")

    def handle(self, *args, **options) -> None:
        news_id = (options["news_id"] or "").strip()
        title = (options.get("title") or "").strip() or None
        body_file = options.get("body_file")
        docx_path = options.get("docx")
        raw_id = news_id.split(":", 1)[-1] if ":" in news_id else news_id
        try:
            news_id_value = int(raw_id)
        except ValueError as exc:
            raise CommandError("Invalid news id.") from exc
        if news_id_value <= 0:
            raise CommandError("Invalid news id.")

        if body_file and docx_path:
            raise CommandError("Choose either --body-file or --docx, not both.")
        if not title and not body_file and not docx_path:
            raise CommandError("Provide --title, --body-file, or --docx.")

        body_html = None
        if body_file:
            body_path = Path(body_file)
            if not body_path.exists():
                raise CommandError("body file not found.")
            body_html = body_path.read_text(encoding="utf-8")

        if docx_path:
            docx_file = Path(docx_path)
            if not docx_file.exists():
                raise CommandError("docx file not found.")
            stats: dict[str, int] = {}

            def upload_asset(file_name: str | None, mime_type: str | None, file_bytes: bytes) -> int:
                return create_news_asset(
                    news_id=news_id_value,
                    file_name=file_name,
                    mime_type=mime_type,
                    file_bytes=file_bytes,
                )

            docx_headline, docx_body = build_news_body_from_docx(
                str(docx_file),
                asset_uploader=upload_asset,
                stats=stats,
            )

            images_found = stats.get("images_found", 0)
            images_uploaded = stats.get("images_uploaded", 0)
            self.stdout.write(
                f"Images found: {images_found} | Uploaded: {images_uploaded} | Storage: news_assets"
            )

            if not docx_body:
                raise CommandError("No body content found in DOCX.")
            if images_found and not images_uploaded:
                raise CommandError("DOCX contains images but none could be extracted.")

            body_html = docx_body
            if docx_headline and not title:
                title = docx_headline

        try:
            result = update_news_item(
                news_id=news_id_value,
                headline=title,
                body_html=body_html,
            )
        except (NewsStorageError, ValueError) as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(f"Updated {result['id']}"))

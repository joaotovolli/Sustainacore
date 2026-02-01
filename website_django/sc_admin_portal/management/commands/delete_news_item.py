from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from sc_admin_portal.news_storage import NewsStorageError, delete_news_item, get_news_item_preview


class Command(BaseCommand):
    help = "Delete a news item and its associated assets."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--news-id", required=True, help="News item id (e.g., NEWS_ITEMS:44 or 44).")
        parser.add_argument(
            "--yes-delete-permanently",
            action="store_true",
            help="Confirm permanent deletion.",
        )

    def handle(self, *args, **options) -> None:
        news_id = (options["news_id"] or "").strip()
        confirm = options["yes_delete_permanently"]

        preview = get_news_item_preview(news_id=news_id)
        if not preview:
            raise CommandError("News item not found.")

        self.stdout.write(
            f"{preview['id']} | {preview['title']} | Assets: {preview['asset_count']}"
        )
        if preview.get("asset_ids"):
            self.stdout.write(f"Asset ids: {', '.join(str(a) for a in preview['asset_ids'])}")

        if not confirm:
            self.stdout.write("Dry run only. Re-run with --yes-delete-permanently to delete.")
            return

        try:
            result = delete_news_item(news_id=news_id)
        except NewsStorageError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(f"Deleted {result['id']}"))

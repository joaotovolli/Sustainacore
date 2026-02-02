from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from django.conf import settings
from django.core.management.base import BaseCommand

from core import sitemaps


class Command(BaseCommand):
    help = "Generate sitemap index and section XML files."

    def add_arguments(self, parser):
        parser.add_argument(
            "--out",
            dest="out",
            default=None,
            help="Output directory for sitemap.xml and sitemaps/*.xml",
        )

    def handle(self, *args, **options):
        out_dir = Path(options.get("out") or settings.SITEMAP_OUTPUT_DIR)
        sections_dir = out_dir / "sitemaps"
        sections_dir.mkdir(parents=True, exist_ok=True)

        index_entries = sitemaps.get_section_index_entries()
        index_xml = sitemaps.render_sitemap_index(index_entries)
        index_path = out_dir / "sitemap.xml"
        index_path.write_text(index_xml, encoding="utf-8")

        company_count = 0
        news_count = 0
        shard_count = 0

        for entry in index_entries:
            loc = entry.get("loc") or ""
            parsed = urlparse(loc)
            section_name = Path(parsed.path).stem
            if not section_name or parsed.path is None:
                continue
            entries = sitemaps.get_section_entries(section_name)
            if section_name.startswith(sitemaps.COMPANY_SITEMAP_PREFIX):
                company_count += len(entries)
                shard_count += 1
            if section_name.startswith(sitemaps.NEWS_SITEMAP_PREFIX):
                news_count += len(entries)
                shard_count += 1
            xml = sitemaps.render_urlset(entries)
            section_path = sections_dir / f"{section_name}.xml"
            section_path.write_text(xml, encoding="utf-8")

        self.stdout.write(f"Wrote sitemap index: {index_path}")
        self.stdout.write(f"Company URLs: {company_count}")
        self.stdout.write(f"News item URLs: {news_count}")
        self.stdout.write(f"Shard files: {shard_count}")

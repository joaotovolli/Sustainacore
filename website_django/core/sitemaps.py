from datetime import datetime, timezone
from pathlib import Path

from django.conf import settings
from django.contrib.sitemaps import Sitemap
from django.urls import reverse


def _template_lastmod(template_name: str):
    template_path = Path(settings.BASE_DIR) / "templates" / template_name
    try:
        mtime = template_path.stat().st_mtime
    except FileNotFoundError:
        return None
    return datetime.fromtimestamp(mtime, tz=timezone.utc)


class StaticViewSitemap(Sitemap):
    changefreq = "weekly"
    priority = 0.7

    _template_map = {
        "home": "home.html",
        "tech100": "tech100.html",
        "tech100_index": "tech100_index_overview.html",
        "tech100_performance": "tech100_performance.html",
        "tech100_constituents": "tech100_constituents.html",
        "tech100_attribution": "tech100_attribution.html",
        "tech100_stats": "tech100_stats.html",
        "news": "news.html",
        "ask2:ask2_page": "ask2.html",
        "press_index": "press_index.html",
        "press_tech100": "press_tech100.html",
        "privacy": "privacy.html",
        "tech100_methodology": "tech100_methodology.html",
    }

    def items(self):
        return list(self._template_map.keys())

    def location(self, item):
        return reverse(item)

    def lastmod(self, item):
        template_name = self._template_map.get(item)
        if not template_name:
            return None
        return _template_lastmod(template_name)


SITEMAPS = {
    "static": StaticViewSitemap,
}

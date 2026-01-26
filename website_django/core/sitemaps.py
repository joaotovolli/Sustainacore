from datetime import date, datetime, timezone
from pathlib import Path
from xml.sax.saxutils import escape

from django.conf import settings
from django.contrib.sitemaps import Sitemap
from django.urls import reverse


def _template_lastmod(template_name: str):
    template_root = Path(settings.BASE_DIR) / "templates"
    template_path = template_root / template_name
    if not template_path.exists():
        app_template_path = Path(settings.BASE_DIR) / "ai_reg" / "templates" / template_name
        if app_template_path.exists():
            template_path = app_template_path
        else:
            return None
    mtime = template_path.stat().st_mtime
    return datetime.fromtimestamp(mtime, tz=timezone.utc)


def _canonical_url(path: str) -> str:
    return f"{settings.SITE_URL.rstrip('/')}{path}"


def _format_lastmod(value: datetime | date | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.date().isoformat()
    return value.isoformat()


def _build_entries(template_map: dict[str, str], changefreq: str, priority: float) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for url_name, template_name in template_map.items():
        path = reverse(url_name)
        loc = _canonical_url(path)
        lastmod = _format_lastmod(_template_lastmod(template_name))
        entry = {
            "loc": loc,
            "changefreq": changefreq,
            "priority": f"{priority:.1f}",
        }
        if lastmod:
            entry["lastmod"] = lastmod
        entries.append(entry)
    return entries


def _entries_lastmod(entries: list[dict[str, str]]) -> str | None:
    lastmods = [entry.get("lastmod") for entry in entries if entry.get("lastmod")]
    return max(lastmods) if lastmods else None


def render_urlset(entries: list[dict[str, str]]) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for entry in entries:
        lines.append("  <url>")
        lines.append(f"    <loc>{escape(entry['loc'])}</loc>")
        if entry.get("lastmod"):
            lines.append(f"    <lastmod>{entry['lastmod']}</lastmod>")
        if entry.get("changefreq"):
            lines.append(f"    <changefreq>{entry['changefreq']}</changefreq>")
        if entry.get("priority"):
            lines.append(f"    <priority>{entry['priority']}</priority>")
        lines.append("  </url>")
    lines.append("</urlset>")
    return "\n".join(lines)


def render_sitemap_index(section_meta: list[dict[str, str]]) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for entry in section_meta:
        lines.append("  <sitemap>")
        lines.append(f"    <loc>{escape(entry['loc'])}</loc>")
        if entry.get("lastmod"):
            lines.append(f"    <lastmod>{entry['lastmod']}</lastmod>")
        lines.append("  </sitemap>")
    lines.append("</sitemapindex>")
    return "\n".join(lines)


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
        "terms": "terms.html",
        "corrections": "corrections.html",
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


_STATIC_TEMPLATE_MAP = {
    "home": "home.html",
    "press_index": "press_index.html",
    "press_tech100": "press_tech100.html",
    "privacy": "privacy.html",
    "terms": "terms.html",
    "corrections": "corrections.html",
    "ask2:ask2_page": "ask2.html",
    "ai_regulation": "ai_reg/ai_regulation.html",
}

_TECH100_TEMPLATE_MAP = {
    "tech100": "tech100.html",
    "tech100_index": "tech100_index_overview.html",
    "tech100_performance": "tech100_performance.html",
    "tech100_constituents": "tech100_constituents.html",
    "tech100_attribution": "tech100_attribution.html",
    "tech100_stats": "tech100_stats.html",
    "tech100_methodology": "tech100_methodology.html",
}

_NEWS_TEMPLATE_MAP = {
    "news": "news.html",
}

SITEMAP_SECTIONS = (
    ("static", _STATIC_TEMPLATE_MAP, "weekly", 0.7),
    ("tech100", _TECH100_TEMPLATE_MAP, "weekly", 0.7),
    ("news", _NEWS_TEMPLATE_MAP, "daily", 0.6),
)


def get_section_entries(section: str) -> list[dict[str, str]]:
    for key, template_map, changefreq, priority in SITEMAP_SECTIONS:
        if key == section:
            return _build_entries(template_map, changefreq, priority)
    return []


def get_section_lastmod(section: str) -> str | None:
    entries = get_section_entries(section)
    return _entries_lastmod(entries)


def get_section_index_entries() -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for key, template_map, changefreq, priority in SITEMAP_SECTIONS:
        section_entries = _build_entries(template_map, changefreq, priority)
        if not section_entries:
            continue
        lastmod = _entries_lastmod(section_entries)
        entries.append(
            {
                "loc": _canonical_url(f"/sitemaps/{key}.xml"),
                "lastmod": lastmod or "",
            }
        )
    return entries


SITEMAPS = {
    "static": StaticViewSitemap,
}

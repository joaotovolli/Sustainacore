from datetime import date, datetime, timezone
import logging
from pathlib import Path
from xml.sax.saxutils import escape

from django.conf import settings
from django.contrib.sitemaps import Sitemap
from django.core.cache import cache
from django.urls import reverse

from core import news_data
from core import tech100_company_data

logger = logging.getLogger(__name__)

COMPANY_SITEMAP_CHUNK = 2000
NEWS_SITEMAP_CHUNK = 2000
COMPANY_SITEMAP_PREFIX = "tech100_companies"
NEWS_SITEMAP_PREFIX = "news_items"

_company_entries_error = False
_news_entries_error = False

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


def _parse_lastmod(value) -> datetime | date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            try:
                return date.fromisoformat(text)
            except ValueError:
                return None
    return None


def _format_lastmod(value, *, date_only: bool = False) -> str | None:
    parsed = _parse_lastmod(value)
    if parsed is None:
        return None
    if isinstance(parsed, datetime):
        if date_only:
            return parsed.date().isoformat()
        normalized = parsed.replace(microsecond=0).astimezone(timezone.utc)
        return normalized.isoformat().replace("+00:00", "Z")
    return parsed.isoformat()


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
    candidates = []
    for entry in entries:
        parsed = _parse_lastmod(entry.get("lastmod"))
        if parsed is None:
            continue
        if isinstance(parsed, date) and not isinstance(parsed, datetime):
            parsed = datetime.combine(parsed, datetime.min.time(), tzinfo=timezone.utc)
        candidates.append(parsed)
    if not candidates:
        return None
    latest = max(candidates)
    return _format_lastmod(latest, date_only=True)


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
        "tech100_scores": "tech100.html",
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
}

_TECH100_TEMPLATE_MAP = {
    "tech100_scores": "tech100.html",
    "tech100_index": "tech100_index_overview.html",
    "tech100_company_root": "tech100_company_root.html",
    "tech100_performance": "tech100_performance.html",
    "tech100_constituents": "tech100_constituents.html",
    "tech100_attribution": "tech100_attribution.html",
    "tech100_stats": "tech100_stats.html",
    "tech100_methodology": "tech100_methodology.html",
}

_NEWS_TEMPLATE_MAP = {
    "news": "news.html",
}

_AI_REG_TEMPLATE_MAP = {
    "ai_regulation": "ai_reg/ai_regulation.html",
}

SITEMAP_SECTIONS = (
    ("static", _STATIC_TEMPLATE_MAP, "weekly", 0.7),
    ("tech100", _TECH100_TEMPLATE_MAP, "weekly", 0.7),
    ("news", _NEWS_TEMPLATE_MAP, "daily", 0.6),
    ("ai_regulation", _AI_REG_TEMPLATE_MAP, "weekly", 0.6),
)


def get_section_entries(section: str) -> list[dict[str, str]]:
    if section.startswith(f"{COMPANY_SITEMAP_PREFIX}_"):
        return _get_sharded_entries(section, COMPANY_SITEMAP_PREFIX, COMPANY_SITEMAP_CHUNK, _get_company_entries)
    if section.startswith(f"{NEWS_SITEMAP_PREFIX}_"):
        return _get_sharded_entries(section, NEWS_SITEMAP_PREFIX, NEWS_SITEMAP_CHUNK, _get_news_entries)
    for key, template_map, changefreq, priority in SITEMAP_SECTIONS:
        if key == section:
            entries = _build_entries(template_map, changefreq, priority)
            if key == "tech100":
                entries.extend(_get_company_url_entries())
            return entries
    return []


def get_section_lastmod(section: str) -> str | None:
    if section.startswith(f"{COMPANY_SITEMAP_PREFIX}_") or section.startswith(f"{NEWS_SITEMAP_PREFIX}_"):
        entries = get_section_entries(section)
        return _entries_lastmod(entries)
    entries = get_section_entries(section)
    return _entries_lastmod(entries)


def get_section_index_entries() -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for key, template_map, changefreq, priority in SITEMAP_SECTIONS:
        section_entries = _build_entries(template_map, changefreq, priority)
        if key == "tech100":
            section_entries = section_entries + _get_company_url_entries()
        if not section_entries:
            continue
        lastmod = _entries_lastmod(section_entries)
        entries.append(
            {
                "loc": _canonical_url(f"/sitemaps/{key}.xml"),
                "lastmod": lastmod or "",
            }
        )

    entries.extend(_build_shard_index_entries(NEWS_SITEMAP_PREFIX, NEWS_SITEMAP_CHUNK, _get_news_entries))
    return entries


def _build_shard_index_entries(
    prefix: str,
    chunk_size: int,
    loader,
) -> list[dict[str, str]]:
    shard_entries: list[dict[str, str]] = []
    entries = loader()
    if not entries:
        return shard_entries
    for index, chunk in enumerate(_chunk_entries(entries, chunk_size), start=1):
        if not chunk:
            continue
        lastmod = _entries_lastmod(chunk) or ""
        shard_entries.append(
            {
                "loc": _canonical_url(f"/sitemaps/{prefix}_{index}.xml"),
                "lastmod": lastmod,
            }
        )
    return shard_entries


def _chunk_entries(entries: list[dict[str, str]], chunk_size: int) -> list[list[dict[str, str]]]:
    if chunk_size <= 0:
        return [entries]
    return [entries[i : i + chunk_size] for i in range(0, len(entries), chunk_size)]


def _get_sharded_entries(
    section: str,
    prefix: str,
    chunk_size: int,
    loader,
) -> list[dict[str, str]]:
    try:
        index = int(section.replace(f"{prefix}_", "").strip())
    except (TypeError, ValueError):
        return []
    if index <= 0:
        return []
    entries = loader()
    chunks = _chunk_entries(entries, chunk_size)
    if index > len(chunks):
        return []
    return chunks[index - 1]


def _get_company_entries() -> list[dict[str, str]]:
    global _company_entries_error
    cache_key = "sitemap_company_entries"
    cached = cache.get(cache_key)
    if isinstance(cached, list):
        return cached
    try:
        items = tech100_company_data.get_company_sitemap_items()
    except Exception:
        _company_entries_error = True
        logger.exception("Failed to load Tech100 company sitemap items.")
        return []
    entries: list[dict[str, str]] = []
    for item in items:
        ticker = str(item.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        entry = {
            "loc": _canonical_url(f"/tech100/company/{ticker}/"),
            "changefreq": "daily",
            "priority": "0.9",
        }
        lastmod = _format_lastmod(item.get("lastmod"))
        if lastmod:
            entry["lastmod"] = lastmod
        entries.append(entry)
    _company_entries_error = False
    cache.set(cache_key, entries, 600)
    return entries


def _get_company_url_entries() -> list[dict[str, str]]:
    try:
        companies = tech100_company_data.get_company_list()
    except Exception:
        logger.exception("Failed to load Tech100 company list for sitemap.")
        return []
    if not companies:
        return []
    lastmod_map = {}
    try:
        for item in tech100_company_data.get_company_sitemap_items():
            ticker = str(item.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            lastmod = _format_lastmod(item.get("lastmod"))
            if lastmod:
                lastmod_map[ticker] = lastmod
    except Exception:
        logger.exception("Failed to load Tech100 company sitemap lastmod values.")
    entries: list[dict[str, str]] = []
    for company in companies:
        ticker = str(company.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        entry = {
            "loc": _canonical_url(f"/tech100/company/{ticker}/"),
            "changefreq": "daily",
            "priority": "0.9",
        }
        lastmod = lastmod_map.get(ticker)
        if lastmod:
            entry["lastmod"] = lastmod
        entries.append(entry)
    return entries


def _get_news_entries() -> list[dict[str, str]]:
    global _news_entries_error
    cache_key = "sitemap_news_entries"
    cached = cache.get(cache_key)
    if isinstance(cached, list):
        return cached
    try:
        items = news_data.fetch_news_sitemap_items()
    except Exception:
        _news_entries_error = True
        logger.exception("Failed to load news sitemap items.")
        return []
    entries: list[dict[str, str]] = []
    for item in items:
        news_id = str(item.get("news_id") or "").strip()
        if not news_id:
            continue
        entry = {
            "loc": _canonical_url(f"/news/{news_id}/"),
            "changefreq": "daily",
            "priority": "0.8",
        }
        lastmod = _format_lastmod(item.get("lastmod"))
        if lastmod:
            entry["lastmod"] = lastmod
        entries.append(entry)
    _news_entries_error = False
    cache.set(cache_key, entries, 600)
    return entries


SITEMAPS = {
    "static": StaticViewSitemap,
}

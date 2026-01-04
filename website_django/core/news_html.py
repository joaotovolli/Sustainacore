from __future__ import annotations

import re
from html import escape
from html.parser import HTMLParser
from typing import List, Optional
from urllib.parse import urlparse

_ALLOWED_TAGS = {
    "p",
    "br",
    "ul",
    "ol",
    "li",
    "strong",
    "em",
    "b",
    "i",
    "a",
    "blockquote",
    "h2",
    "h3",
    "h4",
    "table",
    "thead",
    "tbody",
    "tr",
    "th",
    "td",
    "img",
}

_VOID_TAGS = {"br", "img"}

_ALLOWED_ATTRS = {
    "a": {"href", "title"},
    "img": {"src", "alt", "title", "width", "height"},
    "td": {"colspan", "rowspan"},
    "th": {"colspan", "rowspan"},
}


def _is_safe_href(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme and parsed.scheme not in {"http", "https", "mailto"}:
        return False
    return True


def _is_safe_img_src(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.scheme:
        host = (parsed.hostname or "").lower()
        return bool(host) and host.endswith("sustainacore.org")
    return value.startswith("/news/assets/")


def _clean_attr_value(tag: str, key: str, value: str) -> Optional[str]:
    if tag == "a" and key == "href":
        return value if _is_safe_href(value) else None
    if tag == "img" and key == "src":
        return value if _is_safe_img_src(value) else None
    if key in {"width", "height", "colspan", "rowspan"}:
        return value if value.isdigit() else None
    return value


class _NewsHTMLSanitizer(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []
        self.open_tags: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if tag not in _ALLOWED_TAGS:
            return
        allowed_attrs = _ALLOWED_ATTRS.get(tag, set())
        cleaned_attrs: List[tuple[str, str]] = []
        attr_map = {key: value for key, value in attrs if value is not None}
        if tag == "img":
            src_val = attr_map.get("src")
            if not src_val or not _is_safe_img_src(src_val):
                return
        for key, value in attrs:
            if key not in allowed_attrs or value is None:
                continue
            cleaned_value = _clean_attr_value(tag, key, value)
            if cleaned_value is None:
                continue
            cleaned_attrs.append((key, cleaned_value))
        attrs_join = "".join(f' {k}="{escape(v, quote=True)}"' for k, v in cleaned_attrs)
        self.parts.append(f"<{tag}{attrs_join}>")
        if tag not in _VOID_TAGS:
            self.open_tags.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag not in _ALLOWED_TAGS or tag in _VOID_TAGS:
            return
        if tag in self.open_tags:
            self.parts.append(f"</{tag}>")
            for idx in range(len(self.open_tags) - 1, -1, -1):
                if self.open_tags[idx] == tag:
                    self.open_tags.pop(idx)
                    break

    def handle_data(self, data: str) -> None:
        self.parts.append(escape(data))

    def handle_entityref(self, name: str) -> None:
        self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.parts.append(f"&#{name};")

    def sanitized(self) -> str:
        while self.open_tags:
            tag = self.open_tags.pop()
            self.parts.append(f"</{tag}>")
        return "".join(self.parts)


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def handle_entityref(self, name: str) -> None:
        self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        self.parts.append(f"&#{name};")

    def text(self) -> str:
        return "".join(self.parts)


def sanitize_news_html(raw_html: str) -> str:
    parser = _NewsHTMLSanitizer()
    parser.feed(raw_html or "")
    parser.close()
    return parser.sanitized()


def render_news_html(raw_html: str) -> str:
    sanitized = sanitize_news_html(raw_html)
    if "<table" not in sanitized.lower():
        return sanitized
    return re.sub(
        r"(<table\b[\s\S]*?</table>)",
        r'<div class="news-detail__table-wrap">\1</div>',
        sanitized,
        flags=re.IGNORECASE,
    )


def summarize_html(raw_html: str, max_len: int = 400) -> str:
    parser = _TextExtractor()
    parser.feed(raw_html or "")
    parser.close()
    text = " ".join(parser.text().split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"

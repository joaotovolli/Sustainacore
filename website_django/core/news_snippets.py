from __future__ import annotations

import re
from html.parser import HTMLParser


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def text(self) -> str:
        return "".join(self.parts)


def _strip_html(value: str) -> str:
    parser = _HTMLStripper()
    parser.feed(value)
    parser.close()
    return parser.text()


def build_news_snippet(raw_text: str | None, max_len: int = 260) -> str:
    if not raw_text:
        return ""
    stripped = _strip_html(str(raw_text))
    normalized = re.sub(r"\s+", " ", stripped).strip()
    if not normalized:
        return ""
    if len(normalized) <= max_len:
        return normalized
    cut = normalized[:max_len].rstrip()
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return f"{cut}..."

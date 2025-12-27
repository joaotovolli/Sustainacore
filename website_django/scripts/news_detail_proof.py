from __future__ import annotations

import argparse
import re
import sys
from html.parser import HTMLParser
from urllib.request import urlopen


class _BodyTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.capture = False
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "div" and any(name == "class" and value and "news-detail__body" in value for name, value in attrs):
            self.capture = True
            return
        if self.capture and tag in {"p", "br", "li"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self.capture and tag == "div":
            self.capture = False

    def handle_data(self, data: str) -> None:
        if self.capture:
            self.parts.append(data)

    def text(self) -> str:
        return "".join(self.parts).strip()


def _fetch(url: str) -> str:
    with urlopen(url, timeout=8) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _extract_debug_len(html: str) -> int | None:
    match = re.search(r"news_debug[^>]*len=(\d+)", html)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000/news/ESG_NEWS:738/")
    parser.add_argument("--min-chars", type=int, default=1500)
    args = parser.parse_args()

    html = _fetch(args.url)
    extractor = _BodyTextExtractor()
    extractor.feed(html)
    body_text = extractor.text()
    rendered_len = len(body_text)
    debug_len = _extract_debug_len(html)
    print(f"rendered_body_chars={rendered_len}")
    if debug_len is not None:
        print(f"debug_body_len={debug_len}")

    if rendered_len < args.min_chars and (debug_len or 0) >= args.min_chars:
        print("Rendered body is shorter than expected.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

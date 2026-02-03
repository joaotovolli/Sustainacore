from __future__ import annotations

import re
from typing import Iterable, Optional

DEFAULT_BOT_UA_REGEX = (
    r"bot|spider|crawler|crawl|scrape|scanner|headless|playwright|"
    r"python-requests|curl/|wget|postman|httpclient|http-client|aiohttp|"
    r"libwww|java/|go-http-client|okhttp|insomnia|codex"
)

DEFAULT_PROBE_PATH_REGEX = (
    r"/\.env|/\.git|/phpinfo|/wp-admin|/wp-login|/cgi-bin|/vendor/|/server-status|"
    r"/actuator|/\.well-known/|/\.aws|/\.ssh|/\.svn|/config\.php|/env\b"
)


def _compile(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE)


def is_bot_user_agent(user_agent: Optional[str], *, extra_tokens: Optional[Iterable[str]] = None) -> bool:
    ua = (user_agent or "").strip()
    if not ua:
        return False
    if _compile(DEFAULT_BOT_UA_REGEX).search(ua):
        return True
    if extra_tokens:
        lowered = ua.lower()
        for token in extra_tokens:
            if token and token.lower() in lowered:
                return True
    return False


def is_probe_path(path: Optional[str], *, extra_regex: Optional[str] = None) -> bool:
    value = (path or "").strip()
    if not value:
        return False
    if _compile(DEFAULT_PROBE_PATH_REGEX).search(value):
        return True
    if extra_regex and _compile(extra_regex).search(value):
        return True
    return False

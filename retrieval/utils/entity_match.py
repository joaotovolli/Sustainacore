"""Entity normalization and fuzzy matching helpers for /ask2."""
from __future__ import annotations

import re

_QUESTION_PREFIXES = {
    "is",
    "are",
    "was",
    "were",
    "do",
    "does",
    "did",
    "can",
    "could",
    "should",
    "would",
    "will",
    "the",
    "a",
    "an",
}

_CORP_SUFFIXES = {
    "corporation",
    "corp",
    "corp.",
    "inc",
    "inc.",
    "company",
    "co",
    "co.",
    "limited",
    "ltd",
    "ltd.",
    "llc",
    "plc",
    "nv",
    "sa",
    "ag",
}

_PUNCT_RE = re.compile(r"[^0-9A-Za-z&'./-]+")


def _canonicalize_question_entity(raw: str) -> str:
    if not raw:
        return ""
    value = raw.strip().rstrip("?!.")
    tokens = re.split(r"\s+", value)
    while tokens and tokens[0].lower().strip(".,") in _QUESTION_PREFIXES:
        tokens.pop(0)
    return " ".join(tokens).strip()


def _normalize_company_name(raw: str) -> str:
    if not raw:
        return ""
    text = raw.strip()
    text = text.replace("“", "\"").replace("”", "\"").replace("’", "'")
    text = _PUNCT_RE.sub(" ", text)
    tokens = [tok.strip(".").lower() for tok in text.split() if tok]
    while tokens and tokens[-1] in _CORP_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def _fuzzy_contains(haystack: str, needle: str) -> bool:
    if not haystack or not needle:
        return False
    hay_norm = _normalize_company_name(haystack)
    needle_norm = _normalize_company_name(needle)
    if not hay_norm or not needle_norm:
        return False
    if f" {needle_norm} " in f" {hay_norm} ":
        return True
    hay_tokens = set(hay_norm.split())
    needle_tokens = needle_norm.split()
    if not needle_tokens:
        return False
    if set(needle_tokens).issubset(hay_tokens):
        return True
    return False

__all__ = [
    "_canonicalize_question_entity",
    "_normalize_company_name",
    "_fuzzy_contains",
]

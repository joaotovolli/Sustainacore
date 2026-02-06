"""Deterministic quality guards for Ask2.

These guards exist to prevent obviously low-information prompts (greetings,
thanks, gibberish) from triggering retrieval and producing irrelevant answers.
They also provide a minimal abstention path when retrieval confidence is low.

Keep this module cheap: pure-Python heuristics over a tiny number of contexts.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


_WORD_RE = re.compile(r"[A-Za-z0-9]{2,}")
_VOWEL_RE = re.compile(r"[aeiou]", re.IGNORECASE)

_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "can",
    "do",
    "does",
    "for",
    "from",
    "get",
    "give",
    "help",
    "how",
    "i",
    "in",
    "is",
    "it",
    "latest",
    "me",
    "my",
    "of",
    "on",
    "please",
    "tell",
    "thanks",
    "thank",
    "that",
    "the",
    "this",
    "to",
    "us",
    "what",
    "when",
    "where",
    "who",
    "why",
    "with",
    "you",
    "your",
}

_GREETINGS = {
    "hi",
    "hello",
    "hey",
    "yo",
    "hiya",
    "morning",
    "afternoon",
    "evening",
}

_THANKS = {
    "thanks",
    "thank",
    "ty",
}


def _tokens(text: str) -> List[str]:
    return [m.group(0).lower() for m in _WORD_RE.finditer(text or "")]


def _content_tokens(text: str) -> List[str]:
    toks = _tokens(text)
    return [t for t in toks if t not in _STOPWORDS and len(t) >= 3]


def _looks_like_gibberish(token: str) -> bool:
    # A long token with no vowels is usually not a meaningful query term.
    # Keep this conservative so we don't misclassify real acronyms (e.g., "EU").
    if len(token) < 7:
        return False
    if token.isdigit():
        return False
    # Keyboard mashing often has very low character variety (e.g., "asdasdasd").
    if len(token) >= 8 and len(set(token.lower())) <= 3:
        return True
    if _VOWEL_RE.search(token):
        return False
    return True


def is_greeting_or_thanks(question: str) -> bool:
    text = (question or "").strip().lower()
    if not text:
        return False

    toks = _tokens(text)
    if not toks:
        return True

    # Single-token greetings/thanks, or two-token variants like "good morning".
    if len(toks) <= 2 and all(t in _GREETINGS or t in _THANKS or t == "good" for t in toks):
        return True

    if "thank you" in text or "thx" in text:
        return True

    return False


def is_low_information(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return True

    content = _content_tokens(text)
    if not content:
        return True

    # One long vowel-less token tends to be random keyboard mashing (e.g. asdasdasd).
    if len(content) == 1 and _looks_like_gibberish(content[0]):
        return True

    # Extremely short prompts are usually not answerable without clarification.
    if len(content) <= 1 and len(text) <= 8:
        return True

    return False


def smalltalk_answer(question: str) -> str:
    # Keep this consistent with the {answer,sources,meta} contract (answer body only).
    return (
        "**Answer**\n"
        "Hi. I can help with Sustainacore questions about Tech100, AI regulation, news, and index performance.\n\n"
        "**Try asking**\n"
        "- How is the Tech100 index built?\n"
        "- What is the status of the EU AI Act?\n"
        "- Summarize the latest Sustainacore news about AI regulation.\n"
    )


def clarify_answer(question: str) -> str:
    # For gibberish / low-information prompts that are not simple greetings.
    return (
        "**Answer**\n"
        "I couldn’t find enough information to answer that as written.\n\n"
        "**Try**\n"
        "- Adding a company ticker (e.g., “AAPL”) or “Tech100”.\n"
        "- Asking about a specific jurisdiction or regulation (e.g., “EU AI Act”, “Brazil AI regulation”).\n"
        "- Asking about a specific Sustainacore page (news, performance, methodology).\n"
    )


def infer_source_type_filters(question: str) -> Optional[List[str]]:
    """Return a conservative SOURCE_TYPE filter hint for Oracle retrieval."""

    q = (question or "").lower().strip()
    if not q:
        return None

    if any(tok in q for tok in ("ai act", "regulation", "jurisdiction", "milestone", "instrument snapshots")):
        return ["regulatory"]

    if any(tok in q for tok in ("news", "press", "release", "headline")):
        return ["news_release"]

    if any(tok in q for tok in ("performance", "returns", "return", "drawdown", "volatility")):
        return ["performance"]

    # Generic company questions: bias toward company pages instead of large regulatory corpora.
    if any(tok in q for tok in ("tell me about", "what does", "who is", "company", "ticker")):
        return ["company_profile"]

    return None


def _context_text(ctx: Dict[str, Any]) -> str:
    parts = [
        str(ctx.get("title") or ""),
        str(ctx.get("source_url") or ""),
        str(ctx.get("chunk_text") or ctx.get("snippet") or ""),
    ]
    return " ".join(p for p in parts if p).strip()


def _best_score(contexts: Sequence[Dict[str, Any]]) -> Optional[float]:
    for ctx in contexts or []:
        try:
            score = float(ctx.get("score"))
        except Exception:
            score = None
        if score is not None:
            return score
    return None


def _max_token_overlap(question: str, contexts: Sequence[Dict[str, Any]]) -> int:
    q_tokens = set(_content_tokens(question))
    if not q_tokens:
        return 0
    best = 0
    for ctx in contexts[:3]:
        if not isinstance(ctx, dict):
            continue
        ctx_tokens = set(_content_tokens(_context_text(ctx)))
        best = max(best, len(q_tokens & ctx_tokens))
    return best


@dataclass(frozen=True)
class AbstainDecision:
    abstain: bool
    reason: str
    best_score: Optional[float] = None
    max_token_overlap: int = 0


def should_abstain(question: str, contexts: Sequence[Dict[str, Any]]) -> AbstainDecision:
    """Decide whether to abstain based on low-confidence retrieval."""

    best = _best_score(contexts)
    overlap = _max_token_overlap(question, contexts)

    # If we don't have scores, fall back to overlap only.
    if best is None:
        if overlap == 0 and not is_low_information(question):
            return AbstainDecision(True, "no_overlap_no_score", None, overlap)
        return AbstainDecision(False, "no_score", None, overlap)

    # Hard floor: very low score is almost certainly irrelevant.
    if best < 0.20:
        return AbstainDecision(True, "best_score_too_low", best, overlap)

    # If lexical overlap is zero and score is only mediocre, prefer abstention for safety.
    if overlap == 0 and best < 0.45 and not is_low_information(question):
        return AbstainDecision(True, "low_overlap_mediocre_score", best, overlap)

    # For low-information prompts, we should already have bypassed upstream.
    return AbstainDecision(False, "ok", best, overlap)

# ask2_llm_orchestrator.py (Ollama "micro" edition)
import io, os, re, json, time, urllib.request
from typing import List, Dict, Any

def _env(n, d=None): 
    v = os.environ.get(n)
    return v if v is not None and v != "" else d

def _post_json(url: str, payload: dict, timeout=120):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _ollama_chat(messages, *, json_mode=True):
    base = _env("OLLAMA_URL", "http://127.0.0.1:11434")
    model = _env("SCAI_OLLAMA_MODEL", "tinyllama")
    num_ctx     = int(_env("SCAI_OLLAMA_NUM_CTX", "512"))
    num_predict = int(_env("SCAI_OLLAMA_NUM_PREDICT", "64"))
    num_thread  = int(_env("SCAI_OLLAMA_NUM_THREAD", "1"))
    num_batch   = int(_env("SCAI_OLLAMA_NUM_BATCH", "8"))

    sys_content = None
    user_messages = []
    for m in messages:
        if m.get("role") == "system":
            sys_content = m["content"]
        else:
            user_messages.append(m)
    prompt_msgs = []
    if sys_content:
        prompt_msgs.append({"role":"system","content":sys_content})
    prompt_msgs.extend(user_messages)

    body = {
        "model": model,
        "messages": prompt_msgs,
        "stream": False,
        "options": {
            "num_ctx": num_ctx,
            "num_predict": num_predict,
            "num_thread": num_thread,
            "num_batch": num_batch
        }
    }
    out = _post_json(f"{base}/api/chat", body, timeout=180)
    content = out.get("message", {}).get("content","").strip()
    if json_mode:
        try: return json.loads(content)
        except Exception: return {"answer": content}
    return {"answer": content}

_MEM = {}
def _mem_key(env): return f"{env.get('HTTP_X_FORWARDED_FOR') or env.get('REMOTE_ADDR') or ''}|{env.get('HTTP_USER_AGENT') or ''}"
def _mem_get(env): 
    r = _MEM.get(_mem_key(env), {})
    if r and time.time()-r.get('ts',0) > 600: r = {}
    return r or {}
def _mem_set(env, entity=None, intent=None):
    k = _mem_key(env); r = _MEM.get(k, {"ts": time.time()})
    if entity: r["entity"] = entity
    if intent: r["intent"] = intent
    r["ts"] = time.time(); _MEM[k] = r

GREETING_RE = re.compile(r"^\s*(hi|hello|hey|hola|ol[aá]|oi|howdy|yo)[\s!?.]*$", re.I)
META_RE     = re.compile(r"(who are you|what (is|are) (this|sustaina?core)|what can you do|help|how to use)", re.I)
BRACKET_REF = re.compile(r"\[(?:S|s)\d+\]")
INLINE_NUM  = re.compile(r"\[\d+\]")

def _is_greeting(q): return bool(GREETING_RE.match(q or ""))
def _is_meta(q): return bool(META_RE.search(q or ""))

def _json_or_none(b: bytes):
    try: return json.loads(b.decode("utf-8"))
    except Exception: return None

def _dedup_sources(sources, limit=3, registry=None):
    seen, out = set(), []
    for s in sources or []:
        title, url = "", ""
        if isinstance(s, dict):
            title = (s.get("title") or s.get("name") or s.get("id") or "").strip()
            url   = (s.get("url") or s.get("link") or "").strip()
        elif isinstance(s, str):
            title = s.strip()
        if registry and title in registry:
            m = registry[title]; title = m.get("title", title); url = m.get("url", url)
        key = (title.lower(), url.lower())
        if key in seen: continue
        seen.add(key); out.append({"title": title or (url or "Source"), "url": url})
        if len(out) >= limit: break
    return out

def _extract_evidence(payload):
    ev = {"snippets": [], "sources": []}
    for k in ("snippets","chunks","evidence","fragments","items"):
        v = payload.get(k)
        if isinstance(v, list):
            for it in v:
                if isinstance(it, dict) and it.get("text"): ev["snippets"].append(str(it["text"]))
                elif isinstance(it, dict) and it.get("snippet"): ev["snippets"].append(str(it["snippet"]))
                elif isinstance(it, dict) and it.get("source"): ev["sources"].append({"title": it.get("source"), "url": it.get("url","")})
                elif isinstance(it, str): ev["snippets"].append(it)
    if isinstance(payload.get("sources"), list):
        for s in payload["sources"]:
            if isinstance(s, dict): ev["sources"].append({"title": s.get("title") or s.get("name") or "", "url": s.get("url") or ""})
            elif isinstance(s, str): ev["sources"].append({"title": s, "url": ""})
    return ev

def _prioritize_snippets(snips, intent):
    ranked = []
    for s in snips or []:
        ss = (s or "").strip()
        if not ss or ss.startswith("- [S") or BRACKET_REF.search(ss): continue
        score = 0; low = ss.lower()
        if "tech100" in low: score += 3
        if any(w in low for w in ("membership","included","constituent","part of")): score += 3
        if "rank" in low: score += 2
        if any(w in low for w in ("equal-weight","portfolio")): score += 1
        if any(w in low for w in ("founded","headquartered","company profile")): score -= 2
        ranked.append((score, ss))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [t for _, t in ranked[:10]]

def _strip_inline_refs(text: str) -> str:
    text = BRACKET_REF.sub("", text)
    text = INLINE_NUM.sub("", text)
    return re.sub(r"\s{2,}", " ", text).strip()

_JUNK_PREFIXES = (
    "te.",
    "sibility.",
    "key sco",
    "e investment advice",
    "meta:",
)

def _normalize_punctuation(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = text.replace("•", " ")
    cleaned = re.sub(r"\s*\.\s*\.\s*", ". ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s+([.,;:])", r"\1", cleaned)
    cleaned = re.sub(r"([.,;:])([^\s])", r"\1 \2", cleaned)
    cleaned = re.sub(r"([A-Za-z0-9])\.\s+(com|org|net|io|ai|gov|edu|co|us|uk|info|biz)\b", r"\1.\2", cleaned, flags=re.I)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()

def _looks_fragmentary(text: str) -> bool:
    if not isinstance(text, str):
        return True
    stripped = text.strip()
    if not stripped:
        return True
    lowered = stripped.lower()
    if any(lowered.startswith(prefix) for prefix in _JUNK_PREFIXES):
        return True
    if "card_path=" in lowered or "doc_ref=" in lowered or "aliases=" in lowered:
        return True
    if len(stripped) < 30 and not _has_numeric_fact(stripped):
        return True
    if stripped.lower().startswith(("by ", "and ", "or ", "as ", "to ", "of ")):
        return True
    if stripped[0].islower() and len(stripped) < 80:
        return True
    if ". ." in stripped or ".." in stripped:
        return True
    if "investment advice" in lowered and len(stripped) < 80:
        return True
    return False

def _sanitize_snippet(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = _normalize_punctuation(_strip_inline_refs(text))
    cleaned = cleaned.strip(" \"'“” .;:-")
    if _looks_fragmentary(cleaned):
        return ""
    return cleaned

def _normalize_sentence(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()

def _clean_fact_sentence(text: str) -> str:
    cleaned = _normalize_punctuation(text)
    cleaned = re.sub(r"^Definition\s*[-–:]\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^As noted,\s*", "", cleaned, flags=re.I)
    if cleaned.lower().startswith("meta:"):
        return ""
    if ">" in cleaned or "›" in cleaned:
        return ""
    cleaned = cleaned.strip()
    if _looks_fragmentary(cleaned):
        return ""
    if not re.search(
        r"\b(is|are|was|were|has|have|measures|measured|ranks|ranked|includes|included|contains|refreshes|rebalances|receives|receive|derived|derives|forms|form|designed|tracks|targets|uses|built|based)\b",
        cleaned,
        re.I,
    ) and not _has_numeric_fact(cleaned):
        return ""
    return cleaned


def _has_numeric_fact(text: str) -> bool:
    return bool(re.search(r"\b\d+(?:\.\d+)?%?\b", text))

def _extract_fact_sentences(snippet: str) -> List[str]:
    cleaned = _sanitize_snippet(snippet)
    if not cleaned:
        return []
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", cleaned)
    out = []
    for sentence in sentences:
        fact = _clean_fact_sentence(sentence)
        if fact:
            out.append(fact)
    return out


def _keyword_sentences(snippet: str, keywords: List[str]) -> List[str]:
    if not keywords or not isinstance(snippet, str):
        return []
    cleaned = _normalize_punctuation(_strip_inline_refs(snippet))
    text = cleaned or snippet
    out = []
    lowered_keywords = [kw.lower() for kw in keywords if kw]
    for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", text):
        lowered = sentence.lower()
        if any(kw in lowered for kw in lowered_keywords):
            candidate = sentence.strip()
            if candidate.lower().startswith("meta:"):
                continue
            if "card_path=" in candidate.lower() or "doc_ref=" in candidate.lower() or "aliases=" in candidate.lower():
                continue
            if candidate and len(candidate) >= 20:
                out.append(candidate)
    return out

def _score_fact(sentence: str) -> int:
    score = 0
    lowered = sentence.lower()
    if _has_numeric_fact(sentence):
        score += 2
    if any(term in lowered for term in ("rank", "weight", "rebalance", "quarter", "annual", "score", "index")):
        score += 2
    if any(term in lowered for term in ("includes", "included", "contains", "comprises")):
        score += 1
    return score

def _question_keywords(question: str) -> List[str]:
    if not isinstance(question, str):
        return []
    tokens = re.findall(r"[A-Za-z0-9]+", question.lower())
    stop = {
        "is",
        "are",
        "the",
        "a",
        "an",
        "of",
        "in",
        "on",
        "and",
        "or",
        "to",
        "for",
        "does",
        "do",
        "did",
        "what",
        "how",
        "who",
        "when",
        "where",
        "why",
        "show",
        "me",
        "summarize",
        "latest",
        "by",
    }
    return [t for t in tokens if t not in stop and len(t) > 2]


_GENERIC_QUERY_TERMS = {
    "tech100",
    "index",
    "score",
    "ai",
    "governance",
    "ethics",
    "sustainacore",
    "latest",
    "headlines",
    "headline",
    "news",
    "top",
    "company",
    "companies",
}

def _select_key_facts(snips, max_items=3, keywords=None):
    candidates = []
    for snippet in snips or []:
        for sentence in _extract_fact_sentences(snippet):
            candidates.append((_score_fact(sentence), sentence))
        for sentence in _keyword_sentences(snippet, keywords or []):
            candidates.append((_score_fact(sentence) + 1, sentence))
    candidates.sort(key=lambda x: x[0], reverse=True)
    keywords = [kw for kw in (keywords or []) if kw]
    preferred = []
    if keywords:
        for score, sentence in candidates:
            lowered = sentence.lower()
            if any(kw in lowered for kw in keywords):
                preferred.append((score + 2, sentence))
    ordered = preferred if preferred else candidates
    out = []
    seen = set()
    for _, sentence in ordered:
        key = _normalize_sentence(sentence)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(sentence)
        if len(out) >= max_items:
            break
    return out

def _dedupe_sentences(items, seen=None):
    if seen is None:
        seen = set()
    out = []
    for item in items or []:
        key = _normalize_sentence(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out, seen

def _ownership_intent(question: str) -> bool:
    lowered = (question or "").lower()
    return any(term in lowered for term in ("own", "owner", "created", "creator", "founded", "admin"))

def _news_intent(question: str) -> bool:
    lowered = (question or "").lower()
    return any(term in lowered for term in ("news", "headline", "headlines", "latest"))

def _ranking_intent(question: str) -> bool:
    lowered = (question or "").lower()
    return any(term in lowered for term in ("top companies", "top company", "highest", "ranked", "ranking", "top by"))

def _rank_sentences(snips):
    sentences = []
    for snippet in snips or []:
        if not isinstance(snippet, str):
            continue
        cleaned = _normalize_punctuation(_strip_inline_refs(snippet))
        for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", cleaned):
            if not re.search(r"\branks?\b", sentence, re.I):
                continue
            if not _has_numeric_fact(sentence):
                continue
            if not re.search(r"\b[A-Z][a-z]", sentence):
                continue
            if _looks_fragmentary(sentence):
                continue
            sentences.append(sentence.strip())
    return sentences

def _filter_news_citations(citations):
    news = []
    for item in citations or []:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or item.get("name") or "").lower()
        if "news" in title or "headline" in title:
            news.append(item)
    return news

def _has_ownership_statement(snips):
    markers = ("owned by", "owner", "owned", "created by", "creator", "founded by", "founded", "operated by", "run by", "administered by")
    for snippet in snips or []:
        cleaned = _sanitize_snippet(snippet)
        lowered = cleaned.lower()
        if any(marker in lowered for marker in markers):
            return True
    return False

def _postprocess_formatted(text: str) -> str:
    if not isinstance(text, str):
        return ""
    out = re.sub(r"\s*•\s*", "\n- ", text)
    out = re.sub(r"\n{3,}", "\n\n", out)
    out = out.replace("\r\n", "\n").strip()
    out = out.replace("...", "")
    out = re.sub(r"\n\*\*Key facts\*\*", "\n\n**Key facts**", out)
    out = re.sub(r"\n\*\*Sources\*\*", "\n\n**Sources**", out)
    if "**Sources**" not in out and re.search(r"^\d+\\. ", out, re.M):
        out = re.sub(r"\n+(\\d+\\.)", "\n\n**Sources**\n\\1", out, count=1)
    return out

def _slugify_title(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return cleaned or "source"

def _resolve_source_url(title: str, url: str) -> str:
    if isinstance(url, str) and url.strip():
        try:
            from urllib.parse import urlparse
        except Exception:
            urlparse = None
        if urlparse:
            try:
                parsed = urlparse(url.strip())
            except Exception:
                parsed = None
            if parsed and parsed.scheme in {"http", "https"} and parsed.netloc:
                host = (parsed.hostname or "").lower()
                if host.endswith("sustainacore.org"):
                    path = parsed.path or "/"
                    return f"https://sustainacore.org{path}"
    lower_title = (title or "").lower()
    if "contact" in lower_title:
        return "https://sustainacore.org/contact"
    if "press" in lower_title:
        return "https://sustainacore.org/press"
    if "news" in lower_title or "headline" in lower_title:
        return "https://sustainacore.org/news"
    if "constituent" in lower_title:
        return "https://sustainacore.org/tech100/constituents"
    if "performance" in lower_title:
        return "https://sustainacore.org/tech100/performance"
    if "stats" in lower_title or "score" in lower_title or "metrics" in lower_title:
        return "https://sustainacore.org/tech100/stats"
    if "tech100" in lower_title or "index" in lower_title or "methodology" in lower_title:
        return "https://sustainacore.org/tech100"
    if "about" in lower_title or "sustainacore" in lower_title:
        return "https://sustainacore.org"
    return ""

def _build_sources_list(citations, max_items=3):
    sources = []
    seen = set()
    for item in citations or []:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or item.get("name") or "").strip()
        if ">" in title:
            title = title.split(">")[-1].strip()
        if "›" in title:
            title = title.split("›")[-1].strip()
        url = (item.get("url") or item.get("link") or "").strip()
        if not title and not url:
            continue
        if url.startswith(("local://", "file://", "internal://")):
            url = ""
        url = _resolve_source_url(title, url)
        if not url:
            continue
        key = f"{title.lower()}|{url.lower()}"
        if key in seen:
            continue
        seen.add(key)
        label = f"{title} — {url}" if title else url
        sources.append(label)
        if len(sources) >= max_items:
            break
    return sources

def _ensure_period(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    cleaned = text.strip()
    if cleaned[-1] not in ".!?":
        cleaned += "."
    return cleaned

def _fit_content(answer_lines, key_fact_lines, max_len):
    def _joined(lines):
        return "\n".join(lines).strip()
    if max_len <= 0:
        return _joined(answer_lines + key_fact_lines)
    header = key_fact_lines[:1]
    bullets = key_fact_lines[1:]
    key_block = _joined(header + bullets)
    while bullets and len(key_block) > max_len:
        bullets = bullets[:-1]
        key_block = _joined(header + bullets)
    if not key_block or len(key_block) > max_len:
        key_block = _joined(
            [
                "**Key facts**",
                "- See the Sources section for details.",
            ]
        )
    remaining = max_len - len(key_block) - 4
    answer_block = _joined(answer_lines)
    if remaining <= 0:
        answer_block = "**Answer**\nSee the Sources section for the retrieved references."
    elif len(answer_block) > remaining:
        answer_block = answer_block[:remaining].rstrip()
    return _joined([answer_block, "", key_block])

def _strip_section_markers(text: str) -> str:
    if not isinstance(text, str):
        return ""
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        lowered = line.lower().strip("*").strip()
        if lowered in {"answer", "key facts (from sustainacore)", "key facts", "evidence", "sources"}:
            continue
        if lowered.startswith("key facts"):
            continue
        if lowered.startswith("evidence"):
            continue
        lines.append(raw)
    return "\n".join(lines).strip()

def _normalize_bullet_runs(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""
    if text.count(" - ") >= 2:
        return text.replace(" - ", "\n- ")
    return text

def _split_paragraphs(text: str):
    if not isinstance(text, str) or not text.strip():
        return []
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    return paragraphs or [text.strip()]

def _first_sentence(text: str, max_len=240):
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = re.sub(r"\s+", " ", text.strip())
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", collapsed)
    sentence = parts[0].strip() if parts else collapsed
    if len(sentence) > max_len:
        cut = max(sentence.rfind(".", 0, max_len), sentence.rfind("!", 0, max_len), sentence.rfind("?", 0, max_len))
        sentence = sentence[: cut + 1].rstrip() if cut > 0 else sentence[:max_len].rstrip()
    if sentence and sentence[-1] not in ".!?":
        sentence += "."
    return sentence

def _shorten_snippet(text: str, max_len=160):
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = re.sub(r"\s+", " ", text.strip())
    if len(collapsed) <= max_len:
        return collapsed
    cut = max(collapsed.rfind(".", 0, max_len), collapsed.rfind("!", 0, max_len), collapsed.rfind("?", 0, max_len))
    if cut > 0:
        return collapsed[: cut + 1].rstrip()
    return collapsed[:max_len].rstrip()

def _build_key_facts_from_snippets(snips, max_bullets=5, question: str = ""):
    keywords = _question_keywords(question)
    focus_terms = [kw for kw in keywords if kw not in _GENERIC_QUERY_TERMS]
    bullets = _select_key_facts(snips, max_items=max_bullets, keywords=focus_terms or keywords)
    if bullets:
        return bullets
    fallback = []
    seen = set()
    for snippet in snips or []:
        sentence = _first_sentence(_sanitize_snippet(snippet))
        sentence = _clean_fact_sentence(sentence) if sentence else ""
        if not sentence:
            continue
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        fallback.append(sentence)
        if len(fallback) >= max_bullets:
            break
    return fallback

def _build_sources_list_or_fallback(citations, snips):
    sources = _build_sources_list(citations)
    if sources:
        return sources
    return ["SustainaCore — https://sustainacore.org"]

def _format_structured_answer(question, answer_text, snips, citations):
    cleaned = _normalize_bullet_runs(_strip_section_markers(_strip_inline_refs(answer_text or "")))
    paragraphs = _split_paragraphs(cleaned)
    if not paragraphs:
        paragraphs = []
    if not paragraphs and snips:
        seed_facts = _select_key_facts(snips, max_items=2)
        if seed_facts:
            paragraphs = [f"Based on SustainaCore sources, {seed_facts[0].rstrip('.')}.",
                          f"In addition, {seed_facts[1].rstrip('.')}."] if len(seed_facts) > 1 else [
                f"Based on SustainaCore sources, {seed_facts[0].rstrip('.')}.",
            ]
    if not paragraphs:
        paragraphs = ["I could not find enough SustainaCore context to answer this question."]

    if len(snips or []) < 2:
        paragraphs.append("Coverage looks thin based on the retrieved snippets.")
    if len(paragraphs) > 2:
        paragraphs = paragraphs[:2]

    key_facts = _build_key_facts_from_snippets(snips, max_bullets=4, question=question)
    ranked_sentences = _rank_sentences(snips) if _ranking_intent(question) else []
    if ranked_sentences:
        key_facts = ranked_sentences[:3]
    if not key_facts:
        key_facts = ["No high-confidence facts were retrieved for this query."]
    if (
        paragraphs
        and paragraphs[0].startswith("I could not find enough SustainaCore context")
        and key_facts
        and not key_facts[0].startswith("No high-confidence")
    ):
        paragraphs = [f"Based on SustainaCore sources, {key_facts[0].rstrip('.')}."] + (
            [f"In addition, {key_facts[1].rstrip('.')}."] if len(key_facts) > 1 else []
        )
    if _ranking_intent(question) and ranked_sentences:
        paragraphs = [
            "Here are the top companies by AI Governance & Ethics Score mentioned in the retrieved snippets."
        ]

    news_sources = _filter_news_citations(citations) if _news_intent(question) else []
    sources = _build_sources_list_or_fallback(news_sources or citations, snips)

    seen = set()
    paragraphs, seen = _dedupe_sentences(paragraphs, seen)
    key_facts, seen = _dedupe_sentences(key_facts, seen)
    if len(key_facts) < 3:
        extras = _build_key_facts_from_snippets(snips, question=question)
        for item in extras:
            if item in key_facts:
                continue
            key_facts.append(item)
            if len(key_facts) >= 3:
                break
    if len(key_facts) < 3:
        for snippet in snips or []:
            fact = _first_sentence(_sanitize_snippet(snippet))
            if not fact or _normalize_sentence(fact) in {_normalize_sentence(k) for k in key_facts}:
                continue
            key_facts.append(fact)
            if len(key_facts) >= 3:
                break
    if len(key_facts) < 2:
        key_facts.append("The retrieved snippets do not include a ranked company list for this question.")
    sources_filtered = []
    for item in sources:
        if _normalize_sentence(item) in seen:
            continue
        sources_filtered.append(item)
    sources = sources_filtered or sources

    if _ownership_intent(question) and not _has_ownership_statement(snips):
        paragraphs = [
            "SustainaCore pages do not explicitly state the owner or creator of sustainacore.org.",
            "For administrative details, refer to the SustainaCore About or Contact page.",
        ]
        key_facts = _build_key_facts_from_snippets(snips, question=question)
        if not key_facts:
            key_facts = ["The About and Contact pages are the only relevant sources in the retrieved results."]
        if len(key_facts) < 3:
            for snippet in snips or []:
                cleaned = _sanitize_snippet(snippet)
                for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", cleaned):
                    sentence = sentence.strip()
                    if not sentence:
                        continue
                    if _normalize_sentence(sentence) in {_normalize_sentence(k) for k in key_facts}:
                        continue
                    key_facts.append(sentence)
                    if len(key_facts) >= 3:
                        break
                if len(key_facts) >= 3:
                    break
        sources = _build_sources_list_or_fallback(citations, snips)

    key_facts = [_ensure_period(item) for item in key_facts if item]
    if _news_intent(question) and not _filter_news_citations(citations):
        paragraphs = [
            "I couldn’t find recent headlines in the indexed news set right now.",
            "Here are the most relevant governance items I found instead.",
        ]
    answer_lines = ["**Answer**"]
    for idx, para in enumerate(paragraphs):
        answer_lines.append(para)
        if idx < len(paragraphs) - 1:
            answer_lines.append("")
    key_fact_lines = ["**Key facts**"] + [f"- {item}" for item in key_facts[:4]]
    source_lines = ["**Sources**"] + [f"{idx}. {item}" for idx, item in enumerate(sources[:3], start=1)]

    cap = int(os.getenv("ASK2_ANSWER_CHAR_CAP", "2000"))
    if cap > 0:
        sources_text = "\n".join(source_lines).strip()
        remaining = max(cap - len(sources_text) - 2, 0)
        content = _fit_content(answer_lines, key_fact_lines, remaining)
        if not content:
            content = "\n".join(
                [
                    "**Answer**",
                    "See the Sources section for the retrieved references.",
                    "",
                    "**Key facts**",
                    "- See the Sources section for details.",
                ]
            )
        combined = "\n".join([content, "", sources_text]).strip()
    else:
        combined = "\n".join(answer_lines + [""] + key_fact_lines + [""] + source_lines).strip()
    return _postprocess_formatted(combined)

def _chat(messages, *, json_mode=True): 
    return _ollama_chat(messages, json_mode=json_mode)

class Ask2LLMOrchestratorMiddleware:
    def __init__(self, app):
        self.app = app
        self.strict = _env("SCAI_LLM_STRICT","0") == "1"
        self.style = _env("SCAI_GUIDE_STYLE","concise, human, professional; first sentence answers directly; 90-140 words")

    def _greet_meta(self, q: str) -> str:
        sys = {"role":"SustainaCore Assistant","style":self.style,"goals":[
            "Greet or briefly explain what SustainaCore/TECH100 is and how to use it.",
            "Offer ONE relevant example question.",
            "Do not include a Sources footer."
        ],"format":"Return JSON { answer: string }"}
        out = _chat([{"role":"system","content":json.dumps(sys)}, {"role":"user","content":q or "hi"}], json_mode=True)
        return out.get("answer","").strip()

    def __call__(self, environ, start_response):
        if environ.get("PATH_INFO","") != "/ask2" or environ.get("REQUEST_METHOD","GET").upper() == "OPTIONS":
            return self.app(environ, start_response)

        try: length = int(environ.get("CONTENT_LENGTH") or "0")
        except Exception: length = 0
        body = environ["wsgi.input"].read(length) if length > 0 else b"{}"
        req = _json_or_none(body) or {}
        question = (req.get("question") or req.get("q") or "").strip()

        if not question:
            start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
            return [json.dumps({"answer":"", "sources":[]}).encode("utf-8")]

        if _is_greeting(question) or _is_meta(question):
            try:
                ans = self._greet_meta(question)
            except Exception:
                ans = "" if self.strict else ""
            start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
            return [json.dumps({"answer": ans, "sources": []}).encode("utf-8")]

        captured = {"status": "200 OK", "headers": []}
        captured_body = []

        def cap(status, headers, exc_info=None):
            captured["status"] = status
            captured["headers"] = headers

            def _write(data):
                if isinstance(data, bytes):
                    captured_body.append(data)
                else:
                    captured_body.append(str(data).encode("utf-8"))

            return _write

        inner_env = environ.copy()
        j = json.dumps(req).encode("utf-8")
        inner_env["wsgi.input"] = io.BytesIO(j)
        inner_env["CONTENT_LENGTH"] = str(len(j))
        try:
            inner_iter = self.app(inner_env, cap)
            for chunk in inner_iter:
                if isinstance(chunk, bytes):
                    captured_body.append(chunk)
                else:
                    captured_body.append(str(chunk).encode("utf-8"))
            if hasattr(inner_iter,"close"):
                inner_iter.close()
            body_bytes = b"".join(captured_body)
            payload = _json_or_none(body_bytes) or {}
        except Exception:
            body_bytes = b""
            payload = {}

        status = captured.get("status", "200 OK")
        headers = captured.get("headers") or []

        if not status.startswith("200"):
            start_response(status, headers or [("Content-Type","application/json; charset=utf-8")])
            return [body_bytes]

        meta = payload.get("meta") if isinstance(payload, dict) else None
        if isinstance(meta, dict) and meta.get("gemini_first"):
            shaped = {
                "answer": str(payload.get("answer") or ""),
                "sources": payload.get("sources") if isinstance(payload.get("sources"), list) else [],
                "meta": meta,
            }
            start_response(status, [("Content-Type","application/json; charset=utf-8")])
            return [json.dumps(shaped, ensure_ascii=False).encode("utf-8")]

        ev = _extract_evidence(payload)
        curated = _prioritize_snippets(ev.get("snippets",[]), "general")
        citations = _dedup_sources(payload.get("sources",[]) if isinstance(payload, dict) else ev.get("sources",[]), limit=3)

        sys1 = {
            "role":"Write a unified answer using ONLY the provided evidence; no hallucinations; no inline refs.",
            "style":self.style,
            "format":"Return JSON { answer: string }"
        }
        draft = _chat([{"role":"system","content":json.dumps(sys1)},
                       {"role":"user","content":json.dumps({"question":question,"evidence_snippets":curated})}], json_mode=True)
        sys2 = {
            "role":"Improve tone/clarity; keep human; remove repetition; no inline refs.",
            "style":self.style,
            "format":"Return JSON { answer: string }"
        }
        improved = _chat([{"role":"system","content":json.dumps(sys2)},
                          {"role":"user","content":json.dumps({"question":question,"draft":draft})}], json_mode=True)
        sys3 = {
            "role":"Validate against evidence; compress to ~120 words; format for final response.",
            "style":self.style,
            "format":"Return JSON { answer: string }",
            "output_format":[
                "**Answer**",
                "1-2 short paragraphs",
                "",
                "**Key facts**",
                "- 3-5 bullets, one sentence each",
                "",
                "**Sources**",
                "1. Title — URL"
            ]
        }
        final = _chat([{"role":"system","content":json.dumps(sys3)},
                       {"role":"user","content":json.dumps({"question":question,"answer":improved,"evidence_snippets":curated})}], json_mode=True)

        answer = _format_structured_answer(question, final.get("answer",""), curated, citations)

        start_response("200 OK",[("Content-Type","application/json; charset=utf-8")])
        return [json.dumps({"answer": answer, "sources": citations}).encode("utf-8")]

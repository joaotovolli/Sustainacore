# coding: utf-8
import re

SUGGESTIONS = [
    "TECH100 membership",
    "Rank (latest)",
    "AI & ESG profile",
    "Explain TECH100 / Five pillars",
]

GREETING_RE = re.compile(r'^\s*(hi|hello|hey|hiya|hola|gm|good (morning|afternoon|evening)|greetings)\b[ !?.]*$', re.I)
THANKS_RE   = re.compile(r'\b(thanks|thank you|ty|cheers|appreciated)\b', re.I)
BYE_RE      = re.compile(r'^\s*(bye|goodbye|see (ya|you)|later|cya)\b', re.I)
HELP_RE     = re.compile(r'\b(help|what can you do|commands|menu)\b', re.I)

def _resp(kind, msg):
    return {"type": kind, "answer": msg, "suggestions": SUGGESTIONS}

def smalltalk_response(text):
    q = (text or "").strip()
    if not q or GREETING_RE.match(q):
        return _resp("greeting",
            "Hi! I'm the SustainaCore Assistant. I can help with TECH100 membership, latest rank, AI & ESG profiles, and explaining the methodology.")
    if THANKS_RE.search(q):
        return _resp("smalltalk", "You're welcome! Want to check a company's TECH100 status?")
    if HELP_RE.search(q):
        return _resp("help",
            "Try: \"Microsoft TECH100 membership\", \"Rank (latest) for Alphabet\", or \"Explain TECH100 pillars\".")
    if BYE_RE.match(q):
        return _resp("smalltalk", "Bye for now! If you need me again, ask about TECH100 or any company profile.")
    return None


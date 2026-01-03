"""Configuration for scheduled research generator."""
from __future__ import annotations

import os

MODEL_NAME = "gemini-2.5-flash"
GEMINI_BIN = os.getenv("GEMINI_BIN", "gemini")
WORKER_CWD = os.path.abspath(os.path.dirname(__file__))

GPT_MODEL_NAME = os.getenv("RESEARCH_GPT_MODEL", "gpt-5.2")
GPT_API_URL = os.getenv("RESEARCH_GPT_API_URL", "https://api.openai.com/v1/chat/completions")
GPT_MAX_TOKENS = int(os.getenv("RESEARCH_GPT_MAX_TOKENS", "1200"))
GPT_TEMPERATURE = float(os.getenv("RESEARCH_GPT_TEMPERATURE", "0.2"))

DEFAULT_OUTPUT_DIR = os.path.join(WORKER_CWD, "output")
DEFAULT_STATE_DIR = os.path.join(WORKER_CWD, ".state")

QUOTA_STATE_PATH = "/var/lib/sustainacore/research_generator/quota_state.json"
QUOTA_FALLBACK_PATH = os.path.join(DEFAULT_STATE_DIR, "quota_state.json")

REPORT_STATE_PATH = os.path.join(DEFAULT_STATE_DIR, "report_state.json")

MAX_CALLS_PER_MINUTE = 50
SLEEP_ON_MINUTE_LIMIT = 120
MAX_CALLS_PER_DAY = 950
DAILY_BUFFER_SECONDS = 60

REQUEST_TYPE = "RESEARCH_POST"
FILE_MIME_DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

REPORT_METHOD_URL = "https://sustainacore.org/methodology"

BANNED_PHRASES = ["buy", "sell", "should", "target price"]
BANNED_SYMBOLS = ["$", "€", "£", "¥"]

HEADLINE_MIN_WORDS = 8
HEADLINE_MAX_WORDS = 14

MAX_TABLE_ROWS = 12
MAX_COLUMN_NAME_LENGTH = 30

FAST_POLL_SLEEP = 2

CORE_WEIGHT_THRESHOLD = 0.0

PILLAR_COLUMNS = [
    "aiges_pillar_policy",
    "aiges_pillar_transparency",
    "aiges_pillar_accountability",
    "aiges_pillar_safety",
]

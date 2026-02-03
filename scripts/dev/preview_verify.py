#!/usr/bin/env python3
"""Local vs prod preview verification.

Creates HTML snapshots and a report under local_artifacts/.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import quote

DEFAULT_LOCAL_BASE = "http://127.0.0.1:8000"
DEFAULT_PROD_BASE = "https://sustainacore.org"

NEWS_FIXTURE_IDS = ["NEWS_ITEMS:99", "NEWS_ITEMS:44", "NEWS_ITEMS:77"]
TECH100_FIXTURE_TICKERS = ["TCH01", "TCH02", "TCH03"]


def _slugify(path: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", path.strip("/"))
    return slug or "root"


def _curl_fetch(url: str, output_path: Path, timeout: int) -> Tuple[str, float]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "curl",
        "-L",
        "-sS",
        "--connect-timeout",
        "5",
        "--max-time",
        str(timeout),
        "-o",
        str(output_path),
        "-w",
        "%{http_code} %{time_total}",
        url,
    ]
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.stderr:
        # Avoid leaking secrets; stderr here should be safe network errors.
        sys.stderr.write(result.stderr.strip() + "\n")
    parts = (result.stdout or "").strip().split()
    if len(parts) == 2:
        return parts[0], float(parts[1])
    return "000", 0.0


def _read_title(path: Path) -> str:
    try:
        html = path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return ""
    match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _extract_links(html: str, pattern: str, limit: int) -> List[str]:
    matches = re.findall(pattern, html, flags=re.IGNORECASE)
    unique: List[str] = []
    for match in matches:
        if match not in unique:
            unique.append(match)
        if len(unique) >= limit:
            break
    return unique


def _fetch_prod_html(url: str, timeout: int) -> str:
    cmd = [
        "curl",
        "-L",
        "-sS",
        "--connect-timeout",
        "5",
        "--max-time",
        str(timeout),
        url,
    ]
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        return ""
    return result.stdout or ""


def build_url_list(prod_base: str, timeout: int) -> List[str]:
    urls = [
        "/",
        "/robots.txt",
        "/sitemap.xml",
        "/sitemaps/news.xml",
        "/privacy/",
        "/terms/",
        "/press/",
        "/tech100/",
        "/news/",
        "/ai-regulation/",
    ]

    prefer_news_fixtures = os.getenv("NEWS_UI_DATA_MODE") == "fixture"
    prod_news_html = _fetch_prod_html(f"{prod_base}/news/", timeout)
    news_links = []
    if not prefer_news_fixtures:
        news_links = _extract_links(
            prod_news_html,
            r'href=["\'](/news/(?!admin/|assets/)[^"\']+/)["\']',
            3,
        )
    if not news_links:
        news_links = [f"/news/{quote(item, safe='')}/" for item in NEWS_FIXTURE_IDS]

    prefer_tech_fixtures = os.getenv("TECH100_UI_DATA_MODE") == "fixture"
    prod_tech_html = _fetch_prod_html(f"{prod_base}/tech100/", timeout)
    tech_links = []
    if not prefer_tech_fixtures:
        tech_links = _extract_links(
            prod_tech_html,
            r'href=["\'](/tech100/company/[^"\']+/)["\']',
            3,
        )
    if not tech_links:
        tech_links = [f"/tech100/company/{ticker}/" for ticker in TECH100_FIXTURE_TICKERS]

    return urls + news_links + tech_links


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-base", default=DEFAULT_LOCAL_BASE)
    parser.add_argument("--prod-base", default=DEFAULT_PROD_BASE)
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--timestamp", default="")
    parser.add_argument("--output-root", default="local_artifacts")
    args = parser.parse_args()

    timestamp = args.timestamp or time.strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output_root)
    snapshot_dir = output_root / f"snapshots_{timestamp}"
    report_path = output_root / f"report_{timestamp}.md"
    url_list_path = output_root / f"url_list_{timestamp}.json"

    urls = build_url_list(args.prod_base, args.timeout)
    url_entries = []

    for path in urls:
        slug = _slugify(path)
        local_url = f"{args.local_base}{path}"
        prod_url = f"{args.prod_base}{path}"
        local_html = snapshot_dir / f"local_{slug}.html"
        prod_html = snapshot_dir / f"prod_{slug}.html"

        local_status, local_time = _curl_fetch(local_url, local_html, args.timeout)
        prod_status, prod_time = _curl_fetch(prod_url, prod_html, args.timeout)

        url_entries.append(
            {
                "path": path,
                "slug": slug,
                "local_url": local_url,
                "prod_url": prod_url,
                "local_status": local_status,
                "prod_status": prod_status,
                "local_time": local_time,
                "prod_time": prod_time,
                "local_title": _read_title(local_html),
                "prod_title": _read_title(prod_html),
            }
        )

    output_root.mkdir(parents=True, exist_ok=True)
    url_list_path.write_text(json.dumps(url_entries, indent=2), encoding="utf-8")

    lines = [
        "# Local Preview Verification Report",
        "",
        f"- Timestamp: {timestamp}",
        f"- Local base: {args.local_base}",
        f"- Prod base: {args.prod_base}",
        f"- Snapshots: {snapshot_dir}",
        "- Screenshots: local_artifacts/screenshots_<timestamp>/",
        f"- URL list: {url_list_path}",
        "",
        "## URL Results",
        "| Path | Local Status | Local Time (s) | Prod Status | Prod Time (s) | Local Title | Prod Title |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for entry in url_entries:
        lines.append(
            "| {path} | {local_status} | {local_time:.3f} | {prod_status} | {prod_time:.3f} | {local_title} | {prod_title} |".format(
                **entry
            )
        )

    warnings = []
    for entry in url_entries:
        if entry["local_status"] not in {"200", "301", "302", "404"}:
            warnings.append(f"Local {entry['path']} returned {entry['local_status']}")
        if entry["prod_status"] not in {"200", "301", "302", "404"}:
            warnings.append(f"Prod {entry['path']} returned {entry['prod_status']}")

    lines.extend(["", "## Warnings"])
    if warnings:
        lines.extend([f"- {warn}" for warn in warnings])
    else:
        lines.append("- None")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

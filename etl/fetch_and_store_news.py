import feedparser
from newspaper import Article
from trafilatura import fetch_url, extract
from Connect_apexRI4X6 import get_connection
from datetime import datetime
import cx_Oracle
import time
from urllib.parse import quote_plus, urlparse, parse_qs
import requests

# ESG-related RSS sources
RSS_FEEDS = [
    "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en",
    "https://www.esgtoday.com/feed/",
    "https://feeds.feedburner.com/EnvironmentalLeader",
    "https://sustainablebrands.com/feed",
    "https://www.csrwire.com/rss/press_releases",
]

def resolve_actual_url(link):
    # If it's a Google News link, extract final article URL
    if "news.google.com" in link:
        try:
            resp = requests.get(link, timeout=10, allow_redirects=True)
            return resp.url
        except:
            return None
    return link

def fetch_news(company, max_results=200):
    all_entries = []
    query = quote_plus(f"{company} ESG")

    for url in RSS_FEEDS:
        feed_url = url.format(query=query) if "{query}" in url else url
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            if "{query}" not in url:
                # Filter static feeds by content
                text = f"{entry.get('title', '')} {entry.get('summary', '')}".lower()
                if company.lower() not in text:
                    continue
            all_entries.append(entry)

    return all_entries[:max_results]

def extract_full_text(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        if article.text.strip():
            return article.text.strip()
    except:
        pass

    try:
        downloaded = fetch_url(url)
        if downloaded:
            text = extract(downloaded)
            if text:
                return text.strip()
    except:
        pass

    return None

def insert_news(entries, company):
    conn = get_connection()
    cursor = conn.cursor()

    insert_sql = '''
        INSERT INTO ESG_NEWS (COMPANY, TITLE, URL, SOURCE, DATE_PUBLISHED, TEXT)
        VALUES (:1, :2, :3, :4, :5, :6)
    '''

    inserted = 0
    skipped = 0

    for entry in entries:
        title = entry.get("title", "")[:1000]
        original_url = entry.get("link")
        published = entry.get("published_parsed")

        if not original_url:
            print("⚠️ Skipping: no URL")
            continue

        # Resolve redirect if it's a Google link
        url = resolve_actual_url(original_url)
        if not url:
            print(f"⚠️ Could not resolve URL: {original_url}")
            continue

        try:
            dt_published = datetime.fromtimestamp(time.mktime(published)) if published else datetime.utcnow()
        except:
            print("⚠️ Invalid date for:", title)
            continue

        source = entry.get("source", {}).get("title", "")
        if not source and "-" in title:
            source = title.split("-")[-1].strip()
        source = source[:200]

        # Summary fallback only if valid
        summary = entry.get("summary", "").strip()
        text = summary if len(summary) > 200 and "<" not in summary else None

        # Fallback to article extraction if needed
        if not text:
            text = extract_full_text(url)

        if not text:
            print(f"⚠️ No text extracted from: {title}")
            continue

        try:
            cursor.execute(insert_sql, (
                company.lower(),
                title,
                url,
                source,
                dt_published,
                text
            ))
            inserted += 1
        except cx_Oracle.IntegrityError:
            skipped += 1
        except Exception as e:
            print(f"🛑 DB error: {e} | URL: {url}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n✅ Inserted: {inserted}, ⚠️ Skipped (duplicates or no content): {skipped}")

if __name__ == "__main__":
    company = input("Enter company name: ").strip()
    print(f"\n🔍 Fetching news for: {company}")
    entries = fetch_news(company)
    print(f"📥 Fetched {len(entries)} entries.")
    insert_news(entries, company)

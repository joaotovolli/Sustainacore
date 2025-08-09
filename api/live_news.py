
import feedparser
import urllib.parse

# ESG keyword filters
ESG_KEYWORDS = [
    "environment", "sustainability", "climate", "carbon", 
    "diversity", "inclusion", "social", "governance", 
    "ethics", "labor", "emissions", "recycling", "transparency"
]

def fetch_esg_news(company, max_articles=10):
    query = f'"{company}" ESG OR environment OR social OR governance'
    encoded_query = urllib.parse.quote(query)
    rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    feed = feedparser.parse(rss_url)
    filtered_articles = []

    for entry in feed.entries:
        title = entry.title.lower()
        summary = entry.summary.lower()
        link = entry.link
        published = entry.published if 'published' in entry else None

        if any(keyword in title + summary for keyword in ESG_KEYWORDS):
            filtered_articles.append({
                "title": entry.title,
                "summary": entry.summary,
                "url": link,
                "published": published
            })

        if len(filtered_articles) >= max_articles:
            break

    return filtered_articles

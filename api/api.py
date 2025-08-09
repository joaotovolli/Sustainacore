
from fastapi import FastAPI, Query
from Connect_apexRI4X6 import get_connection
from live_news import fetch_esg_news
import datetime

app = FastAPI()

@app.get("/analyze")
def analyze_company(company: str = Query(..., description="Company name to analyze")):
    try:
        articles = fetch_esg_news(company)
        if not articles:
            return {"message": "No ESG-related news found for this company."}

        conn = get_connection()
        cursor = conn.cursor()

        insert_sql = '''
            INSERT INTO ESG_NEWS (company, title, summary, url, published)
            VALUES (:1, :2, :3, :4, :5)
        '''

        for article in articles:
            published_date = None
            try:
                published_date = datetime.datetime.strptime(article["published"], "%a, %d %b %Y %H:%M:%S %Z")
            except:
                pass

            cursor.execute(insert_sql, (
                company,
                article["title"],
                article["summary"],
                article["url"],
                published_date
            ))

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "message": f"Inserted {len(articles)} articles for '{company}'.",
            "articles": articles
        }

    except Exception as e:
        return {"error": str(e)}

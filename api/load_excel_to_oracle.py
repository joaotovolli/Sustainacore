
import pandas as pd
from Connect_apexRI4X6 import get_connection
import cx_Oracle

# Path to your Excel file
excel_path = r"C:\Users\joaot\OneDrive\Documentos\My Project\ESG_AI\Data\dec30_to_jan12\ESG\daily_E_score.csv"
df = pd.read_excel(excel_path, parse_dates=["DATE"])

# Clean and rename columns
df = df.rename(columns={
    "DATE": "published",
    "Organization": "company",
    "URL": "url",
    "SourceCommonName": "source"
})

df = df.dropna(subset=["company", "url", "published"])

# Connect to Oracle
conn = get_connection()
cursor = conn.cursor()

insert_sql = '''
    INSERT INTO ESG_NEWS (company, title, summary, url, published)
    VALUES (:1, :2, :3, :4, :5)
'''

inserted = 0
skipped = 0

for _, row in df.iterrows():
    try:
        cursor.execute(insert_sql, (
            row["company"],
            row.get("title", "") or row.get("TITLE", "") or "N/A",
            row.get("summary", "") or "Imported from Excel",
            row["url"],
            row["published"].to_pydatetime() if not pd.isna(row["published"]) else None
        ))
        inserted += 1
    except cx_Oracle.IntegrityError:
        skipped += 1
    except Exception as e:
        print(f"Error inserting row: {e}")

conn.commit()
cursor.close()
conn.close()

print(f"✅ Inserted: {inserted}")
print(f"⚠️ Skipped (duplicates): {skipped}")

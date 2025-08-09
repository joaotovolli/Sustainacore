
import pandas as pd
from Connect_apexRI4X6 import get_connection
import cx_Oracle
import datetime

# Load your CSV file
csv_path = "Data/all_data.csv"
df = pd.read_csv(csv_path, parse_dates=["DATE"])

# Prepare and clean columns
df = df.rename(columns={
    "DATE": "published",
    "Organization": "company",
    "URL": "url",
    "SourceCommonName": "source"
})

# Optional: drop rows with missing required fields
df = df.dropna(subset=["company", "url", "published"])

# Insert into Oracle
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
            row.get("summary", "") or "Imported from CSV",
            row["url"],
            row["published"].to_pydatetime() if not pd.isna(row["published"]) else None
        ))
        inserted += 1
    except cx_Oracle.IntegrityError:
        skipped += 1  # duplicate
    except Exception as e:
        print(f"Error inserting row: {e}")

conn.commit()
cursor.close()
conn.close()

print(f"✅ Inserted: {inserted}")
print(f"⚠️ Skipped (duplicates): {skipped}")

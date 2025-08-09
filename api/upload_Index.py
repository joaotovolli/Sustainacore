import pandas as pd
import cx_Oracle
import sys
import os

# -----------------------------------------------------------------------------
# 1. Reuse the connection from your existing wallet-based config
# -----------------------------------------------------------------------------
sys.path.append(r"C:\Users\joaot\.spyder-py3")
from Connect_apexRI4X6 import connection  # uses your wallet and environment

# -----------------------------------------------------------------------------
# 2. Load Excel and prepare data
# -----------------------------------------------------------------------------
file_path = r"C:\Users\joaot\OneDrive\Documentos\My Project\TECH100_AIGovernance_Q2_2025.xlsx"
df = pd.read_excel(file_path)

# Normalize column names
df.columns = df.columns.str.strip().str.upper().str.replace(r"\s+", "_", regex=True)
print("Detected columns:", list(df.columns))

# Fill missing columns and rename
if "TICKER" not in df.columns:
    df["TICKER"] = ""
if "SOURCE_LINKS" not in df.columns:
    df["SOURCE_LINKS"] = ""
if "AIGES_COMPOSITE" in df.columns:
    df.rename(columns={"AIGES_COMPOSITE": "AIGES_COMPOSITE_AVERAGE"}, inplace=True)

# Reorder and select final columns
required_cols = [
    "PORT_DATE", "RANK_INDEX", "COMPANY_NAME", "TICKER", "PORT_WEIGHT", 
    "GICS_SECTOR", "TRANSPARENCY", "ETHICAL_PRINCIPLES", "GOVERNANCE_STRUCTURE", 
    "REGULATORY_ALIGNMENT", "STAKEHOLDER_ENGAGEMENT", "AIGES_COMPOSITE_AVERAGE", 
    "SUMMARY", "SOURCE_LINKS"
]

missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

df["PORT_DATE"] = pd.to_datetime(df["PORT_DATE"]).dt.date
df["SUMMARY"] = df["SUMMARY"].astype(str)
df["SOURCE_LINKS"] = df["SOURCE_LINKS"].astype(str)

# -----------------------------------------------------------------------------
# 3. Upload to Oracle
# -----------------------------------------------------------------------------
sql = """
INSERT INTO WKSP_ESGAPEX.TECH11_AI_GOV_ETH_INDEX (
    PORT_DATE, RANK_INDEX, COMPANY_NAME, TICKER, PORT_WEIGHT,
    GICS_SECTOR, TRANSPARENCY, ETHICAL_PRINCIPLES, GOVERNANCE_STRUCTURE,
    REGULATORY_ALIGNMENT, STAKEHOLDER_ENGAGEMENT, AIGES_COMPOSITE_AVERAGE,
    SUMMARY, SOURCE_LINKS
) VALUES (
    :1, :2, :3, :4, :5,
    :6, :7, :8, :9,
    :10, :11, :12,
    :13, :14
)
"""

cur = connection.cursor()
data = [tuple(row[col] for col in required_cols) for _, row in df.iterrows()]
cur.executemany(sql, data)
connection.commit()
cur.close()
connection.close()

print(f"✅ Uploaded {len(data)} rows to Oracle using wallet.")

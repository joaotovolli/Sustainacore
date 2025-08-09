import pandas as pd
from datetime import datetime
from Connect_apexRI4X6 import get_connection

# Load Excel file
file_path = r"C:\Users\joaot\OneDrive\Documentos\My Project\TECH100_AI_Governance_Ethics_Index.xlsx"
df = pd.read_excel(file_path, engine="openpyxl")  # engine avoids compatibility issues

# Ensure PORT_DATE is interpreted as DD/MM/YYYY
df["PORT_DATE"] = pd.to_datetime(df["PORT_DATE"], dayfirst=True).dt.date

# Confirm expected columns exist
expected_cols = [
    "PORT_DATE", "RANK_INDEX", "Company_Name", "TICKER", "PORT_WEIGHT", "GICS_SECTOR",
    "Transparency", "Ethical_Principles", "Governance_Structure",
    "Regulatory_Alignment", "Stakeholder_Engagement",
    "AIGES", "Summary", "Source_Links"
]
missing = [col for col in expected_cols if col not in df.columns]
if missing:
    raise ValueError(f"Missing expected columns: {missing}")

# Reorder and rename columns to match Oracle table
df = df[[
    "PORT_DATE",
    "RANK_INDEX",
    "Company_Name",
    "TICKER",
    "PORT_WEIGHT",
    "GICS_SECTOR",
    "Transparency",
    "Ethical_Principles",
    "Governance_Structure",
    "Regulatory_Alignment",
    "Stakeholder_Engagement",
    "AIGES",
    "Summary",
    "Source_Links"
]]
df.columns = [
    "PORT_DATE",
    "RANK_INDEX",
    "COMPANY_NAME",
    "TICKER",
    "PORT_WEIGHT",
    "GICS_SECTOR",
    "TRANSPARENCY",
    "ETHICAL_PRINCIPLES",
    "GOVERNANCE_STRUCTURE",
    "REGULATORY_ALIGNMENT",
    "STAKEHOLDER_ENGAGEMENT",
    "AIGES_COMPOSITE_AVERAGE",
    "SUMMARY",
    "SOURCE_LINKS"
]

# Replace NaNs with None to avoid Oracle insertion errors
df = df.where(pd.notnull(df), None)

# Upload to Oracle
conn = get_connection()
cur = conn.cursor()

sql = """
INSERT INTO WKSP_ESGAPEX.TECH11_AI_GOV_ETH_INDEX (
    PORT_DATE,
    RANK_INDEX,
    COMPANY_NAME,
    TICKER,
    PORT_WEIGHT,
    GICS_SECTOR,
    TRANSPARENCY,
    ETHICAL_PRINCIPLES,
    GOVERNANCE_STRUCTURE,
    REGULATORY_ALIGNMENT,
    STAKEHOLDER_ENGAGEMENT,
    AIGES_COMPOSITE_AVERAGE,
    SUMMARY,
    SOURCE_LINKS
) VALUES (
    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14
)
"""

data = [tuple(row) for row in df.itertuples(index=False, name=None)]
cur.executemany(sql, data)
conn.commit()
cur.close()
conn.close()

print("✅ Upload complete with correct date format (DD/MM/YYYY).")

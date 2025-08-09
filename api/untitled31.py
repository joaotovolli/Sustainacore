import pandas as pd
from datetime import datetime
from Connect_apexRI4X6 import get_connection

# Load Excel file
file_path = r"C:\Users\joaot\OneDrive\Documentos\My Project\TECH100_AI_Governance_Ethics_Index_Preview.xlsx"
df = pd.read_excel(file_path)

# Confirm expected columns exist
expected_cols = [
    "Company_Name", "Transparency", "Ethical_Principles",
    "Governance_Structure", "Regulatory_Alignment", "Stakeholder_Engagement",
    "AIGES", "Summary", "Source_Links"
]
missing = [col for col in expected_cols if col not in df.columns]
if missing:
    raise ValueError(f"Missing expected columns: {missing}")

# Add PORT_DATE
df["PORT_DATE"] = datetime.today().date()

# Reorder columns to match Oracle table
df = df[[
    "PORT_DATE",
    "Company_Name",
    "Transparency",
    "Ethical_Principles",
    "Governance_Structure",
    "Regulatory_Alignment",
    "Stakeholder_Engagement",
    "AIGES",
    "Summary",
    "Source_Links"
]]

# Rename columns to Oracle column names
df.columns = [
    "PORT_DATE",
    "COMPANY_NAME",
    "TRANSPARENCY",
    "ETHICAL_PRINCIPLES",
    "GOVERNANCE_STRUCTURE",
    "REGULATORY_ALIGNMENT",
    "STAKEHOLDER_ENGAGEMENT",
    "AIGES_COMPOSITE_AVERAGE",
    "SUMMARY",
    "SOURCE_LINKS"
]

# Upload to Oracle
conn = get_connection()
cur = conn.cursor()

sql = """
INSERT INTO WKSP_ESGAPEX.TECH11_AI_GOV_ETH_INDEX (
    PORT_DATE,
    COMPANY_NAME,
    TRANSPARENCY,
    ETHICAL_PRINCIPLES,
    GOVERNANCE_STRUCTURE,
    REGULATORY_ALIGNMENT,
    STAKEHOLDER_ENGAGEMENT,
    AIGES_COMPOSITE_AVERAGE,
    SUMMARY,
    SOURCE_LINKS
) VALUES (
    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10
)
"""

cur.executemany(sql, [tuple(row) for row in df.itertuples(index=False)])
conn.commit()
cur.close()
conn.close()

print("✅ Upload complete.")

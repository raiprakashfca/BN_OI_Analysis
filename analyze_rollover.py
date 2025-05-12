# analyze_rollover.py
import pandas as pd
import datetime as dt
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os

# --- CONFIG ---
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
ROLLOVER_SHEET = "Rollover_Analysis"
REQUIRED_HEADERS = [
    "Date", "Stock", "Current Expiry", "Next Expiry",
    "Current OI", "Next OI", "Total OI", "Rollover %", "Delivery OI"
]

# --- Authenticate ---
def load_gsheet_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")

    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += "=" * (4 - padding)

    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(credentials)

def ensure_headers_exist(sheet, required_headers):
    existing = sheet.row_values(1)
    if existing != required_headers:
        sheet.resize(rows=1)
        sheet.update("A1", [required_headers])
        print(f"✅ Headers written to sheet: {sheet.title}")
    else:
        print(f"✅ Headers already present in: {sheet.title}")

# --- Main Logic ---
if __name__ == "__main__":
    client = load_gsheet_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(ROLLOVER_SHEET)

    # Ensure headers
    ensure_headers_exist(sheet, REQUIRED_HEADERS)

    # Placeholder: Replace this with actual logic in live use
    today = dt.date.today().isoformat()
    sample_data = [
        today, "BANKNIFTY", "2024-05-30", "2024-06-27",
        1200000, 900000, 2100000, 42.9, 57.1
    ]
    sheet.append_row(sample_data)
    print("✅ Sample rollover data appended.")

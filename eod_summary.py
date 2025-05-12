
import pandas as pd
import datetime as dt
import gspread
import json
import base64
import os
from google.oauth2.service_account import Credentials

# --- CONFIG ---
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
SHEET_NAME = "EOD_Summary"
HEADERS = ["Date", "Symbol", "Start OI", "End OI", "Net OI Change (%)", "Start Price", "End Price", "Price Change (%)", "Classification", "Anomaly"]

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
    client = gspread.authorize(credentials)
    return client

def ensure_headers(client, sheet_name, required_headers):
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(sheet_name)
    data = sheet.get_all_values()
    if not data or data[0] != required_headers:
        print(f"⚠️ Updating headers in sheet: {sheet_name}")
        sheet.clear()
        sheet.insert_row(required_headers, 1)
    else:
        print(f"✅ Headers present in sheet: {sheet_name}")

def main():
    client = load_gsheet_client()
    ensure_headers(client, SHEET_NAME, HEADERS)
    print("✅ EOD_Summary headers check completed.")

if __name__ == "__main__":
    main()

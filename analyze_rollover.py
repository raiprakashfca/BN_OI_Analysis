# analyze_rollover_debug.py — robust with debug logs and header checks
import pandas as pd
import datetime as dt
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os

# --- CONFIG ---
ROLL_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
ROLL_SHEET_NAME = "Rollover_Analysis"
EXPECTED_COLUMNS = ['Date', 'Symbol', 'Today OI', 'Previous OI', 'Net Change (%)', 'Price Change (%)', 'Rollover Category']

def load_gsheet_client():
    print("🔐 Decoding service account credentials...")
    b64_json = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not b64_json:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    padding = len(b64_json) % 4
    if padding:
        b64_json += "=" * (4 - padding)
    creds_dict = json.loads(base64.b64decode(b64_json).decode("utf-8"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(credentials)

def check_and_write_headers(sheet):
    data = sheet.get_all_values()
    if not data or data[0] != EXPECTED_COLUMNS:
        print(f"⚠️ Headers missing or incorrect in {ROLL_SHEET_NAME}. Writing headers...")
        sheet.clear()
        sheet.append_row(EXPECTED_COLUMNS)
    else:
        print(f"✅ Headers present in {ROLL_SHEET_NAME}")

def simulate_rollover_data():
    today = dt.datetime.now().strftime("%Y-%m-%d")
    dummy_data = [
        [today, 'BANKNIFTY', 1_200_000, 1_100_000, 9.09, 0.85, 'Long Build-up'],
        [today, 'SBIN', 800_000, 880_000, -9.09, -0.65, 'Long Unwinding']
    ]
    return dummy_data

if __name__ == "__main__":
    print("🚀 Starting Rollover Analysis Debug Script")
    try:
        client = load_gsheet_client()
        sheet = client.open_by_key(ROLL_SHEET_ID).worksheet(ROLL_SHEET_NAME)
        check_and_write_headers(sheet)
        rows = simulate_rollover_data()
        for row in rows:
            sheet.append_row(row)
        print(f"✅ Appended {len(rows)} rows to {ROLL_SHEET_NAME}")
    except Exception as e:
        print(f"❌ Failed to update {ROLL_SHEET_NAME}: {e}")

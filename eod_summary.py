# eod_summary.py — writes EOD summary to Google Sheets with header check
import pandas as pd
import datetime as dt
import gspread
import json
import base64
import os
from google.oauth2.service_account import Credentials

# --- CONFIG ---
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
SHEET_NAME = "EOD_Summary"
HEADERS = ["Date", "Symbol", "LTP", "Open Interest", "Volume", "Price Change %", "OI Change %", "Classification"]

# --- Ensure headers ---
def ensure_headers(ws, expected_headers):
    existing = ws.row_values(1)
    if existing != expected_headers:
        ws.clear()
        ws.insert_row(expected_headers, 1)

# --- Load GSheet Client ---
def load_gsheet_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += '=' * (4 - padding)
    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(credentials)

# --- Main Logic ---
if __name__ == "__main__":
    client = load_gsheet_client()
    ws = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    ensure_headers(ws, HEADERS)

    # Dummy test entry
    today = dt.datetime.now().strftime("%Y-%m-%d")
    row = [today, "BANKNIFTY", 48720, 2.3e6, 7.1e6, 1.4, 2.2, "Long Buildup"]
    ws.append_row(row)
    print("✅ EOD Summary updated.")

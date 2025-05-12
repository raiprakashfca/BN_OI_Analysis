# analyze_rollover.py — analyzes futures rollover and logs to Google Sheets with header check
import pandas as pd
import datetime as dt
import gspread
import json
import base64
import os
from google.oauth2.service_account import Credentials

# --- CONFIG ---
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
SHEET_NAME = "Rollover_Analysis"
HEADERS = ["Date", "Symbol", "Current OI", "Next OI", "Delivery %", "Roll %"]

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

    # Dummy entry
    today = dt.datetime.now().strftime("%Y-%m-%d")
    row = [today, "ICICIBANK", 12_00_000, 8_50_000, 41.5, 58.5]
    ws.append_row(row)
    print("✅ Rollover analysis logged.")

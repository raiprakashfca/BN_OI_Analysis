
# analyze_rollover.py — Analyze rollover and delivery on expiry
import pandas as pd
import datetime as dt
from google.oauth2.service_account import Credentials
import gspread
import base64
import json
import os

# --- CONFIG ---
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
ROLLOVER_SHEET_NAME = "Rollover_Analysis"
EXPECTED_HEADERS = ["Date", "Symbol", "Expiry", "OI on Expiry", "OI Next Day", "Delivery Qty", "Rollover %", "Delivery %"]

# --- Load Google Sheet Client ---
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

# --- Ensure Headers Exist ---
def ensure_sheet_headers(client, sheet_id, sheet_name, expected_headers):
    sheet = client.open_by_key(sheet_id)
    try:
        worksheet = sheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
    current_headers = worksheet.row_values(1)
    if current_headers != expected_headers:
        worksheet.resize(rows=1)  # Clear sheet if incorrect headers
        worksheet.update("A1", [expected_headers])
    return worksheet

# --- Main ---
if __name__ == "__main__":
    client = load_gsheet_client()
    worksheet = ensure_sheet_headers(client, OI_LOG_SHEET_ID, ROLLOVER_SHEET_NAME, EXPECTED_HEADERS)
    print("✅ Rollover_Analysis sheet is ready with correct headers.")

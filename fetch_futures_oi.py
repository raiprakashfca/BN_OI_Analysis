import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os

# --- Constants ---
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
GSHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
GSHEET_TAB = "EOD_Summary"
REQUIRED_HEADERS = [
    "Date", "Symbol", "Start OI", "End OI", "Net OI Change (%)",
    "Start Price", "End Price", "Price Change (%)", "Classification", "Anomaly"
]

# --- Load Google Sheet Client ---
def load_gsheet_client():
    print("🔐 Authenticating with Google Sheets...")
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += "=" * (4 - padding)
    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(credentials)

# --- Load Kite Tokens ---
def load_kite_client(client):
    print("📦 Loading Zerodha tokens from Google Sheets...")
    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    access_token = sheet.acell("C1").value
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

# --- Simulate EOD Data ---
def generate_dummy_eod():
    print("📊 Simulating EOD data for testing...")
    today = dt.date.today().strftime("%Y-%m-%d")
    return pd.DataFrame([
        [today, "BANKNIFTY", 1_500_000, 1_800_000, 20.0, 48700, 48950, 0.51, "Long Build-up", ""],
        [today, "AXISBANK", 2_300_000, 1_900_000, -17.4, 1210, 1180, -2.48, "Long Unwinding", ""]
    ], columns=REQUIRED_HEADERS)

# --- Write to Google Sheet ---
def write_to_google_sheet(client, df):
    print("🧾 Preparing to write EOD Summary...")
    sheet = client.open_by_key(GSHEET_ID)
    try:
        worksheet = sheet.worksheet(GSHEET_TAB)
    except gspread.exceptions.WorksheetNotFound:
        print(f"⚠️ Sheet '{GSHEET_TAB}' not found. Creating new...")
        worksheet = sheet.add_worksheet(title=GSHEET_TAB, rows="1000", cols="20")

    current_headers = worksheet.row_values(1)
    if current_headers != REQUIRED_HEADERS:
        print("⚠️ Headers mismatch.")
        worksheet.clear()
        worksheet.append_row(REQUIRED_HEADERS)

    print(f"📤 Writing {len(df)} rows to Google Sheet...")
    worksheet.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
    print("✅ Write complete.")

# --- Main Entry ---
if __name__ == "__main__":
    client = load_gsheet_client()
    kite = load_kite_client(client)
    df = generate_dummy_eod()
    write_to_google_sheet(client, df)
    print("🎯 Script execution finished.")


# fetch_futures_oi.py — debug version to write EOD futures data
import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os
import sys

TOKEN_SHEET_NAME = "ZerodhaTokenStore"
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
TAB_NAME = "EOD_Summary"

def is_trading_day():
    today = dt.date.today()
    return today.weekday() < 5

def load_kite_client():
    print("🔐 Authenticating with Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += '=' * (4 - padding)
    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(credentials)

    print("📦 Loading Zerodha tokens from Google Sheets...")
    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    access_token = sheet.acell("C1").value

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

def write_to_google_sheet(client, df):
    print("🧾 Preparing to write EOD Summary...")
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(TAB_NAME)
    existing = worksheet.get_all_values()

    expected_headers = ["Date", "Symbol", "LTP", "Change %", "Volume"]

    if not existing:
        print("⚠️ Sheet is empty, writing headers.")
        worksheet.append_row(expected_headers)
    elif existing[0] != expected_headers:
        print("⚠️ Headers mismatch.")
        raise ValueError("Header mismatch")

    if df.empty:
        print("⚠️ No EOD data to write.")
        return

    rows = df.values.tolist()
    worksheet.append_rows(rows)
    print(f"✅ Wrote {len(rows)} rows to {TAB_NAME}.")

if __name__ == "__main__":
    if not is_trading_day():
        print("Market closed today. Skipping run.")
        sys.exit()

    kite, client = load_kite_client()

    print("📊 Simulating EOD data for testing...")
    data = {
        "Date": [str(dt.date.today())] * 2,
        "Symbol": ["BANKNIFTY", "AXISBANK"],
        "LTP": [48800, 1168],
        "Change %": [0.55, -0.78],
        "Volume": [152430, 348000]
    }
    df = pd.DataFrame(data)
    write_to_google_sheet(client, df)

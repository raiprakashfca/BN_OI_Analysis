
# fetch_oi_futures.py — with classification and anomaly detection and debug logs
import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os
import sys

# --- CONFIG ---
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
TAB_NAME = "Sheet1"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

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
    print("✅ Token valid for user:", sheet.acell("D1").value if sheet.acell("D1").value else "Unknown")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

def get_futures_tokens(kite):
    print("🧠 Caching instrument list...")
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst.segment == "NFO-FUT") & (df_inst.name.isin(STOCKS))]
    print(f"✅ Retrieved {len(df_fut)} futures instruments.")
    return df_fut[["name", "instrument_type", "expiry", "strike", "instrument_token"]]

def write_to_google_sheet(client, df):
    print("🧾 Preparing to write to Google Sheet...")
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(TAB_NAME)
    existing = worksheet.get_all_values()

    expected_headers = ["name", "instrument_type", "expiry", "strike", "instrument_token"]

    if not existing:
        print("⚠️ Sheet is empty, writing headers.")
        worksheet.append_row(expected_headers)
    elif existing[0] != expected_headers:
        print("⚠️ Headers in sheet don't match expected headers.")
        print("Expected:", expected_headers)
        print("Found:", existing[0])
        raise ValueError("Header mismatch")

    if df.empty:
        print("⚠️ No data to write.")
        return

    rows = df.values.tolist()
    worksheet.append_rows(rows)
    print(f"✅ Wrote {len(rows)} rows to {TAB_NAME}.")

if __name__ == "__main__":
    if not is_trading_day():
        print("Market closed today. Skipping run.")
        sys.exit()

    kite, client = load_kite_client()
    df = get_futures_tokens(kite)
    write_to_google_sheet(client, df)

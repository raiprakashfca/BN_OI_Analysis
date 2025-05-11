# fetch_futures_oi.py — Updated to use SERVICE_ACCOUNT_JSON_B64
import os
import json
import base64
from datetime import datetime
import pandas as pd
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread

# --- Constants ---
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
SHEET_NAME = "Futures_OI_Log"
SYMBOLS = ["BANKNIFTY", "HDFCBANK", "SBIN", "ICICIBANK", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

# --- Setup Google Sheet client ---
def get_gsheet_client():
    b64_json = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not b64_json:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64 environment variable")
    padding = len(b64_json) % 4
    if padding:
        b64_json += '=' * (4 - padding)
    creds_dict = json.loads(base64.b64decode(b64_json).decode("utf-8"))
    credentials = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(credentials)

# --- Setup Zerodha client ---
def get_kite_client():
    api_key = os.getenv("ZERODHA_API_KEY")
    access_token = os.getenv("ZERODHA_ACCESS_TOKEN")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

# --- Fetch Futures OI ---
def fetch_futures_oi():
    kite = get_kite_client()
    instruments = pd.DataFrame(kite.instruments("NSE"))
    today = datetime.now().date()

    fno = pd.DataFrame(kite.instruments("NFO"))
    fno = fno[(fno["segment"] == "NFO-FUT") & (fno["instrument_type"] == "FUT")]

    records = []
    for symbol in SYMBOLS:
        df = fno[fno["name"] == symbol].sort_values("expiry")
        if df.empty:
            print(f"⚠️ No futures found for {symbol}")
            continue

        row = df.iloc[0]  # Nearest expiry
        token = row["instrument_token"]
        lot_size = row["lot_size"]
        expiry = row["expiry"].strftime("%Y-%m-%d")

        try:
            quote = kite.ltp([f"NFO:{row['tradingsymbol']}"])
            oi = quote[f"NFO:{row['tradingsymbol']}"]["depth"]["buy"]
            total_oi = sum([level["orders"] for level in oi])
            records.append([str(today), symbol, expiry, token, total_oi, lot_size])
            print(f"✅ {symbol} | Expiry: {expiry} | OI: {total_oi}")
        except Exception as e:
            print(f"❌ Error fetching OI for {symbol}: {e}")

    return records

# --- Append to Google Sheet ---
def append_to_sheet(records):
    client = get_gsheet_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    sheet.append_rows(records, value_input_option="USER_ENTERED")
    print(f"✅ Logged {len(records)} rows to {SHEET_NAME}")

# --- Main Entry ---
if __name__ == "__main__":
    rows = fetch_futures_oi()
    if rows:
        append_to_sheet(rows)
    else:
        print("⚠️ No data to log.")

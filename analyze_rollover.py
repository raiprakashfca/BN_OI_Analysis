# analyze_rollover.py — Logs rollover vs delivery volume for top BankNifty components
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
SHEET_NAME = "Rollover_Analysis"
SYMBOLS = ["BANKNIFTY", "HDFCBANK", "SBIN", "ICICIBANK", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

# --- Setup Google Sheet client ---
def get_gsheet_client():
    b64_json = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not b64_json:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64 env var")
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

# --- Analyze Rollover vs Delivery ---
def analyze_rollovers():
    kite = get_kite_client()
    fno = pd.DataFrame(kite.instruments("NFO"))
    fno = fno[(fno["segment"] == "NFO-FUT") & (fno["instrument_type"] == "FUT")]
    today = datetime.now().date()

    output = []
    for symbol in SYMBOLS:
        df = fno[fno["name"] == symbol].sort_values("expiry")
        if len(df) < 2:
            print(f"⚠️ Not enough expiries for {symbol}")
            continue

        curr = df.iloc[0]
        next_ = df.iloc[1]
        try:
            quote_curr = kite.ltp([f"NFO:{curr['tradingsymbol']}"])
            quote_next = kite.ltp([f"NFO:{next_['tradingsymbol']}"])
            oi_curr = quote_curr[f"NFO:{curr['tradingsymbol']}"]["depth"]["buy"]
            oi_next = quote_next[f"NFO:{next_['tradingsymbol']}"]["depth"]["buy"]
            sum_curr = sum([level["orders"] for level in oi_curr])
            sum_next = sum([level["orders"] for level in oi_next])
            delivery = max(sum_curr - sum_next, 0)
            rollover = min(sum_next, sum_curr)
            output.append([str(today), symbol, curr["expiry"].strftime("%Y-%m-%d"), sum_curr, sum_next, rollover, delivery])
            print(f"✅ {symbol} | Rollover: {rollover}, Delivery: {delivery}")
        except Exception as e:
            print(f"❌ Error for {symbol}: {e}")
    return output

# --- Append to Sheet ---
def append_to_sheet(rows):
    client = get_gsheet_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    sheet.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"✅ Logged {len(rows)} rows to {SHEET_NAME}")

# --- Main ---
if __name__ == "__main__":
    results = analyze_rollovers()
    if results:
        append_to_sheet(results)
    else:
        print("⚠️ No data to log.")

# fetch_futures_oi.py — robust version with debug logs and safety checks
import os
import json
import base64
import datetime as dt
import pandas as pd
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import requests

# --- CONFIG ---
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
FUTURES_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
FUTURES_TAB = "Futures_OI_Log"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

def is_trading_hour():
    now = dt.datetime.now()
    if now.weekday() >= 5:
        return False
    return dt.time(9, 15) <= now.time() <= dt.time(15, 30)

def load_kite_and_gsheet():
    print("🔐 Authenticating Google Sheets and Zerodha...")
    b64_json = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not b64_json:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    b64_json += "=" * ((4 - len(b64_json) % 4) % 4)
    creds_dict = json.loads(base64.b64decode(b64_json).decode("utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)

    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    access_token = sheet.acell("C1").value

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    try:
        kite.profile()
        print("✅ Zerodha token valid.")
    except Exception as e:
        raise Exception("❌ Invalid Zerodha access token") from e

    return kite, client

def fetch_futures_data(kite):
    print("📡 Fetching futures instruments...")
    instruments = kite.instruments("NSE") + kite.instruments("NFO")
    df = pd.DataFrame(instruments)
    df_fut = df[(df.segment == "NFO-FUT") & (df.name.isin(STOCKS))]
    print(f"✅ Fetched {len(df_fut)} futures contracts.")
    return df_fut

def ensure_headers(sheet):
    headers = ["Date", "Time", "Symbol", "Expiry", "OI", "Change in OI", "LTP"]
    data = sheet.get_all_values()
    if not data or data[0] != headers:
        print("🔧 Writing headers to Futures_OI_Log...")
        sheet.update("A1", [headers])
    else:
        print("✅ Headers present.")

def append_dummy_data(sheet):
    now = dt.datetime.now()
    row = [now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S"), "BANKNIFTY", "2024-05-30", 152300, 12400, 49090]
    sheet.append_row(row)
    print("✅ Dummy row appended.")

if __name__ == "__main__":
    if not is_trading_hour():
        print("⏰ Outside trading hours.")
        exit()

    try:
        kite, client = load_kite_and_gsheet()
        sheet = client.open_by_key(FUTURES_SHEET_ID).worksheet(FUTURES_TAB)
        ensure_headers(sheet)
        df = fetch_futures_data(kite)
        append_dummy_data(sheet)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("⚠️ Rate limit hit.")
        else:
            raise
    except Exception as e:
        print("❌ Error during execution:", e)

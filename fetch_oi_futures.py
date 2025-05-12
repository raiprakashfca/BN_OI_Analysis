# fetch_oi_futures.py — with caching, token validation, and rate-limit handling
import os
import json
import base64
import time
import pickle
import datetime as dt
import pandas as pd
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import requests

# === CONFIG ===
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
OI_LOG_SHEET_NAME = "Sheet1"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]
CACHE_FILE = "instrument_cache.pkl"

def is_trading_hour():
    now = dt.datetime.now()
    if now.weekday() >= 5:
        return False
    return dt.time(9, 15) <= now.time() <= dt.time(15, 30)

def load_kite_and_gsheet():
    print("🔐 Authenticating with Google Sheets...")
    b64_json = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not b64_json:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64 env variable")
    b64_json += "=" * ((4 - len(b64_json) % 4) % 4)  # padding
    creds_dict = json.loads(base64.b64decode(b64_json).decode("utf-8"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(credentials)

    print("📦 Loading Zerodha tokens from Google Sheets...")
    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    access_token = sheet.acell("C1").value

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    try:
        profile = kite.profile()
        print(f"✅ Token valid for user: {profile['user_name']}")
    except Exception as e:
        raise Exception("❌ Invalid Zerodha Access Token") from e

    return kite, client

def get_instrument_tokens(kite):
    print("🧠 Caching instrument list...")
    today = dt.date.today().isoformat()
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            cache_data = pickle.load(f)
        if cache_data.get("date") == today:
            print("✅ Loaded cached instruments.")
            return cache_data["data"]

    print("📡 Fetching fresh instrument list from Zerodha...")
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst["segment"] == "NFO-FUT") & (df_inst["name"].isin(STOCKS))]
    cache_data = {"date": today, "data": df_fut}
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache_data, f)
    return df_fut

def main():
    if not is_trading_hour():
        print("⏰ Outside trading hours. Skipping execution.")
        return

    try:
        kite, client = load_kite_and_gsheet()
        df_fut = get_instrument_tokens(kite)
        print(f"✅ Retrieved {len(df_fut)} futures instruments.")
        # Add logic to process df_fut and log to Sheets if needed
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("⚠️ API rate limit hit. Skipping this run.")
        else:
            raise
    except Exception as ex:
        print("❌ Error during execution:", ex)

if __name__ == "__main__":
    main()

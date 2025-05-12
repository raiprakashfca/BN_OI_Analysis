# fetch_futures_oi.py — enhanced with header write + detailed debug
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
SHEET_NAME = "Futures_OI_Log"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

def load_kite_client():
    print("🔐 Loading service account credentials...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")

    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += "=" * (4 - padding)

    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(credentials)

    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    access_token = sheet.acell("C1").value

    print("✅ API key and token retrieved.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

def ensure_headers(ws, required_headers):
    existing = ws.get_all_values()
    if not existing or existing[0] != required_headers:
        print("⚠️ Headers missing or incorrect — writing headers.")
        ws.update("A1", [required_headers])
    else:
        print("✅ Headers already present.")

def append_data(ws, row):
    ws.append_row(row, value_input_option="USER_ENTERED")
    print("✅ Row appended.")

def fetch_and_log_futures_oi(kite, client):
    print("📦 Fetching instruments...")
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst["segment"] == "NFO-FUT") & (df_inst["name"].isin(STOCKS))]

    print(f"✅ {len(df_fut)} futures contracts found.")
    df_fut["date"] = dt.datetime.now().strftime("%Y-%m-%d")
    df_fut["time"] = dt.datetime.now().strftime("%H:%M:%S")

    columns = ["date", "time", "name", "tradingsymbol", "expiry", "lot_size", "instrument_token", "last_price", "oi", "change_oi"]
    data = []

    for _, row in df_fut.iterrows():
        try:
            quote = kite.ltp([row["instrument_token"]])[str(row["instrument_token"])]
            ltp = quote["last_price"]
            oi = quote.get("oi", 0)
            data.append([
                row["date"],
                row["time"],
                row["name"],
                row["tradingsymbol"],
                row["expiry"],
                row["lot_size"],
                row["instrument_token"],
                ltp,
                oi,
                None  # placeholder for change_oi
            ])
            print(f"📈 {row['tradingsymbol']} LTP: {ltp}, OI: {oi}")
        except Exception as e:
            print(f"❌ Failed to fetch LTP for {row['tradingsymbol']}: {e}")

    if not data:
        print("⚠️ No data collected.")
        return

    ws = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    ensure_headers(ws, columns)
    for row in data:
        append_data(ws, row)

if __name__ == "__main__":
    try:
        kite, client = load_kite_client()
        fetch_and_log_futures_oi(kite, client)
        print("✅ Script completed successfully.")
    except Exception as e:
        print("❌ Exception occurred during fetch_futures_oi:", str(e))
        raise

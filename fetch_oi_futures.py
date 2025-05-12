# fetch_oi_futures.py — with classification and anomaly detection
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
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
OI_LOG_SHEET_NAME = "Sheet1"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]
OI_SPIKE_THRESHOLD = 0.2  # 20%
PRICE_DIVERGENCE_THRESHOLD = 0.1  # 10%

# --- Check for weekend or holiday ---
def is_trading_day():
    today = dt.date.today()
    return today.weekday() < 5

# --- Load Zerodha Tokens from Google Sheet ---
def load_kite_client():
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

    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    access_token = sheet.acell("C1").value

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

# --- Detect Current Month Futures Token ---
def get_futures_tokens(kite):
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst.segment == "NFO-FUT") & (df_inst.name.isin(STOCKS))]
    today = dt.date.today()
    return df_fut

# --- Main Entry Point ---
if __name__ == "__main__":
    if not is_trading_day():
        print("Market closed today. Skipping run.")
        sys.exit()

    kite, client = load_kite_client()
    df_fut = get_futures_tokens(kite)
    # Check if Sheet1 has proper headers
    try:
        sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(OI_LOG_SHEET_NAME)
        existing_data = sheet.get_all_values()
        if not existing_data or len(existing_data) < 1 or len(existing_data[0]) < 2:
            sheet.clear()
            headers = ["Timestamp", "Symbol", "Price", "OI", "Classification", "OI Change %", "Anomaly", "Price Divergence %"]
            sheet.append_row(headers)
            print(f"✅ Headers written to {OI_LOG_SHEET_NAME}")
    except Exception as e:
        print(f"⚠️ Could not verify or write headers to {OI_LOG_SHEET_NAME}: {e}")
    print("Fetched futures tokens for analysis.")

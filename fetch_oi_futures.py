import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os
import sys

# CONFIG
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
INTRADAY_SHEET = "Sheet1"
EOD_SHEET = "EOD_Summary"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

INTRADAY_HEADERS = ["Timestamp", "Symbol", "OI", "OI Change (%)", "Price"]
EOD_HEADERS = ["Date", "Symbol", "Start OI", "End OI", "Net OI Change (%)", "Start Price", "End Price", "Price Change (%)", "Classification", "Anomaly"]

def is_trading_day():
    today = dt.date.today()
    return today.weekday() < 5

def load_kite_client():
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

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

def ensure_headers(client, sheet_name, required_headers):
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(sheet_name)
    data = sheet.get_all_values()
    if not data or data[0] != required_headers:
        print(f"⚠️ Updating headers for sheet: {sheet_name}")
        sheet.clear()
        sheet.insert_row(required_headers, 1)
    else:
        print(f"✅ Headers are correct in sheet: {sheet_name}")

def get_futures_tokens(kite):
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst.segment == "NFO-FUT") & (df_inst.name.isin(STOCKS))]
    return df_fut

# --- Main ---
if __name__ == "__main__":
    if not is_trading_day():
        print("Market closed today. Skipping run.")
        sys.exit()

    kite, client = load_kite_client()

    # Ensure headers are present
    ensure_headers(client, INTRADAY_SHEET, INTRADAY_HEADERS)
    ensure_headers(client, EOD_SHEET, EOD_HEADERS)

    df_fut = get_futures_tokens(kite)
    print("✅ Fetched futures tokens for analysis.")

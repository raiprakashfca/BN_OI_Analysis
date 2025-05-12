
# fetch_futures_oi.py — with detailed logging and header safety
import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os

TOKEN_SHEET_NAME = "ZerodhaTokenStore"
FUTURES_OI_SHEET_NAME = "Futures_OI_Log"
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"

HEADERS = ["Date", "Time", "Symbol", "Open Interest", "Change %", "Price", "Action"]

def log(message):
    print(f"🔍 {message}", flush=True)

def is_trading_day():
    today = dt.date.today()
    return today.weekday() < 5

def load_kite_client():
    log("Loading Kite client and Google credentials...")
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
    log("✅ Kite client and token loaded successfully.")
    return kite, client

def ensure_headers(worksheet, headers):
    existing = worksheet.row_values(1)
    if existing != headers:
        log("⚠️ Headers not found or mismatched. Updating headers...")
        worksheet.clear()
        worksheet.insert_row(headers, index=1)
    else:
        log("✅ Headers verified.")

def fetch_and_log_data(kite, sheet):
    log("Fetching instrument list...")
    instruments = pd.DataFrame(kite.instruments("NFO"))
    symbols = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]
    now = dt.datetime.now().strftime("%H:%M")
    today = dt.datetime.now().strftime("%Y-%m-%d")
    worksheet = sheet.worksheet(FUTURES_OI_SHEET_NAME)
    ensure_headers(worksheet, HEADERS)

    log("Scanning futures contracts for target symbols...")
    for symbol in symbols:
        try:
            row = instruments[(instruments.name == symbol) & (instruments.segment == "NFO-FUT")].iloc[0]
            token = int(row["instrument_token"])
            ltp = kite.ltp([token])[str(token)]["last_price"]
            oi = row["open_interest"]
            change = row["change_oi"]
            action = "Logged"
            data = [today, now, symbol, oi, change, ltp, action]
            worksheet.append_row(data)
            log(f"✅ {symbol} logged successfully.")
        except Exception as e:
            log(f"❌ Failed to process {symbol}: {str(e)}")

if __name__ == "__main__":
    log("🚀 Starting fetch_futures_oi.py")
    if not is_trading_day():
        log("⛔ Market closed today.")
        exit()

    kite, client = load_kite_client()
    sheet = client.open_by_key(SHEET_ID)
    fetch_and_log_data(kite, sheet)
    log("🏁 Script completed.")

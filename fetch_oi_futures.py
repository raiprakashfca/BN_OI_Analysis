import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os

# --- CONFIG ---
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
OI_LOG_SHEET_NAME = "Futures_OI_Log"
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
    print("✅ Token valid for user:", sheet.acell("B1").value)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

def get_futures_tokens(kite):
    print("🧠 Caching instrument list...")
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst.segment == "NFO-FUT") & (df_inst.name.isin(STOCKS))]
    return df_fut

def write_to_google_sheet(client, df):
    print("🧾 Preparing to write to Google Sheet...")
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(OI_LOG_SHEET_NAME)
    existing_data = sheet.get_all_values()

    if not existing_data:
        print("📋 Writing headers...")
        sheet.append_row(df.columns.tolist())
    elif df.columns.tolist() != existing_data[0]:
        print("⚠️ Headers in sheet don't match expected headers.")
        print("Expected:", df.columns.tolist())
        print("Found:", existing_data[0])
        raise ValueError("Header mismatch")

    for _, row in df.iterrows():
        sheet.append_row(row.astype(str).tolist())
    print("✅ Successfully wrote data to sheet.")

if __name__ == "__main__":
    if not is_trading_day():
        print("Market closed today. Skipping run.")
        exit()

    kite, client = load_kite_client()
    df_fut = get_futures_tokens(kite)
    print("✅ Retrieved", len(df_fut), "futures instruments.")
    print(df_fut[["name", "instrument_type", "expiry", "strike", "instrument_token"]].head())

    write_to_google_sheet(client, df_fut)

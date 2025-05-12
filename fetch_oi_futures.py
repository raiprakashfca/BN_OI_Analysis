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
FUTURES_SHEET_NAME = "Futures_OI_Log"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]
REQUIRED_HEADERS = ["Date", "Symbol", "Expiry", "Token", "OI", "Lot Size"]

def is_trading_day():
    today = dt.date.today()
    return today.weekday() < 5

def load_kite_client():
    print("🔐 Authenticating with Google Sheets...")
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")

    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += '=' * (4 - padding)
    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))

    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(credentials)

    print("📦 Loading Zerodha tokens from Google Sheets...")
    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    access_token = sheet.acell("C1").value

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    print("✅ Token valid for user:", kite.profile()["user_name"])
    return kite, client

def get_futures_tokens(kite):
    print("🧠 Caching instrument list...")
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst.segment == "NFO-FUT") & (df_inst.name.isin(STOCKS))]
    return df_fut

def fetch_futures_oi(kite, df_fut):
    today = dt.date.today()
    rows = []

    print("📡 Fetching OI data from Zerodha...")
    for _, row in df_fut.iterrows():
        try:
            ltp_data = kite.ltp([row["instrument_token"]])
            oi = ltp_data[str(row["instrument_token"])]["last_price"]
            rows.append({
                "Date": today.strftime("%Y-%m-%d"),
                "Symbol": row["name"],
                "Expiry": row["expiry"],
                "Token": row["instrument_token"],
                "OI": oi,
                "Lot Size": row["lot_size"]
            })
        except Exception as e:
            print(f"⚠️ Error fetching OI for {row['name']}: {e}")

    return pd.DataFrame(rows)

def write_to_google_sheet(client, df):
    print("🧾 Preparing to write to Google Sheet...")
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(FUTURES_SHEET_NAME)
    headers = sheet.row_values(1)

    if not headers or all(h == "" for h in headers):
        print("🆕 Writing headers to sheet...")
        sheet.insert_row(REQUIRED_HEADERS, index=1)
        headers = REQUIRED_HEADERS

    if headers != REQUIRED_HEADERS:
        print(f"⚠️ Headers in sheet don't match expected headers.\nExpected: {REQUIRED_HEADERS}\nFound: {headers}")
        raise ValueError("Header mismatch")

    values = df[REQUIRED_HEADERS].astype(str).values.tolist()
    sheet.append_rows(values)
    print(f"✅ Wrote {len(values)} rows to Google Sheet.")

# --- Main Execution ---
if __name__ == "__main__":
    if not is_trading_day():
        print("🚫 Market closed today. Skipping run.")
    else:
        kite, client = load_kite_client()
        df_fut = get_futures_tokens(kite)
        print("✅ Retrieved", len(df_fut), "futures instruments.\n", df_fut[["name", "instrument_token", "expiry"]].head())
        df_oi = fetch_futures_oi(kite, df_fut)
        write_to_google_sheet(client, df_oi)

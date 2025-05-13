from datetime import datetime
import pandas as pd
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import base64
import json
import os

# --- CONFIG ---
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
GOOGLE_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
TARGET_SHEET_NAME = "Sheet1"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]
EXPECTED_HEADERS = ["Timestamp", "Symbol", "OI", "OI Change (%)", "Price"]

# --- Authenticate with Google Sheets and Kite ---
def load_kite_and_gsheets_clients():
    print("🔐 Authenticating with Google Sheets...")
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += "=" * (4 - padding)
    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(credentials)

    print("📦 Loading Zerodha tokens from Google Sheets...")
    token_sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = token_sheet.acell("A1").value
    access_token = token_sheet.acell("C1").value

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    print(f"✅ Token valid for user: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return kite, client

# --- Write with Header Check ---
def write_to_google_sheet(client, df):
    print("🧾 Preparing to write to Google Sheet...")
    sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(TARGET_SHEET_NAME)
    current_headers = sheet.row_values(1)
    if not current_headers or current_headers != EXPECTED_HEADERS:
        print(f"⚠️ Headers mismatch or missing. Updating headers to: {EXPECTED_HEADERS}")
        sheet.clear()
        sheet.insert_row(EXPECTED_HEADERS, 1)
    rows = df.values.tolist()
    sheet.append_rows(rows)
    print(f"📤 Wrote {len(rows)} rows to Google Sheet.")

# --- Fetch OI Snapshot ---
def fetch_intraday_oi_snapshot(kite):
    print("📡 Fetching OI Snapshot...")
    inst = kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst['name'].isin(STOCKS)) & (df_inst['instrument_type'] == 'FUT')]
    today = datetime.now().date()
    df_fut = df_fut[df_fut['expiry'] >= pd.to_datetime(today)]
    df_latest = df_fut.sort_values(by='expiry').drop_duplicates(subset='name', keep='first')

    tokens = df_latest['instrument_token'].tolist()
    ltp_data = kite.ltp(tokens)

    snapshot = []
    for _, row in df_latest.iterrows():
        symbol = row['name']
        token = row['instrument_token']
        data = ltp_data.get(str(token), {})
        oi = data.get('depth', {}).get('buy', [{}])[0].get('quantity', None)
        price = data.get('last_price', None)
        if oi and price:
            snapshot.append({
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Symbol": symbol,
                "OI": oi,
                "OI Change (%)": 0,  # Placeholder: future enhancement
                "Price": price
            })
    df_snapshot = pd.DataFrame(snapshot)
    print(f"✅ Fetched snapshot for {len(df_snapshot)} stocks.")
    return df_snapshot

# --- MAIN ---
if __name__ == "__main__":
    kite, client = load_kite_and_gsheets_clients()
    df = fetch_intraday_oi_snapshot(kite)
    if df.empty:
        print("❌ No data to write.")
    else:
        write_to_google_sheet(client, df)

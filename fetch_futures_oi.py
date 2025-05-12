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
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
SHEET_NAME = "EOD_Summary"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

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

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

def fetch_eod_data(kite):
    print("📈 Fetching actual EOD data from Zerodha...")
    instruments = kite.instruments()
    df_inst = pd.DataFrame(instruments)
    df_fut = df_inst[(df_inst["segment"] == "NFO-FUT") & (df_inst["name"].isin(STOCKS))]
    
    today = dt.date.today()
    df_latest = df_fut[df_fut["expiry"] > today].sort_values("expiry").drop_duplicates("name")

    all_data = []
    for _, row in df_latest.iterrows():
        try:
            token = row["instrument_token"]
            ohlc = kite.ohlc([token])[str(token)]["ohlc"]
            data = {
                "Date": today.strftime("%Y-%m-%d"),
                "Symbol": row["name"],
                "Start OI": None,
                "End OI": None,
                "Net OI Change (%)": None,
                "Start Price": ohlc["open"],
                "End Price": ohlc["close"],
                "Price Change (%)": round(((ohlc["close"] - ohlc["open"]) / ohlc["open"]) * 100, 2),
                "Classification": "",
                "Anomaly": ""
            }
            all_data.append(data)
        except Exception as e:
            print(f"⚠️ Error fetching EOD for {row['name']}: {e}")

    return pd.DataFrame(all_data)

def write_to_google_sheet(client, df):
    print("🧾 Preparing to write EOD Summary...")
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    existing = sheet.get_all_values()
    headers = ["Date", "Symbol", "Start OI", "End OI", "Net OI Change (%)", "Start Price", "End Price", "Price Change (%)", "Classification", "Anomaly"]
    if not existing:
        sheet.append_row(headers)
        print("✅ Header row written to sheet.")
    elif existing[0] != headers:
        raise ValueError("Header mismatch")

    records = df.values.tolist()
    for row in records:
        sheet.append_row(row)
    print(f"📤 Written {len(records)} rows to Google Sheet.")

if __name__ == "__main__":
    kite, client = load_kite_client()
    df = fetch_eod_data(kite)
    write_to_google_sheet(client, df)
    print("🎯 Script execution complete.")

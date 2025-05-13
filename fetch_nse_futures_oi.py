import requests
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from time import sleep

# --- CONFIG ---
SHEET_NAME = "OI_DailyLog"
TAB_NAME = "Sheet1"
HEADERS = ["Timestamp", "Symbol", "LTP", "OI", "OI Change"]

BANKNIFTY_COMPONENTS = [
    "BANKNIFTY",
    "ICICIBANK",
    "SBIN",
    "HDFCBANK",
    "AXISBANK",
    "KOTAKBANK",
    "BANKBARODA",
    "PNB"
]

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

# --- SHEETS AUTH ---
def authorize_google_sheets():
    creds = Credentials.from_service_account_file("service_account.json", scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ])
    return gspread.authorize(creds)

def validate_and_write_headers(ws):
    if ws.row_values(1) != HEADERS:
        print("🔄 Updating header row...")
        ws.clear()
        ws.append_row(HEADERS)

def write_to_google_sheet(sheet_client, df):
    ws = sheet_client.open(SHEET_NAME).worksheet(TAB_NAME)
    validate_and_write_headers(ws)
    ws.append_rows(df.values.tolist())

# --- NSE SCRAPER ---
def fetch_futures_oi(symbol):
    url = f"https://www.nseindia.com/api/quote-derivative?symbol={symbol}"
    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    try:
        # Get cookies to avoid 403
        session.get("https://www.nseindia.com", timeout=10)
        sleep(1.5)

        res = session.get(url, timeout=10)

        # Check response type
        if "application/json" not in res.headers.get("Content-Type", ""):
            raise ValueError("NSE returned non-JSON response (likely blocked)")

        data = res.json()

        oi = data.get("oi")
        change = data.get("change")
        ltp = data.get("lastPrice")

        return {
            "Symbol": symbol,
            "LTP": ltp,
            "OI": oi,
            "OI Change": change
        }

    except Exception as e:
        print(f"❌ Failed to fetch {symbol}: {e}")
        return None

# --- MAIN ---
def main():
    print("🔐 Connecting to Google Sheets...")
    sheet_client = authorize_google_sheets()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    print("📡 Fetching Futures OI from NSE...")
    for symbol in BANKNIFTY_COMPONENTS:
        data = fetch_futures_oi(symbol)
        if data:
            rows.append([
                timestamp,
                data["Symbol"],
                data["LTP"],
                data["OI"],
                data["OI Change"]
            ])
        sleep(1.0)

    if rows:
        df = pd.DataFrame(rows, columns=HEADERS)
        print(df)
        write_to_google_sheet(sheet_client, df)
        print("✅ Data written to Google Sheet.")
    else:
        print("⚠️ No data fetched.")

if __name__ == "__main__":
    main()

# fetch_oi_futures.py
import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
import time

# --- CONFIG ---
TOKEN_SHEET_NAME = "ZerodhaTokenStore"
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
OI_LOG_SHEET_NAME = "Sheet1"
STOCKS = ["BANKNIFTY", "ICICIBANK", "HDFCBANK", "SBIN", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]

# --- Load Zerodha Tokens from Google Sheet ---
def load_kite_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_file("service_account.json", scopes=scope)
    client = gspread.authorize(credentials)

    sheet = client.open(TOKEN_SHEET_NAME).sheet1
    api_key = sheet.acell("A1").value
    api_secret = sheet.acell("B1").value
    access_token = sheet.acell("C1").value

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, client

# --- Detect Current Month Futures Token ---
def get_futures_tokens(kite):
    inst = kite.instruments("NSE") + kite.instruments("NFO")
    df_inst = pd.DataFrame(inst)
    df_fut = df_inst[(df_inst.segment == "NFO-FUT") & (df_inst.name.isin(STOCKS))]

    # Filter current month expiry only
    today = dt.date.today()
    df_fut["expiry"] = pd.to_datetime(df_fut["expiry"]).dt.date
    df_fut = df_fut[df_fut["expiry"] >= today]
    df_fut = df_fut.sort_values("expiry").drop_duplicates(subset="name", keep="first")

    return df_fut.set_index("tradingsymbol")

# --- Fetch OI Snapshot ---
def fetch_oi_snapshot(kite, df_tokens):
    symbols = df_tokens.index.tolist()
    tokens = df_tokens.instrument_token.tolist()
    quotes = kite.ltp(tokens)
    market_depth = kite.quote(tokens)

    rows = []
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

    for sym, token in zip(symbols, tokens):
        ltp = quotes[token]["last_price"]
        oi = market_depth[token]["depth"]["buy"][0]["oi"]
        vol = market_depth[token]["volume_traded"]
        expiry = df_tokens.loc[sym, "expiry"]

        row = [now, sym, expiry, ltp, vol, oi, "", "", ""]
        rows.append(row)

    return rows

# --- Append to Google Sheet ---
def append_to_sheet(client, rows):
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(OI_LOG_SHEET_NAME)
    existing = sheet.get_all_values()
    if not existing:
        sheet.append_row(["Timestamp", "Symbol", "Expiry", "LTP", "Volume", "OI", "OI Change", "Price Change", "Interpretation"])
    sheet.append_rows(rows)

# --- Main Runner ---
def main():
    try:
        kite, client = load_kite_client()
        df_tokens = get_futures_tokens(kite)
        rows = fetch_oi_snapshot(kite, df_tokens)
        append_to_sheet(client, rows)
        print("✅ OI snapshot logged.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()

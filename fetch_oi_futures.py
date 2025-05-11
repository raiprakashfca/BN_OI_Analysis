# fetch_oi_futures.py — with classification and anomaly detection
import pandas as pd
import datetime as dt
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
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
    credentials = Credentials.from_service_account_file("service_account.json", scopes=scope)
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
    df_fut["expiry"] = pd.to_datetime(df_fut["expiry"]).dt.date
    df_fut = df_fut[df_fut["expiry"] >= today]
    df_fut = df_fut.sort_values("expiry").drop_duplicates(subset="name", keep="first")
    return df_fut.set_index("tradingsymbol")

# --- Load previous snapshot for OI/Price delta comparison ---
def get_previous_snapshot(sheet):
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0]) if len(data) > 1 else pd.DataFrame()
    df = df[df.Timestamp.str.startswith(str(dt.date.today()))]
    df = df.sort_values("Timestamp")
    return df.set_index("Symbol") if not df.empty else pd.DataFrame()

# --- Classify OI Behavior ---
def classify_oi(price_delta, oi_delta):
    if price_delta > 0 and oi_delta > 0:
        return "Long Buildup"
    elif price_delta < 0 and oi_delta > 0:
        return "Short Buildup"
    elif price_delta > 0 and oi_delta < 0:
        return "Short Covering"
    elif price_delta < 0 and oi_delta < 0:
        return "Long Unwinding"
    else:
        return "Unclear"

# --- Fetch OI Snapshot with classification ---
def fetch_oi_snapshot(kite, df_tokens, prev_df):
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

        # Defaults
        oi_chg, px_chg, label, note = "", "", "", ""

        if sym in prev_df.index:
            try:
                prev_oi = float(prev_df.loc[sym, "OI"])
                prev_px = float(prev_df.loc[sym, "LTP"])
                if prev_oi > 0 and prev_px > 0:
                    oi_chg_val = (oi - prev_oi) / prev_oi
                    px_chg_val = (ltp - prev_px) / prev_px
                    oi_chg = round(oi_chg_val * 100, 2)
                    px_chg = round(px_chg_val * 100, 2)
                    label = classify_oi(px_chg_val, oi_chg_val)

                    if abs(oi_chg_val) > OI_SPIKE_THRESHOLD:
                        note = "OI Spike"
                    elif px_chg_val * oi_chg_val < -PRICE_DIVERGENCE_THRESHOLD:
                        note = "Divergence Detected"
            except:
                pass

        row = [now, sym, expiry, ltp, vol, oi, oi_chg, px_chg, label + (" | " + note if note else "")]
        rows.append(row)

    return rows

# --- Append to Google Sheet ---
def append_to_sheet(client, rows):
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(OI_LOG_SHEET_NAME)
    if not sheet.get_all_values():
        sheet.append_row(["Timestamp", "Symbol", "Expiry", "LTP", "Volume", "OI", "OI Change", "Price Change", "Interpretation"])
    sheet.append_rows(rows)

# --- Main Runner ---
def main():
    if not is_trading_day():
        print("⛔ Not a trading day.")
        sys.exit(0)

    try:
        kite, client = load_kite_client()
        df_tokens = get_futures_tokens(kite)
        sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(OI_LOG_SHEET_NAME)
        prev_df = get_previous_snapshot(sheet)
        rows = fetch_oi_snapshot(kite, df_tokens, prev_df)
        append_to_sheet(client, rows)
        print("✅ OI snapshot with classification logged.")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

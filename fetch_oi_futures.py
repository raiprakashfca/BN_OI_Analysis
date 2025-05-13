import pandas as pd
from datetime import datetime, date
from kiteconnect import KiteConnect
import gspread
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
TOKEN_SHEET_KEY = "1mANuCob4dz3jvjigeO1km96vBzZHr-4ZflZEXxR8-qU"
TOKEN_TAB_NAME = "Sheet1"
OI_SHEET = "OI_DailyLog"  # ✅ Updated to correct sheet name
TOP_BANK_STOCKS = ['AXISBANK', 'ICICIBANK', 'SBIN', 'HDFCBANK', 'KOTAKBANK', 'BANKBARODA', 'PNB']
HEADERS = ['Date', 'Time', 'Symbol', 'OI', 'Change']

# ---------------- GOOGLE SHEETS ----------------
def authorize_google_sheets():
    creds = Credentials.from_service_account_file("service_account.json", scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ])
    return gspread.authorize(creds)

def load_zerodha_tokens(sheet_client):
    sheet = sheet_client.open_by_key(TOKEN_SHEET_KEY).worksheet(TOKEN_TAB_NAME)
    api_key = sheet.cell(1, 1).value
    api_secret = sheet.cell(1, 2).value
    access_token = sheet.cell(1, 3).value
    print(f"✅ Token valid for user: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return api_key, api_secret, access_token

def validate_and_write_headers(ws):
    if ws.row_values(1) != HEADERS:
        print("🔄 Updating header row...")
        ws.clear()
        ws.append_row(HEADERS)

def write_to_google_sheet(sheet_client, df):
    ws = sheet_client.open(OI_SHEET).worksheet("Sheet1")  # ✅ Writes to OI_DailyLog → Sheet1
    validate_and_write_headers(ws)
    ws.append_rows(df.values.tolist())

# ---------------- ZERODHA SETUP ----------------
def authenticate_kite(api_key, access_token):
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def get_futures_instruments(kite):
    instruments = kite.instruments()
    df = pd.DataFrame(instruments)
    df = df[df['segment'] == 'NFO-FUT']
    df['expiry'] = pd.to_datetime(df['expiry'])
    return df

# ---------------- OI FETCH LOGIC ----------------
def fetch_intraday_oi_snapshot(kite):
    today = date.today()
    now = datetime.now().strftime("%H:%M")
    data = []

    print("📡 Fetching OI Snapshot...")
    df_fut = get_futures_instruments(kite)
    df_fut = df_fut[df_fut['expiry'] >= pd.Timestamp(today)]

    for symbol in TOP_BANK_STOCKS:
        contracts = df_fut[df_fut['name'] == symbol]
        if not contracts.empty:
            contract = contracts.sort_values(by='expiry').iloc[0]
            try:
                quote = kite.ltp(contract['instrument_token'])
                oi = quote[str(contract['instrument_token'])]['depth']['sell'][0]['quantity']
                data.append([str(today), now, symbol, oi, 0])  # Change placeholder = 0
                print(f"✅ {symbol}: OI = {oi}")
            except Exception as e:
                print(f"❌ Error fetching OI for {symbol}: {e}")
        else:
            print(f"⚠️ No valid future found for {symbol}")
    return pd.DataFrame(data, columns=HEADERS)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("🔐 Authenticating with Google Sheets...")
    gclient = authorize_google_sheets()

    print("📦 Loading Zerodha tokens from Google Sheets...")
    api_key, api_secret, access_token = load_zerodha_tokens(gclient)

    print("🔗 Connecting to Kite...")
    kite = authenticate_kite(api_key, access_token)

    df = fetch_intraday_oi_snapshot(kite)
    if not df.empty:
        print("📤 Writing intraday OI snapshot to Google Sheets...")
        write_to_google_sheet(gclient, df)
        print("✅ Done.")
    else:
        print("⚠️ No OI data captured.")

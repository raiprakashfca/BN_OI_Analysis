import pandas as pd
import datetime
from kiteconnect import KiteConnect
import gspread
from google.oauth2.service_account import Credentials

# ------------- CONFIGURATION -------------
GSHEET_NAME = "Futures_OI_Log"
TOKEN_SHEET = "ZerodhaTokenStore"
TOP_BANK_STOCKS = ['AXISBANK', 'ICICIBANK', 'SBIN', 'HDFCBANK', 'KOTAKBANK', 'BANKBARODA', 'PNB']
REQUIRED_HEADERS = ['Date', 'Time', 'Symbol', 'OI', 'Change']

# ------------- GOOGLE SHEETS UTILS -------------
def authorize_google_sheets():
    creds = Credentials.from_service_account_file("service_account.json", scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ])
    client = gspread.authorize(creds)
    return client

def load_zerodha_tokens(sheet_client):
    sheet = sheet_client.open(TOKEN_SHEET).sheet1
    api_key = sheet.cell(1, 1).value
    api_secret = sheet.cell(1, 2).value
    access_token = sheet.cell(1, 3).value
    return api_key, api_secret, access_token

def validate_and_write_headers(ws):
    headers = ws.row_values(1)
    if headers != REQUIRED_HEADERS:
        print("🔄 Updating header row...")
        ws.clear()
        ws.append_row(REQUIRED_HEADERS)

def write_to_google_sheet(sheet_client, df):
    ws = sheet_client.open(GSHEET_NAME).sheet1
    validate_and_write_headers(ws)
    ws.append_rows(df.values.tolist())

# ------------- ZERODHA UTILS -------------
def authenticate_kite(api_key, access_token):
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def get_futures_instruments(kite):
    instruments = kite.instruments()
    df = pd.DataFrame(instruments)
    df = df[df['segment'] == 'NFO-FUT']
    return df

# ------------- MAIN FETCH LOGIC -------------
def fetch_futures_oi(kite, df_instruments):
    today = datetime.date.today()
    now = datetime.datetime.now().strftime("%H:%M")
    data = []

    for symbol in TOP_BANK_STOCKS:
        rows = df_instruments[
            (df_instruments['name'] == symbol) &
            (df_instruments['expiry'] > pd.Timestamp(today))
        ]
        if not rows.empty:
            row = rows.sort_values(by='expiry').iloc[0]
            try:
                quote = kite.ltp(row['instrument_token'])
                oi = quote[str(row['instrument_token'])]['depth']['sell'][0]['quantity']
                data.append([str(today), now, symbol, oi, 0])  # Placeholder 0 for change
                print(f"✅ {symbol}: OI = {oi}")
            except Exception as e:
                print(f"❌ Error fetching OI for {symbol}: {e}")
        else:
            print(f"⚠️ No valid futures contract for {symbol}")

    return pd.DataFrame(data, columns=REQUIRED_HEADERS)

# ------------- RUN SCRIPT -------------
if __name__ == "__main__":
    print("🔐 Authenticating Google Sheets...")
    client = authorize_google_sheets()

    print("📦 Loading Zerodha credentials...")
    api_key, api_secret, access_token = load_zerodha_tokens(client)

    print("🔗 Connecting to Kite...")
    kite = authenticate_kite(api_key, access_token)

    print("🧠 Downloading instrument list...")
    df_instruments = get_futures_instruments(kite)

    print("📊 Fetching futures OI data...")
    df_oi = fetch_futures_oi(kite, df_instruments)

    if not df_oi.empty:
        print("📤 Writing to Google Sheet...")
        write_to_google_sheet(client, df_oi)
        print("✅ Done.")
    else:
        print("⚠️ No data fetched.")

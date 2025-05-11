# fetch_futures_oi.py — Daily OI Logger for Futures
import pandas as pd
from kiteconnect import KiteConnect
from google.oauth2.service_account import Credentials
import gspread
from datetime import datetime
import os

# --- Constants ---
SYMBOLS = ["BANKNIFTY", "HDFCBANK", "SBIN", "ICICIBANK", "AXISBANK", "KOTAKBANK", "PNB", "BANKBARODA"]
SHEET_NAME = "OI_DailyLog"
TAB_NAME = "Futures_OI_Log"

# --- Load credentials from environment secrets ---
creds_dict = {
    "type": os.getenv("GDRIVE_TYPE"),
    "project_id": os.getenv("GDRIVE_PROJECT_ID"),
    "private_key_id": os.getenv("GDRIVE_PRIVATE_KEY_ID"),
    "private_key": os.getenv("GDRIVE_PRIVATE_KEY").replace("\\n", "\n"),
    "client_email": os.getenv("GDRIVE_CLIENT_EMAIL"),
    "client_id": os.getenv("GDRIVE_CLIENT_ID"),
    "auth_uri": os.getenv("GDRIVE_AUTH_URI"),
    "token_uri": os.getenv("GDRIVE_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("GDRIVE_AUTH_PROVIDER_CERT"),
    "client_x509_cert_url": os.getenv("GDRIVE_CLIENT_CERT"),
    "universe_domain": os.getenv("GDRIVE_UNIVERSE_DOMAIN")
}
credentials = Credentials.from_service_account_info(creds_dict)
client = gspread.authorize(credentials)
sheet = client.open(SHEET_NAME)
worksheet = sheet.worksheet(TAB_NAME)

# --- Kite Init ---
kite = KiteConnect(api_key=os.getenv("ZERODHA_API_KEY"))
kite.set_access_token(os.getenv("ZERODHA_ACCESS_TOKEN"))

# --- Fetch Instruments ---
instruments = kite.instruments("NSE") + kite.instruments("NFO")
df_instruments = pd.DataFrame(instruments)

# --- Filter for Futures ---
fut_rows = []
today_str = datetime.now().strftime("%Y-%m-%d")

for symbol in SYMBOLS:
    df_fut = df_instruments[
        (df_instruments["name"] == symbol) &
        (df_instruments["segment"] == "NFO-FUT")
    ].copy()

    for _, row in df_fut.iterrows():
        try:
            quote = kite.ltp([row["instrument_token"]])[str(row["instrument_token"])]
            oi = quote.get("depth", {}).get("buy", [{}])[0].get("quantity", None)
            fut_rows.append({
                "Date": today_str,
                "Symbol": symbol,
                "Expiry": row["expiry"],
                "Token": row["instrument_token"],
                "OI": oi,
                "Lot Size": row["lot_size"]
            })
        except Exception as e:
            print(f"⚠️ Failed for {symbol} — {e}")

# --- Write to Google Sheet ---
df_out = pd.DataFrame(fut_rows)
if not df_out.empty:
    worksheet.append_rows(df_out.values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ Logged {len(df_out)} futures OI entries to Google Sheet")
else:
    print("⚠️ No futures data found to log.")

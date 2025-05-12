from datetime import datetime
import pandas as pd
import gspread
import json
import base64
import os
from google.oauth2.service_account import Credentials

# --- CONFIG ---
GSHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
SHEET_NAME = "Rollover_Analysis"
REQUIRED_COLUMNS = ["Symbol", "Near Expiry", "Far Expiry", "Near OI", "Far OI", "Rollover %", "Date"]

def load_gsheet_client():
    print("🔐 Authenticating with Google Sheets...")

    creds_b64 = os.environ.get("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")

    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += '=' * (4 - padding)

    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)

    return gspread.authorize(credentials)

def write_to_google_sheet(client, df):
    print("🧾 Preparing to write to Google Sheet...")

    sheet = client.open_by_key(GSHEET_ID).worksheet(SHEET_NAME)
    existing = sheet.get_all_values()
    if not existing or existing[0] != REQUIRED_COLUMNS:
        print("📄 Writing headers to Google Sheet...")
        sheet.update("A1", [REQUIRED_COLUMNS])
    else:
        print("✅ Headers already exist.")

    print("✍️ Appending data to sheet...")
    sheet.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
    print("✅ Data written successfully.")

def simulate_rollover_data():
    print("🔢 Simulating dummy rollover data...")
    today = datetime.now().strftime("%Y-%m-%d")
    return pd.DataFrame([
        ["BANKNIFTY", "2025-05-30", "2025-06-27", 1000000, 250000, 25.0, today],
        ["ICICIBANK", "2025-05-30", "2025-06-27", 800000, 200000, 25.0, today],
        ["HDFCBANK", "2025-05-30", "2025-06-27", 750000, 300000, 28.6, today],
    ], columns=REQUIRED_COLUMNS)

# --- MAIN ---
if __name__ == "__main__":
    print("🚀 Starting Rollover Analysis Summary...")

    try:
        client = load_gsheet_client()
        df = simulate_rollover_data()
        write_to_google_sheet(client, df)
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        raise e

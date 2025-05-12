
import pandas as pd
import datetime as dt
from google.oauth2.service_account import Credentials
import gspread

# --- CONFIG ---
ROLLOVER_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
FUTURES_LOG_SHEET = "Futures_OI_Log"
ROLLOVER_ANALYSIS_SHEET = "Rollover_Analysis"
LOT_SIZES = {
    "BANKNIFTY": 15,
    "ICICIBANK": 1375,
    "HDFCBANK": 550,
    "SBIN": 1500,
    "AXISBANK": 1200,
    "KOTAKBANK": 400,
    "PNB": 8000,
    "BANKBARODA": 5400
}

# --- Load Google Sheet Client ---
def load_gsheet_client():
    credentials = Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(credentials)

# --- Read and Analyze Data ---
def analyze_rollover():
    client = load_gsheet_client()
    sheet = client.open_by_key(ROLLOVER_SHEET_ID)

    try:
        data = sheet.worksheet(FUTURES_LOG_SHEET).get_all_values()
    except Exception as e:
        print(f"❌ Failed to read Futures_OI_Log: {e}")
        return

    df = pd.DataFrame(data[1:], columns=data[0])
    if df.empty:
        print("⚠️ No data found in Futures_OI_Log")
        return

    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['OI'] = pd.to_numeric(df['OI'], errors='coerce')
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

    latest = df.sort_values('Timestamp').groupby('Symbol').last().reset_index()
    earliest = df.sort_values('Timestamp').groupby('Symbol').first().reset_index()

    merged = pd.merge(latest, earliest, on='Symbol', suffixes=('_latest', '_earliest'))
    merged['OI_Change_%'] = ((merged['OI_latest'] - merged['OI_earliest']) / merged['OI_earliest']) * 100
    merged['Price_Change_%'] = ((merged['Price_latest'] - merged['Price_earliest']) / merged['Price_earliest']) * 100

    def classify(row):
        if row['OI_Change_%'] > 0 and row['Price_Change_%'] > 0:
            return "Long Buildup"
        elif row['OI_Change_%'] < 0 and row['Price_Change_%'] < 0:
            return "Long Unwinding"
        elif row['OI_Change_%'] > 0 and row['Price_Change_%'] < 0:
            return "Short Buildup"
        elif row['OI_Change_%'] < 0 and row['Price_Change_%'] > 0:
            return "Short Covering"
        else:
            return "Neutral"

    merged['Category'] = merged.apply(classify, axis=1)
    merged['Date'] = dt.date.today().strftime('%Y-%m-%d')

    result = merged[['Date', 'Symbol', 'OI_Change_%', 'Price_Change_%', 'Category']]

    # --- Write to Rollover_Analysis sheet ---
    try:
        ws = sheet.worksheet(ROLLOVER_ANALYSIS_SHEET)
        existing = ws.get_all_values()
        if not existing:
            ws.append_row(result.columns.tolist())
        elif result.columns.tolist() != existing[0]:
            print("⚠️ Header mismatch in Rollover_Analysis. Consider manual fix.")
        for row in result.itertuples(index=False, name=None):
            ws.append_row(list(row))
        print(f"✅ Rollover summary updated: {len(result)} rows")
    except Exception as e:
        print(f"❌ Failed to write to Rollover_Analysis: {e}")

# --- Main ---
if __name__ == "__main__":
    analyze_rollover()

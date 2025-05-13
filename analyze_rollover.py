import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ------------- CONFIGURATION -------------
TOKEN_SHEET = "ZerodhaTokenStore"
OI_LOG_SHEET = "Futures_OI_Log"
SUMMARY_SHEET = "Rollover_Summary"
HEADERS = ['Symbol', 'Prev OI', 'Curr OI', 'Change', 'Change %']

# ------------- GOOGLE SHEETS UTILS -------------
def authorize_google_sheets():
    creds = Credentials.from_service_account_file("service_account.json", scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ])
    client = gspread.authorize(creds)
    return client

def validate_and_write_headers(ws):
    existing = ws.row_values(1)
    if existing != HEADERS:
        print("🔄 Updating header row...")
        ws.clear()
        ws.append_row(HEADERS)

def write_summary_to_sheet(sheet_client, summary_df):
    ws = sheet_client.open(SUMMARY_SHEET).sheet1
    validate_and_write_headers(ws)
    ws.append_rows(summary_df.values.tolist())

# ------------- CORE LOGIC -------------
def load_oi_log(sheet_client):
    ws = sheet_client.open(OI_LOG_SHEET).sheet1
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])
    return df

def analyze_rollover(df):
    today = df['Date'].max()
    prev_day = today - timedelta(days=1)

    # Get last two trading days (by date, not calendar logic)
    df_today = df[df['Date'] == today]
    df_prev = df[df['Date'] == prev_day]

    if df_today.empty or df_prev.empty:
        print("⚠️ Not enough data to compute rollover.")
        return pd.DataFrame()

    merged = pd.merge(df_prev, df_today, on='Symbol', suffixes=('_prev', '_curr'))
    merged['Change'] = merged['OI_curr'] - merged['OI_prev']
    merged['Change %'] = ((merged['Change']) / merged['OI_prev']) * 100

    summary = merged[['Symbol', 'OI_prev', 'OI_curr', 'Change', 'Change %']].copy()
    summary.columns = HEADERS
    summary = summary.sort_values(by='Change %', ascending=False)

    return summary.round(2)

# ------------- MAIN EXECUTION -------------
if __name__ == "__main__":
    print("🔐 Connecting to Google Sheets...")
    gclient = authorize_google_sheets()

    print("📥 Reading OI log data...")
    df_oi = load_oi_log(gclient)

    print("📊 Analyzing rollover...")
    df_summary = analyze_rollover(df_oi)

    if not df_summary.empty:
        print("📤 Writing summary to Google Sheet...")
        write_summary_to_sheet(gclient, df_summary)
        print("✅ Rollover analysis complete.")
    else:
        print("⚠️ No summary generated.")

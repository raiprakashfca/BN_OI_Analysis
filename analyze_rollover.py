import pandas as pd
import datetime as dt
from google.oauth2.service_account import Credentials
import gspread
import json
import base64
import os

# --- CONFIG ---
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
FUTURES_SHEET_NAME = "Futures_OI_Log"
ROLLOVER_SHEET_NAME = "Rollover_Analysis"
REQUIRED_HEADERS = ["Date", "Symbol", "Expiry", "Token", "OI", "Lot Size"]
ROLLOVER_HEADERS = ["Date", "Symbol", "Near Expiry", "Far Expiry", "Near OI", "Far OI", "Rollover %"]

# --- Load GSheet Client ---
def load_gsheet_client():
    print("🔐 Authenticating with Google Sheets...")
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += '=' * (4 - padding)
    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(credentials)
    return client

# --- Load Data from Futures Sheet ---
def load_futures_data(client):
    print("📥 Loading Futures OI Sheet...")
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(FUTURES_SHEET_NAME)
    headers = sheet.row_values(1)
    if headers != REQUIRED_HEADERS:
        raise ValueError(f"Header mismatch in {FUTURES_SHEET_NAME}. Expected: {REQUIRED_HEADERS} but found: {headers}")
    rows = sheet.get_all_values()
    df = pd.DataFrame(rows[1:], columns=headers)
    df["Date"] = pd.to_datetime(df["Date"])
    df["Expiry"] = pd.to_datetime(df["Expiry"])
    df["OI"] = pd.to_numeric(df["OI"], errors="coerce")
    return df

# --- Analyze Rollover ---
def analyze_rollover(df):
    print("🧠 Performing rollover analysis...")
    today = df["Date"].max()
    df_today = df[df["Date"] == today]

    result = []
    for symbol in df_today["Symbol"].unique():
        df_sym = df_today[df_today["Symbol"] == symbol].sort_values("Expiry")
        if len(df_sym) >= 2:
            near = df_sym.iloc[0]
            far = df_sym.iloc[1]
            total_oi = near["OI"] + far["OI"]
            rollover_pct = round((far["OI"] / total_oi) * 100, 2) if total_oi > 0 else 0.0
            result.append({
                "Date": today.strftime("%Y-%m-%d"),
                "Symbol": symbol,
                "Near Expiry": near["Expiry"].strftime("%Y-%m-%d"),
                "Far Expiry": far["Expiry"].strftime("%Y-%m-%d"),
                "Near OI": near["OI"],
                "Far OI": far["OI"],
                "Rollover %": rollover_pct
            })
    return pd.DataFrame(result)

# --- Write to Google Sheet ---
def write_to_google_sheet(client, df_roll):
    print("📤 Writing rollover summary...")
    sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(ROLLOVER_SHEET_NAME)
    headers = sheet.row_values(1)
    if not headers or headers != ROLLOVER_HEADERS:
        print("🆕 Writing headers to sheet...")
        sheet.update("A1", [ROLLOVER_HEADERS])
    values = df_roll[ROLLOVER_HEADERS].astype(str).values.tolist()
    sheet.append_rows(values)
    print(f"✅ Wrote {len(values)} rows to {ROLLOVER_SHEET_NAME}.")

# --- Main Execution ---
if __name__ == "__main__":
    client = load_gsheet_client()
    df_fut = load_futures_data(client)
    df_roll = analyze_rollover(df_fut)
    write_to_google_sheet(client, df_roll)

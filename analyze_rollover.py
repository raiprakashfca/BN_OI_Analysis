# analyze_rollover.py — with full logging and header safeguard

import pandas as pd
import datetime as dt
from google.oauth2.service_account import Credentials
import gspread
import base64
import json
import os

# --- CONFIG ---
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
ROLLOVER_SHEET = "Rollover_Analysis"
EOD_SHEET = "EOD_Summary"
REQUIRED_HEADERS = ['Stock', 'Open OI', 'Close OI', 'Net % OI Change', 'Price Change %', 'Rollover Category']

def load_gsheet_client():
    print("[INFO] Loading Google Sheet client...")
    creds_b64 = os.getenv("SERVICE_ACCOUNT_JSON_B64")
    if not creds_b64:
        raise ValueError("Missing SERVICE_ACCOUNT_JSON_B64")
    padding = len(creds_b64) % 4
    if padding:
        creds_b64 += "=" * (4 - padding)
    creds_dict = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(credentials)
    print("[INFO] Google Sheet client loaded.")
    return client

def safe_fetch_worksheet(client, sheet_name):
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
        data = sheet.get_all_values()
        if len(data) <= 1:
            print(f"[WARNING] No data or only headers in sheet: {sheet_name}")
            return pd.DataFrame(), sheet
        df = pd.DataFrame(data[1:], columns=data[0])
        return df, sheet
    except Exception as e:
        print(f"[ERROR] Failed to load Google Sheet '{sheet_name}': {e}")
        return pd.DataFrame(), None

def ensure_headers(sheet, expected_headers):
    current_headers = sheet.row_values(1)
    if current_headers != expected_headers:
        print("[INFO] Writing headers to sheet.")
        sheet.update('A1', [expected_headers])

def categorize_rollover(change_pct):
    if change_pct > 15:
        return "Strong Long Buildup"
    elif change_pct > 5:
        return "Mild Long Buildup"
    elif change_pct < -15:
        return "Aggressive Short Covering"
    elif change_pct < -5:
        return "Mild Short Covering"
    else:
        return "Neutral"

if __name__ == "__main__":
    print("🔍 Running analyze_rollover.py with debug logging...")
    client = load_gsheet_client()
    
    df_eod, _ = safe_fetch_worksheet(client, EOD_SHEET)
    if df_eod.empty:
        print("[ERROR] EOD_Summary is empty. Exiting.")
        exit(1)

    df_rollover = []
    for _, row in df_eod.iterrows():
        try:
            stock = row["Stock"]
            open_oi = float(row.get("Open OI", 0))
            close_oi = float(row.get("Close OI", 0))
            price_change = float(row.get("Price Change %", 0))

            if open_oi == 0:
                continue

            net_change = ((close_oi - open_oi) / open_oi) * 100
            category = categorize_rollover(net_change)

            df_rollover.append({
                "Stock": stock,
                "Open OI": open_oi,
                "Close OI": close_oi,
                "Net % OI Change": round(net_change, 2),
                "Price Change %": round(price_change, 2),
                "Rollover Category": category
            })

            print(f"✅ {stock}: {category} ({net_change:.2f}%)")
        except Exception as e:
            print(f"[ERROR] Failed to process row: {row} → {e}")

    df_final = pd.DataFrame(df_rollover)
    ws_rollover = client.open_by_key(SHEET_ID).worksheet(ROLLOVER_SHEET)
    ensure_headers(ws_rollover, REQUIRED_HEADERS)

    if not df_final.empty:
        ws_rollover.update(f"A2", df_final.values.tolist())
        print("📈 Rollover analysis updated.")
    else:
        print("⚠️ No valid data to update.")

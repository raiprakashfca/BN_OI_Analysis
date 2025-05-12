
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import json

# --- Constants ---
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
INTRADAY_SHEET = "Sheet1"
EOD_SHEET = "EOD_Summary"
REQUIRED_HEADERS_INTRADAY = ["Timestamp", "Symbol", "OI", "Price", "Classification", "Anomaly", "Divergence"]
REQUIRED_HEADERS_EOD = ["Date", "Symbol", "Net % OI Change", "Price Change", "Category"]

# --- Load GSheet Client ---
@st.cache_resource
def load_gsheet_client():
    creds_dict = json.loads(st.secrets["SERVICE_ACCOUNT_JSON"])
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(credentials)

# --- Ensure Headers Exist ---
def ensure_headers(sheet, required_headers):
    current_values = sheet.get_all_values()
    if not current_values or current_values[0] != required_headers:
        sheet.clear()
        sheet.insert_row(required_headers, 1)

# --- Load Sheet Data with Header Check ---
def load_sheet(sheet_name, required_headers):
    client = load_gsheet_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    ensure_headers(sheet, required_headers)
    data = sheet.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0]) if len(data) > 1 else pd.DataFrame(columns=data[0])

# --- Streamlit UI ---
st.title("📊 BankNifty OI Visual Dashboard")

try:
    df_intraday = load_sheet(INTRADAY_SHEET, REQUIRED_HEADERS_INTRADAY)
    df_eod = load_sheet(EOD_SHEET, REQUIRED_HEADERS_EOD)

    # Display feedback
    st.success("✅ Data loaded successfully.")

    # Sample Analysis: Show daily summary table
    if not df_eod.empty:
        st.subheader("📅 End of Day Summary")
        st.dataframe(df_eod)

    # Sample Filtering
    if not df_intraday.empty:
        st.subheader("🔍 Intraday Data Explorer")
        date_options = sorted(df_intraday['Timestamp'].str.slice(0, 10).unique())[::-1]
        selected_date = st.selectbox("Select Date", date_options)
        st.dataframe(df_intraday[df_intraday["Timestamp"].str.startswith(selected_date)])

except Exception as e:
    st.error(f"❌ {type(e).__name__}: {str(e)}")

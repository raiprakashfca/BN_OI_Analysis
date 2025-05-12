
# oi_dashboard.py — Streamlit dashboard for OI analysis
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime

# --- Google Sheets Setup ---
SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
INTRADAY_SHEET = "Sheet1"
EOD_SHEET = "EOD_Summary"

@st.cache_resource
def load_gsheet_client():
    credentials = Credentials.from_service_account_info(st.secrets["google_service_account"])
    return gspread.authorize(credentials)

def load_sheet(sheet_name):
    client = load_gsheet_client()
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
        data = sheet.get_all_values()
        if not data or len(data) <= 1:
            st.warning(f"⚠️ No data or only headers in sheet: {sheet_name}")
            return pd.DataFrame()
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except Exception as e:
        st.error(f"❌ Failed to load Google Sheet '{sheet_name}': {e}")
        return pd.DataFrame()

# --- Streamlit UI ---
st.set_page_config(page_title="BankNifty OI Dashboard", layout="wide")
st.title("📊 BankNifty OI Visual Dashboard")

# --- Load Data ---
df_intraday = load_sheet(INTRADAY_SHEET)
df_eod = load_sheet(EOD_SHEET)

# --- Sidebar Controls ---
if not df_intraday.empty and 'Timestamp' in df_intraday.columns:
    date_options = sorted(df_intraday['Timestamp'].str.slice(0, 10).unique())[::-1]
    selected_date = st.sidebar.selectbox("📅 Select Date", date_options)

    st.subheader(f"📈 Intraday OI Changes for {selected_date}")
    filtered = df_intraday[df_intraday["Timestamp"].str.startswith(selected_date)]
    st.dataframe(filtered)

if not df_eod.empty:
    st.subheader("🧾 Daily Summary (EOD)")
    st.dataframe(df_eod)

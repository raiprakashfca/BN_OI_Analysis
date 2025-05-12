# oi_dashboard.py – Streamlit visual dashboard for BankNIFTY OI Analysis
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import matplotlib.pyplot as plt
import datetime as dt

# --- CONFIG ---
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
INTRADAY_SHEET = "Sheet1"
EOD_SHEET = "EOD_Summary"

# --- Authenticate and Connect to Google Sheets ---
@st.cache_resource
def load_gsheet_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = Credentials.from_service_account_info(st.secrets["google_service_account"], scopes=scope)
    return gspread.authorize(credentials)

# --- Load Sheet Data with Header Check ---
def load_sheet(sheet_name):
    try:
        client = load_gsheet_client()
        sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(sheet_name)
        data = sheet.get_all_values()
        if len(data) <= 1:
            st.warning(f"⚠️ No data or only headers in sheet: {sheet_name}")
            return pd.DataFrame()
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except Exception as e:
        st.error(f"❌ Failed to load Google Sheet '{sheet_name}': {e}")
        return pd.DataFrame()

# --- Visualization Logic ---
def plot_intraday(df, selected_date):
    df_day = df[df["Timestamp"].str.startswith(selected_date)]
    if df_day.empty:
        st.info("No intraday data available for selected date.")
        return

    df_day["Timestamp"] = pd.to_datetime(df_day["Timestamp"])
    numeric_cols = ["OI", "Price", "OI_Change_pct", "Price_Change_pct"]
    for col in numeric_cols:
        df_day[col] = pd.to_numeric(df_day[col], errors="coerce")

    st.subheader("📈 Intraday OI and Price Chart")
    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax1.plot(df_day["Timestamp"], df_day["OI"], label="OI", color="blue")
    ax1.set_ylabel("OI", color="blue")
    ax2 = ax1.twinx()
    ax2.plot(df_day["Timestamp"], df_day["Price"], label="Price", color="green")
    ax2.set_ylabel("Price", color="green")
    st.pyplot(fig)

    st.subheader("📉 OI % Change vs Price % Change")
    fig2, ax = plt.subplots(figsize=(10, 4))
    ax.plot(df_day["Timestamp"], df_day["OI_Change_pct"], label="OI % Change", color="purple")
    ax.plot(df_day["Timestamp"], df_day["Price_Change_pct"], label="Price % Change", color="orange")
    ax.axhline(0, linestyle="--", color="grey")
    ax.legend()
    st.pyplot(fig2)

# --- Streamlit UI ---
st.set_page_config(page_title="BankNifty OI Dashboard", layout="wide")
st.title("📊 BankNifty OI Visual Dashboard")

# --- Load Data ---
df_intraday = load_sheet(INTRADAY_SHEET)
df_eod = load_sheet(EOD_SHEET)

# --- Sidebar Controls ---
if not df_intraday.empty and "Timestamp" in df_intraday.columns:
    date_options = sorted(df_intraday['Timestamp'].str.slice(0, 10).unique())[::-1]
    selected_date = st.sidebar.selectbox("Select Date", date_options)
    plot_intraday(df_intraday, selected_date)

# --- Show EOD Summary Table ---
if not df_eod.empty:
    st.subheader("📋 EOD Summary")
    st.dataframe(df_eod)

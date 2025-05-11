# oi_dashboard.py — Streamlit visualizer using secure JSON secrets
import streamlit as st
import pandas as pd
import datetime as dt
from google.oauth2.service_account import Credentials
import gspread
import altair as alt

# --- CONFIG ---
OI_LOG_SHEET_ID = "1ZYjZ0LXbaD69X3U-VcN0Qh3KwtHO9gMXPBdzUuzkCeM"
INTRADAY_SHEET = "Sheet1"
EOD_SHEET = "EOD_Summary"

# --- Google Sheets Auth via structured secret ---
@st.cache_resource
def load_gsheet_client():
    credentials = Credentials.from_service_account_info(st.secrets["google_service_account"])
    return gspread.authorize(credentials)

# --- Load Sheet Data ---
def load_sheet(sheet_name):
    try:
        client = load_gsheet_client()
        # Diagnostic: show available sheets
        sheet_titles = [ws.title for ws in client.open_by_key(OI_LOG_SHEET_ID).worksheets()]
        st.write("✅ Available sheets:", sheet_titles)

        sheet = client.open_by_key(OI_LOG_SHEET_ID).worksheet(sheet_name)
        data = sheet.get_all_values()
        return pd.DataFrame(data[1:], columns=data[0]) if len(data) > 1 else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Failed to load Google Sheet '{sheet_name}': {e}")
        raise e

# --- App Layout ---
st.set_page_config(page_title="BankNifty OI Dashboard", layout="wide")
st.title("📊 BankNifty OI Visual Dashboard")

# --- Load Data ---
df_intraday = load_sheet(INTRADAY_SHEET)
df_eod = load_sheet(EOD_SHEET)

# --- Sidebar Controls ---
date_options = sorted(df_intraday['Timestamp'].str.slice(0, 10).unique())[::-1]
selected_date = st.sidebar.selectbox("Select Date", date_options)
df_day = df_intraday[df_intraday['Timestamp'].str.startswith(selected_date)]

symbols = sorted(df_day['Symbol'].unique())
selected_symbol = st.sidebar.selectbox("Select Symbol", symbols)
df_symbol = df_day[df_day['Symbol'] == selected_symbol]

# --- Intraday OI + Price Chart ---
st.subheader(f"Intraday LTP & OI — {selected_symbol} on {selected_date}")
df_symbol["Timestamp"] = pd.to_datetime(df_symbol["Timestamp"])
df_symbol["LTP"] = pd.to_numeric(df_symbol["LTP"], errors="coerce")
df_symbol["OI"] = pd.to_numeric(df_symbol["OI"], errors="coerce")

chart = alt.layer(
    alt.Chart(df_symbol).mark_line(color='blue').encode(
        x='Timestamp',
        y=alt.Y('LTP', axis=alt.Axis(title='LTP')),
        tooltip=['Timestamp', 'LTP']
    ),
    alt.Chart(df_symbol).mark_line(color='orange').encode(
        x='Timestamp',
        y=alt.Y('OI', axis=alt.Axis(title='Open Interest')),
        tooltip=['Timestamp', 'OI']
    )
).resolve_scale(
    y = 'independent'
).interactive()

st.altair_chart(chart, use_container_width=True)

# --- EOD Summary Table ---
st.subheader("🧾 EOD Summary")
df_eod_filtered = df_eod[df_eod["Date"] == selected_date]
df_eod_filtered["% Price Chg"] = pd.to_numeric(df_eod_filtered["% Price Chg"], errors="coerce")
df_eod_filtered["% OI Chg"] = pd.to_numeric(df_eod_filtered["% OI Chg"], errors="coerce")
st.dataframe(df_eod_filtered, use_container_width=True)

# --- Highlights ---
st.subheader("🔥 Daily Movers")
if not df_eod_filtered.empty:
    top_price = df_eod_filtered.sort_values("% Price Chg", ascending=False).head(3)
    top_oi = df_eod_filtered.sort_values("% OI Chg", ascending=False).head(3)

    col1, col2 = st.columns(2)
    col1.metric("Top Price Gainer", top_price.iloc[0]['Symbol'], f"{top_price.iloc[0]['% Price Chg']}%")
    col2.metric("Top OI Gainer", top_oi.iloc[0]['Symbol'], f"{top_oi.iloc[0]['% OI Chg']}%")

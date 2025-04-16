import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import pytz
import os
from io import BytesIO

# --- Constants ---
IST = pytz.timezone('Asia/Kolkata')
today = datetime.now(IST)
today_str = today.strftime("%Y%m%d")
csv_filename = f"crude_pnl_{today_str}.csv"

# --- Streamlit Setup ---
st.set_page_config(page_title="Crude PnL Tracker", layout="centered")
st.title("🛢️ Crude PnL Tracker")

# --- Tabs ---
tab1, tab2 = st.tabs(["📥 New Entry", "📊 View History"])

# ------------------------- #
# Tab 1: New Entry Section
# ------------------------- #
with tab1:
    pnl_value = st.number_input("Enter PnL Value", step=1.0)
    if st.button("Submit"):
        timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        new_data = pd.DataFrame([[timestamp, pnl_value]], columns=["Timestamp", "PnL"])

        # Save CSV
        if os.path.exists(csv_filename):
            new_data.to_csv(csv_filename, mode='a', header=False, index=False)
        else:
            new_data.to_csv(csv_filename, index=False)

        st.success("PnL Value Saved!")

    # --- Show Today's Chart ---
    if os.path.exists(csv_filename):
        df = pd.read_csv(csv_filename)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.tz_localize("Asia/Kolkata")

        # Key info
        first_time = df["Timestamp"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        last_time = df["Timestamp"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S")
        last_pnl = df["PnL"].iloc[-1]

        # Display stats
        st.markdown(
            f"""
            <div style="text-align: center; font-size: 18px; margin: 20px 0;">
                <b>First Entry (IST):</b> {first_time} &nbsp;|&nbsp;
                <b>Last Entry (IST):</b> {last_time} &nbsp;|&nbsp;
                <b>Last PnL:</b> {last_pnl}
            </div>
            """,
            unsafe_allow_html=True
        )

        # Plot
        st.subheader("📊 PnL Over Time")
        chart_title = f"PnL Trend | {today_str} | Last PnL: {last_pnl}"
        fig, ax = plt.subplots()
        ax.plot(df["Timestamp"], df["PnL"], marker='o')
        ax.set_xlabel("Time (IST)")
        ax.set_ylabel("PnL")
        ax.set_title(chart_title)
        ax.grid(True)
        plt.xticks(rotation=45)
        st.pyplot(fig)

        # Download button
        img_bytes = BytesIO()
        fig.savefig(img_bytes, format='png', bbox_inches='tight')
        st.download_button(
            label="Download Chart as PNG",
            data=img_bytes,
            file_name=f"pnl_chart_{today_str}.png",
            mime="image/png"
        )

# ------------------------- #
# Tab 2: History Viewer
# ------------------------- #
with tab2:
    st.subheader("📅 View Historical PnL")

    # List available CSVs
    csv_files = sorted([f for f in os.listdir('.') if f.startswith('crude_pnl_') and f.endswith('.csv')])

    if not csv_files:
        st.warning("No historical data found.")
    else:
        selected_file = st.selectbox("Select a date file to view:", csv_files)

        df_hist = pd.read_csv(selected_file)
        df_hist["Timestamp"] = pd.to_datetime(df_hist["Timestamp"]).dt.tz_localize("Asia/Kolkata")

        last_hist_pnl = df_hist["PnL"].iloc[-1]
        chart_title = f"PnL Trend | {selected_file.replace('.csv','')} | Last PnL: {last_hist_pnl}"

        fig2, ax2 = plt.subplots()
        ax2.plot(df_hist["Timestamp"], df_hist["PnL"], marker='o')
        ax2.set_xlabel("Time (IST)")
        ax2.set_ylabel("PnL")
        ax2.set_title(chart_title)
        ax2.grid(True)
        plt.xticks(rotation=45)
        st.pyplot(fig2)

        img_bytes2 = BytesIO()
        fig2.savefig(img_bytes2, format='png', bbox_inches='tight')
        st.download_button(
            label="Download Chart as PNG",
            data=img_bytes2,
            file_name=f"{selected_file.replace('.csv','')}.png",
            mime="image/png"
        )

import streamlit as st
import pytz
from datetime import datetime
import pandas as pd
import yfinance as yf
from bq_util import get_trades, update_stop_loss_by_key
from kite_helper import find_strike_banknifty, find_strike_nifty


def calculate_sl(instrument, ltp):
    # Define time zone
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)

    # Extract current time
    current_time = now.strftime('%H:%M')
    

    if instrument == 'BANKNIFTY':
            sl = ltp * 1.31
    elif instrument == 'NIFTY':
            sl = ltp * 1.39
    else:
            return "Invalid instrument"
    # else:
    #     if instrument == 'BANKNIFTY':
    #         sl = ltp * 1.8
    #     elif instrument == 'NIFTY':
    #         sl = ltp * 1.6
    #     else:
    #         return "Invalid instrument"
    

    return sl

def atm_nifty():
    ticker = "^NSEI"  # Ticker symbol for Nifty
    
    data = yf.download(ticker, period="1d")
    spot_price = data["Close"].iloc[-1]  # Get the most recent closing price
    
    rounded_price = round(spot_price / 50) * 50
    
    return spot_price, rounded_price

def atm_banknifty():
    ticker = "^NSEBANK"  # Ticker symbol for Bank Nifty
    
    data = yf.download(ticker, period="1d")
    spot_price = data["Close"].iloc[-1]  # Get the most recent closing price
    
    rounded_price = round(spot_price / 100) * 100
    
    return rounded_price

# Function to fetch unique times from the database
def fetch_unique_times():
    items = get_trades()
    unique_times = pd.Series([item['time'][:16] for item in items]).unique()
    return unique_times

# Function to fetch instruments by selected time
def fetch_instruments_by_time(selected_time):
    items = get_trades()
    print(items)
    filtered_items = [item for item in items if item['time'][:16] == selected_time]
    return filtered_items

# Function to fetch current SL based on name and instrument type
def fetch_current_sl(name, instrument_type):
    items = get_trades()
    for item in items:
        if item['name'] == name and item['instrument_type'] == instrument_type:
            return item['sl']
    return None


tab1, tab2, tab3, tab4 = st.tabs(['Trade Bot','Additional Trade','After 11:30 SL','Hedges'])

with tab1:

    # Streamlit UI
    st.title('Trade Bot Dashboard')

    # 1. Select Time
    unique_times = fetch_unique_times()
    selected_time = st.selectbox("Select Time", unique_times)

    if selected_time:
        # 2. Select Instrument
        instruments = fetch_instruments_by_time(selected_time)
        instrument_options = [f"{item['name']} {item['strike']}" for item in instruments]
        selected_instrument = st.selectbox("Select Instrument", instrument_options)

        if selected_instrument:
            # Find the selected instrument items
            selected_items = [item for item in instruments if f"{item['name']} {item['strike']}" == selected_instrument]
            
            if selected_items:
                st.write("Selected Items Details:")
                for item in selected_items:
                    st.json(item)

                col1, col2 = st.columns(2)
                col3, col4 = st.columns(2)

                # Entry Prices
                with col1:
                    entry_price_call = st.number_input("Entry Price for Call (CE)", value=0.0, format="%.2f")

                with col2:
                    entry_price_put = st.number_input("Entry Price for Put (PE)", value=0.0, format="%.2f")

                # Calculate and display new SL for selected items
                new_sl_call = None
                new_sl_put = None

                # if entry_price_call > 0:
                #     new_sl_call = calculate_sl(selected_items[0]['name'], entry_price_call)
                #     with col3:
                #         st.write(f"Calculated SL for Call (CE): {new_sl_call:.2f}")

                # if entry_price_put > 0:
                #     new_sl_put = calculate_sl(selected_items[0]['name'], entry_price_put)
                #     with col4:
                #         st.write(f"Calculated SL for Put (PE): {new_sl_put:.2f}")

                # Update SL button
                if st.button("Update SL"):

                    if entry_price_call > 0:
                        new_sl_call = round(calculate_sl(selected_items[0]['name'], entry_price_call),2)
                        with col3:
                            st.write(f"Calculated SL for Call (CE): {new_sl_call:.2f}")

                    if entry_price_put > 0:
                        new_sl_put = round(calculate_sl(selected_items[0]['name'], entry_price_put),2)
                        with col4:
                            st.write(f"Calculated SL for Put (PE): {new_sl_put:.2f}")

                    
                    if new_sl_call is not None:
                        for item in selected_items:
                            if item['instrument_type'] == "CE":
                                update_stop_loss_by_key(item['key'], new_sl_call)
                    if new_sl_put is not None:
                        for item in selected_items:
                            if item['instrument_type'] == "PE":
                                update_stop_loss_by_key(item['key'], new_sl_put)
                    
                    st.success("SL updated successfully!")
                
with tab2:
    st.title('Additional Trade')

    entry = st.number_input('Entry Price')

    if entry:

        st.write(f'SL : {entry*1.43}')

    st.divider()

    nifty_trade_ce = st.button('Nifty CE Additional Trade')
    nifty_trade_pe = st.button('Nifty PE Additional Trade')
    banknifty_trade_ce = st.button('Banknifty CE Additional Trade')
    banknifty_trade_pe = st.button('Banknifty PE Additional Trade')

    if nifty_trade_ce:
        _,nifty_atm = atm_nifty()
        near_strike = find_strike_nifty('CE',nifty_atm)
        if near_strike != None:
            st.write(f"{near_strike['name']} - {int(near_strike['strike'])} - {near_strike['instrument_type']}")
        else:
            st.write('No Nearby Strike in given range')


    if nifty_trade_pe:
        _,nifty_atm = atm_nifty()
        near_strike = find_strike_nifty('PE',nifty_atm)
        if near_strike != None:
            st.write(f"{near_strike['name']} - {int(near_strike['strike'])} - {near_strike['instrument_type']}")
        else:
            st.write('No Nearby Strike in given range')

    if banknifty_trade_ce:
        banknifty_atm = atm_banknifty()
        near_strike = find_strike_banknifty('CE',banknifty_atm)
        if near_strike != None:
            st.write(f"{near_strike['name']} - {int(near_strike['strike'])} - {near_strike['instrument_type']}")
        else:
            st.write('No Nearby Strike in given range')

    if banknifty_trade_pe:
        banknifty_atm = atm_banknifty()
        near_strike = find_strike_banknifty('PE',banknifty_atm)
        if near_strike != None:
            st.write(f"{near_strike['name']} - {int(near_strike['strike'])} - {near_strike['instrument_type']}")
        else:
            st.write('No Nearby Strike in given range')

with tab3:

    st.title('After 11:30 AM SL')

    col_nifty, col_banknifty = st.columns(2)

    with col_nifty:

        tab3_nifty_entry = st.number_input('Entry Nifty Price')
        st.write(f'SL for NIFTY {tab3_nifty_entry*1.6}')

    with col_banknifty:

        tab3_bn_entry = st.number_input('Entry Banknifty Price')
        st.write(f'SL for NIFTY {tab3_bn_entry*1.8}')

with tab4:

    st.title('Nifty Hedges')

    if st.button('Get Nifty Hedge'):

        spot , atm = atm_nifty()

        st.write(f'Nity Spot : {spot}')

        st.write(f'NIFTY {atm+300} CALL')
        st.write(f'NIFTY {atm-300} PUT')


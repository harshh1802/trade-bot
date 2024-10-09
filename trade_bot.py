import schedule
import yfinance as yf
import time
from googlesheet import Sheet
from tele_bot import send_msg, send_error_msg
from kite_helper import get_instrument_file, get_ticker_info, get_ltp
import pandas as pd
from datetime import datetime
import pytz
from bq_util import *


# 5298366099:AAHeZkCzOz7W31QZoL9a_Zb4gdG-3uIBIFM

bot_token = "5298366099:AAHeZkCzOz7W31QZoL9a_Zb4gdG-3uIBIFM"  # megabot
chat_id = -969261634

try:
    get_instrument_file()
    df = pd.read_csv('instruments.csv')
except Exception as e:
    print(e)


def is_before_time_limit():
    # Define the time limit
    time_limit_str = '11:50 AM' #TODO : Before 11:50 AM
    
    # Get the current time
    current_time = datetime.now().time()
    
    # Convert the time limit string to a time object
    time_limit = datetime.strptime(time_limit_str, '%I:%M %p').time()
    
    # Compare current time with time limit
    return current_time < time_limit


def atm_nifty():
    ticker = "^NSEI"  # Ticker symbol for Nifty
    
    data = yf.download(ticker, period="1d")
    spot_price = data["Close"].iloc[-1]  # Get the most recent closing price
    
    rounded_price = round(spot_price / 50) * 50
    
    return rounded_price

def atm_banknifty():
    ticker = "^NSEBANK"  # Ticker symbol for Bank Nifty
    
    data = yf.download(ticker, period="1d")
    spot_price = data["Close"].iloc[-1]  # Get the most recent closing price
    
    rounded_price = round(spot_price / 100) * 100
    
    return rounded_price


def atm_finnifty():
    ticker = "NIFTY_FIN_SERVICE.NS"  # Ticker symbol for Bank Nifty
    
    data = yf.download(ticker, period="1d")
    spot_price = data["Close"].iloc[-1]  # Get the most recent closing price
    
    rounded_price = round(spot_price / 100) * 100
    
    return rounded_price

def atm_sensex():
    ticker = "^BSESN"  # Ticker symbol for Bank Nifty
    
    data = yf.download(ticker, period="1d")
    spot_price = data["Close"].iloc[-1]  # Get the most recent closing price
    
    rounded_price = round(spot_price / 100) * 100
    
    return rounded_price

def alert():
    send_msg(bot_token,chat_id,"Peak Margin Logging to be done")

def hedge_alert():
    send_msg(bot_token, chat_id, "Square off previous day hedge and add SL")

def take_hedge_alert():
    send_msg(bot_token, chat_id, "Take hedge for positional")

hedge_alert()

schedule.every().day.at("03:56:00").do(hedge_alert)

schedule.every().day.at("09:46:00").do(take_hedge_alert)

schedule.every().day.at("09:55:00").do(alert)

def get_current_indian_time():
    ist_timezone = pytz.timezone('Asia/Kolkata')
    current_time_ist = datetime.now(ist_timezone)
    return current_time_ist.strftime('%Y-%m-%d %H:%M:%S')


schedule.every().day.at("09:55:00").do(alert)

instrument_functions = {
    "nifty": atm_nifty,
    "banknifty": atm_banknifty,
    "finnifty": atm_finnifty,
    "sensex":atm_sensex
}



#function to send message to telegram-bot
def send(instrument,qty):
        
        strike = instrument_functions[instrument]()
        msg = f'Trade {instrument} {strike} CE PE ({qty})'
        send_msg(bot_token,chat_id,msg)

        if is_before_time_limit():#To check if current trade is intra trade 

            print('getting ce')
            ticker_info_ce = get_ticker_info(df , instrument.upper(), strike, 'CE')
            ltp_ce = get_ltp(ticker_info_ce[1])
            sl_ce = ltp_ce * 1.39 if instrument == 'nifty' else ltp_ce*1.31
            print('getting ce ltp')
            insert_data(instrument_token = int(ticker_info_ce[0]),
                        instrument_type = 'CE',
                        ltp = int(ltp_ce),
                        name = instrument.upper(),
                        qty = int(qty),
                        sl = sl_ce,
                        strike = int(strike),
                        symbol = ticker_info_ce[1],
                        time = get_current_indian_time()
                        )
                        
            print('getting pe')
            ticker_info_pe = get_ticker_info(df , instrument.upper(), strike, 'PE')
            ltp_pe = get_ltp(ticker_info_pe[1])
            sl_pe = ltp_pe * 1.39 if instrument == 'nifty' else ltp_pe*1.31
            print('getting pe ltp')
            insert_data(instrument_token= int(ticker_info_pe[0]),
                        instrument_type= 'PE',
                        ltp = int(ltp_pe),
                        name = instrument.upper(),
                        qty = int(qty),
                        sl = sl_pe,
                        strike = int(strike),
                        symbol = ticker_info_pe[1],
                        time = get_current_indian_time()
                        )
                        
            
        time.sleep(1)


#initiating sheet
a= Sheet("NOCD")
data = a.getWorksheet("mega")


#iterating on data received from sheet
for a in data:

    aday = a["day"]
    trigger_time = a["time"]
    instrument = a["instrument"]
    qty= a["qty"]


    if aday == "monday":
        schedule.every().monday.at(trigger_time).do(send,instrument,qty)

    elif aday == "tuesday":
        schedule.every().tuesday.at(trigger_time).do(send,instrument,qty)

    elif aday == "wednesday":
        schedule.every().wednesday.at(trigger_time).do(send,instrument,qty)

    elif aday == "thursday":
        schedule.every().thursday.at(trigger_time).do(send,instrument,qty)

    elif aday == "friday": #for Test only
        schedule.every().friday.at(trigger_time).do(send,instrument,qty)

    


print(schedule.get_jobs())


while 1:
    try:
        schedule.run_pending()

    except Exception as e:
        send_error_msg(e)
        print(e)
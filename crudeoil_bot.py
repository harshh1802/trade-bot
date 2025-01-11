import schedule
import yfinance as yf
import time
from googlesheet import Sheet
from tele_bot import send_msg, send_error_msg
from kite_helper import get_crude_instrument_file, get_crude_oil_atm
import pandas as pd
from datetime import datetime
import pytz
from bq_util import *


# 5298366099:AAHeZkCzOz7W31QZoL9a_Zb4gdG-3uIBIFM

bot_token = "5298366099:AAHeZkCzOz7W31QZoL9a_Zb4gdG-3uIBIFM"  # megabot
chat_id = -4583759303

try:
    get_crude_instrument_file()
    df = pd.read_csv('instruments.csv')
except Exception as e:
    print(e)

def alert():
    send_msg(bot_token,chat_id,"RUNNING : CRUDE OIL BOT")

def mcx_alert():
    send_msg(bot_token, chat_id, "Start : Crude Oil")

def sq_off_alert():
    send_msg(bot_token, chat_id, "Sq off mcx positions")

mcx_alert()

schedule.every().day.at("10:10:00").do(alert)
schedule.every().day.at("17:30:00").do(sq_off_alert)

def get_current_indian_time():
    ist_timezone = pytz.timezone('Asia/Kolkata')
    current_time_ist = datetime.now(ist_timezone)
    return current_time_ist.strftime('%Y-%m-%d %H:%M:%S')


instrument_functions = {
    "crudeoil": get_crude_oil_atm,
}



#function to send message to telegram-bot
def send(instrument,qty):
        
        strike = instrument_functions[instrument]()
        msg = f'Trade {instrument} {strike} CE PE ({qty})'
        send_msg(bot_token,chat_id,msg)
            
        time.sleep(1)


#initiating sheet
a= Sheet("NOCD")
data = a.getWorksheet("crudeoil")


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

    elif aday == "saturday": #for Test only
        schedule.every().saturday.at(trigger_time).do(send,instrument,qty)

    


print(schedule.get_jobs())


while 1:
    try:
        schedule.run_pending()

    except Exception as e:
        send_error_msg(e)
        print(e)
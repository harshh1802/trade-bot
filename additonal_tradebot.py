import sys
import logging
import time
from tele_bot import send_msg
from kite_helper import get_instrument_file, get_ticker_info, get_ltp, find_strike_banknifty, find_strike_nifty
from kiteconnect import KiteTicker
import threading
import queue
from datetime import datetime
import pytz
from bq_util import get_trades, get_access_token, delete_data_by_key

# Custom formatter to use IST timezone
class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ist = pytz.timezone('Asia/Kolkata')
        record_time = datetime.fromtimestamp(record.created, ist)
        if datefmt:
            return record_time.strftime(datefmt)
        else:
            return record_time.strftime("%Y-%m-%d %H:%M:%S")

# Set up logging with IST timezone
logging.basicConfig(level=logging.INFO, 
                    format='{asctime} - {levelname} - {message}', 
                    style='{',
                    handlers=[logging.FileHandler("trade_tracker.log"),
                              logging.StreamHandler()])

# Replace the default formatter with ISTFormatter
for handler in logging.getLogger().handlers:
    handler.setFormatter(ISTFormatter("{asctime} - {levelname} - {message}", style='{'))

logging.info("Script started.")


bot_token = "5298366099:AAHeZkCzOz7W31QZoL9a_Zb4gdG-3uIBIFM"  # megabot
chat_id = -4583759303

# Fetch trade entries from the Deta Base
def fetch_trade_entries():
    logging.info("Fetching trade entries from BigQ")
    try:
        return list(get_trades())
    except Exception as e:
        logging.error(f"Error fetching trade entries: {e}")
        return []

# Global variable to store trade entries
trade_entries = fetch_trade_entries()


# Fetch Access token
try:
    access_token = get_access_token()
    api_key = "wj9u6r4qnh594duq"
    logging.info("Access token and API key fetched successfully.")
except Exception as e:
    logging.error(f"Error fetching access token or API key: {e}")
    send_msg(bot_token, chat_id, "Critical error occurred: Failed to fetch access token or API key.")
    sys.exit(1)

# Initialize a queue to handle ticks
ticks_queue = queue.Queue()

# Callback functions for WebSocket
def on_ticks(ws, ticks):
    logging.info("Received ticks from WebSocket.")
    try:
        for tick in ticks:
            ticks_queue.put(tick)
    except Exception as e:
        logging.error(f"Error Putting ticks to Queue: {e}")

def on_connect(ws, response):
    global trade_entries
    logging.info("WebSocket connected.")
    send_msg(bot_token, chat_id, 'socket connected')
    try:
        instrument_tokens = [trade['instrument_token'] for trade in trade_entries]
        logging.info(f"Subscribing to instrument tokens: {instrument_tokens}")
        ws.subscribe(instrument_tokens)
    except Exception as e:
        logging.error(f"Error on WebSocket connect: {e}")

def on_close(ws, code, reason):
    logging.info(f"WebSocket closed with code: {code}, reason: {reason}")
    send_msg(bot_token, chat_id, f"Closed connection on code: {code} and reason: {reason}")

# Function to process ticks
def process_ticks():
    global trade_entries

    while True:
        try:
            tick = ticks_queue.get()
            instrument_token = tick['instrument_token']
            ltp = tick['last_price']

            # Find the corresponding trade entry
            for trade in trade_entries:
                if trade['instrument_token'] == instrument_token:
                    logging.info(f"Processing tick for {trade['symbol']} with LTP: {ltp}, SL: {trade['sl']}")
                    if ltp > trade['sl']:
                        send_msg(bot_token, chat_id, f"SL hit for\n {trade['symbol']} {trade['qty']} {trade['time']}")
                        logging.info(f"SL hit for {trade['symbol']} qty:{trade['qty']} {trade['time']}")

                        if trade['name'] == 'NIFTY':
                            print('before prob')
                            nearest_option = find_strike_nifty(trade['instrument_type'], trade['strike'])
                            print('after prob')
                        else:
                            nearest_option = find_strike_banknifty(trade['instrument_type'], trade['strike'])

                        if nearest_option is None:
                            send_msg(bot_token, chat_id, f"No nearest strike found for {trade['symbol']}")
                            logging.warning(f"No nearest strike found for {trade['symbol']}")
                        else:
                            #Nifty lot 25 , bank nifty lot 15
                            qty = round((trade['qty']/25)/4)*25 if trade['name'] == 'NIFTY' else round((trade['qty']/15)/4)*15
                            send_msg(bot_token, chat_id, f"Additional Trade\n{nearest_option['name']} - {int(nearest_option['strike'])} - {nearest_option['instrument_type']} {qty}")
                            logging.info(f"Additional trade for {trade['symbol']}")

                        # Unsubscribe from the token
                        kws.unsubscribe([instrument_token])
                        trade_entries.remove(trade)
                        delete_data_by_key(trade['key'])
        except Exception as e:
            logging.error(f"Error processing tick: {e}")

# Initialize KiteTicker
kws = KiteTicker(api_key, access_token)
logging.info("KiteTicker initialized.")

# Assign the callback functions
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Connect to the WebSocket
logging.info("Connecting to WebSocket.")
kws.connect(threaded=True)

# Function to update trade entries periodically
def update_trade_entries():
    global trade_entries

    while True:
        try:
            temp_trades = fetch_trade_entries()

            if (trade_entries != temp_trades) and (len(temp_trades) != 0):
                trade_entries = temp_trades
                instrument_tokens = [trade['instrument_token'] for trade in trade_entries]
                logging.info(f"Updating trade entries and subscribing to new tokens: {instrument_tokens}")
                kws.subscribe(instrument_tokens)

            time.sleep(60)  # Update every 60 seconds
        except Exception as e:
            logging.error(f"Error updating trade entries: {e}")

# Start the update thread
update_thread = threading.Thread(target=update_trade_entries)
update_thread.start()
logging.info("Started update thread.")

# Start the tick processing thread
tick_processing_thread = threading.Thread(target=process_ticks)
tick_processing_thread.start()
logging.info("Started tick processing thread.")
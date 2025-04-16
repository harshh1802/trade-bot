import sys
import logging
import time
import csv
import threading
import queue
import os
from datetime import datetime
import pytz
from kiteconnect import KiteTicker
from google.cloud import bigquery
from google.oauth2 import service_account
from bq_util import get_access_token

# Constants
INDIA_TZ = pytz.timezone('Asia/Kolkata')
CSV_FILENAME = datetime.now(INDIA_TZ).strftime("%Y%m%d") + ".csv"
CREDENTIALS_FILE = "creds.json"
INSTRUMENT_CSV = "ticker_token.csv"
BQ_DATASET = "hist_data_kite"
BQ_TABLE = "nifty50"
API_KEY = "wj9u6r4qnh594duq"  # Will be added manually

# Set up logging with IST timezone
class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ist = pytz.timezone('Asia/Kolkata')
        record_time = datetime.fromtimestamp(record.created, ist)
        if datefmt:
            return record_time.strftime(datefmt)
        else:
            return record_time.strftime("%Y-%m-%d %H:%M:%S")

logging.basicConfig(level=logging.INFO, 
                    format='{asctime} - {levelname} - {message}', 
                    style='{',
                    handlers=[logging.FileHandler("tick_collector.log"),
                              logging.StreamHandler()])

# Replace the default formatter with ISTFormatter
for handler in logging.getLogger().handlers:
    handler.setFormatter(ISTFormatter("{asctime} - {levelname} - {message}", style='{'))

logging.info("Nifty 50 tick data collector started")

# Initialize a queue to handle ticks
ticks_queue = queue.Queue()

# Read instrument tokens from CSV
def read_instrument_tokens():
    instrument_tokens = []
    token_map = {}
    try:
        with open(INSTRUMENT_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                token = int(row['instrument_token'])
                ticker = row['ticker']
                instrument_tokens.append(token)
                token_map[token] = ticker
        
        logging.info(f"Loaded {len(instrument_tokens)} instruments from {INSTRUMENT_CSV}")
        return instrument_tokens, token_map
    except Exception as e:
        logging.error(f"Error reading instrument tokens: {str(e)}")
        sys.exit(1)

# Initialize CSV file with headers
def initialize_csv():
    headers = [
        'timestamp', 'instrument_token', 'ticker', 'last_price', 
        'last_traded_quantity', 'volume_traded','exchange_timestamp'  # New fields added
    ]
    
    # Adding market depth fields (buy & sell orders in the correct order)
    for i in range(1, 6):  # Assuming 5 levels of depth
        headers.extend([
            f'buy_quantity_{i}', f'buy_price_{i}', f'buy_orders_{i}',
            f'sell_quantity_{i}', f'sell_price_{i}', f'sell_orders_{i}'
        ])

    if not os.path.exists(CSV_FILENAME):
        with open(CSV_FILENAME, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
        logging.info(f"Initialized CSV file: {CSV_FILENAME}")


# Check if market is open
def is_market_open():
    now = datetime.now(INDIA_TZ)
    
    # Check if it's a weekday
    if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        return False
    
    # Market hours: 9:15 AM to 3:30 PM
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_open <= now <= market_close

# Write to CSV with lock for thread safety
csv_lock = threading.Lock()

def write_to_csv(data_row):
    with csv_lock:
        with open(CSV_FILENAME, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data_row.keys())
            writer.writerow(data_row)

# Callback functions for WebSocket
def on_ticks(ws, ticks):
    for tick in ticks:
        print(tick)
        ticks_queue.put(tick)

def on_connect(ws, response):
    logging.info(f"WebSocket connected: {response}")
    instrument_tokens, _ = read_instrument_tokens()
    ws.subscribe(instrument_tokens)
    ws.set_mode(ws.MODE_FULL, instrument_tokens)
    logging.info(f"Subscribed to {len(instrument_tokens)} instruments")

def on_close(ws, code, reason):
    logging.info(f"WebSocket closed: {reason} (Code: {code})")

def on_error(ws, code, reason):
    logging.error(f"WebSocket error: {reason} (Code: {code})")

# Function to process ticks from the queue
def process_ticks():
    token_map = read_instrument_tokens()[1]

    while True:
        try:
            if not is_market_open() and ticks_queue.empty():
                time.sleep(10)
                continue

            tick = ticks_queue.get(timeout=1)

            instrument_token = tick.get('instrument_token')
            ticker = token_map.get(instrument_token, "Unknown")

            # Extract datetime fields
            last_trade_time = tick.get('last_trade_time')
            exchange_timestamp = tick.get('exchange_timestamp')

            # Convert to string format if they exist
            last_trade_time_str = last_trade_time.strftime("%Y-%m-%d %H:%M:%S") if last_trade_time else ""
            exchange_timestamp_str = exchange_timestamp.strftime("%Y-%m-%d %H:%M:%S") if exchange_timestamp else ""

            # Extract Market Depth (Buy & Sell)
            depth = tick.get('depth', {})
            buy_side = depth.get('buy', [])
            sell_side = depth.get('sell', [])

            # Initialize depth values with empty defaults
            depth_data = {}
            for i in range(5):
                depth_data[f'buy_quantity_{i+1}'] = buy_side[i]['quantity'] if i < len(buy_side) else 0
                depth_data[f'buy_price_{i+1}'] = buy_side[i]['price'] if i < len(buy_side) else 0
                depth_data[f'buy_orders_{i+1}'] = buy_side[i]['orders'] if i < len(buy_side) else 0

                depth_data[f'sell_quantity_{i+1}'] = sell_side[i]['quantity'] if i < len(sell_side) else 0
                depth_data[f'sell_price_{i+1}'] = sell_side[i]['price'] if i < len(sell_side) else 0
                depth_data[f'sell_orders_{i+1}'] = sell_side[i]['orders'] if i < len(sell_side) else 0

            data_row = {
                'timestamp': datetime.now(INDIA_TZ).strftime("%Y-%m-%d %H:%M:%S.%f"),
                'instrument_token': instrument_token,
                'ticker': ticker,
                'last_price': tick.get('last_price', 0),
                'last_traded_quantity': tick.get('last_traded_quantity', 0),
                'volume_traded': tick.get('volume_traded', 0),
                # 'total_buy_quantity': tick.get('total_buy_quantity', 0),
                # 'total_sell_quantity': tick.get('total_sell_quantity', 0),
                # 'open': tick.get('ohlc', {}).get('open', 0),
                # 'high': tick.get('ohlc', {}).get('high', 0),
                # 'low': tick.get('ohlc', {}).get('low', 0),
                # 'close': tick.get('ohlc', {}).get('close', 0),
                # 'last_trade_time': last_trade_time_str,
                'exchange_timestamp': exchange_timestamp_str,
                **depth_data  # Add market depth dynamically
            }

            write_to_csv(data_row)

        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Error processing tick: {str(e)}")

# Function to upload CSV to BigQuery
def upload_csv_to_bigquery():
    try:
        logging.info(f"Uploading {CSV_FILENAME} to BigQuery")
        # Set up credentials
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, 
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header
            autodetect=True,  # Auto-detect schema
        )
        
        with open(CSV_FILENAME, "rb") as file:
            job = client.load_table_from_file(file, table_ref, job_config=job_config)
        
        job.result()  # Wait for the job to complete
        logging.info(f"CSV file {CSV_FILENAME} uploaded successfully to BigQuery!")
        
    except Exception as e:
        logging.error(f"Error uploading to BigQuery: {str(e)}")

# Function to schedule upload to BigQuery at 15:40 IST
def schedule_bigquery_upload():
    try:
        # upload_time = datetime.now(INDIA_TZ).replace(hour=15, minute=40, second=0, microsecond=0)
        upload_time = datetime.now(INDIA_TZ).replace(hour=10, minute=12, second=0, microsecond=0)
        
        # If current time is already past 15:40, we'll upload immediately
        now = datetime.now(INDIA_TZ)
        if now >= upload_time:
            logging.info("Market already closed, uploading data immediately")
            upload_csv_to_bigquery()
            return
        
        # Calculate seconds to wait
        seconds_to_wait = (upload_time - now).total_seconds()
        logging.info(f"Scheduling upload in {seconds_to_wait/60:.2f} minutes (at 15:40 IST)")
        
        # Schedule the upload
        timer = threading.Timer(seconds_to_wait, upload_csv_to_bigquery)
        timer.daemon = True
        timer.start()
        
    except Exception as e:
        logging.error(f"Error scheduling upload: {str(e)}")

# Main function
def main():
    try:
        # Initialize CSV file
        initialize_csv()
        
        # Get access token
        access_token = get_access_token()
        logging.info("Access token fetched successfully")
        
        # Schedule BigQuery upload
        schedule_bigquery_upload()
        
        # Start the tick processing thread
        tick_thread = threading.Thread(target=process_ticks)
        tick_thread.daemon = True
        tick_thread.start()
        logging.info("Started tick processing thread")
        
        # Initialize KiteTicker
        kws = KiteTicker(API_KEY, access_token)
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_close = on_close
        kws.on_error = on_error
        
        # Connect to WebSocket
        logging.info("Connecting to WebSocket")
        kws.connect()
        
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, stopping")
    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main()
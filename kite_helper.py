#Importing Libraries
from kiteconnect import KiteConnect
from datetime import datetime
import time
import pandas as pd
from bq_util import get_access_token
import os


#Fetch Access toekn
access_token = get_access_token()


#Kite api
api = "wj9u6r4qnh594duq"
kite = KiteConnect(api_key=api)
kite.set_access_token(access_token)


def get_instrument_file():
    
        instruments = kite.instruments()

        instrument_df = pd.DataFrame(instruments)

        instrument_df = instrument_df[(instrument_df['name'].isin(['NIFTY','BANKNIFTY'])) & (instrument_df['segment'] == 'NFO-OPT')].sort_values(by = 'expiry').to_csv('instruments.csv')

        return 'Saved Instrument File'

def check_instrument_file():

    file_name = 'instruments.csv'

    # Check if the file exists and is up-to-date
    if not os.path.exists(file_name) or datetime.fromtimestamp(os.path.getmtime(file_name)).date() != datetime.today().date():
        print("Regenerating the mktcap file...")
        get_instrument_file()
    else:
        print("Mktcap File is up-to-date.")


    return None


def get_ticker_info(df, symbol, strike , instrument_type):

    temp_df = df[(df['name'] == symbol) & 
                            (df['strike'] == strike) &
                            (df['instrument_type'] == instrument_type)].sort_values(by='expiry')
    
    return temp_df['instrument_token'].iloc[0], temp_df['tradingsymbol'].iloc[0]

def get_ltp(tradingsymbol):
    ltp = kite.ltp(f'NFO:{tradingsymbol}')
    return ltp[f'NFO:{tradingsymbol}']['last_price']


def kite_object():
     return kite

# Function to fetch instruments
def fetch_instruments():
    # Fetch all instruments
    return pd.read_csv('instruments.csv')

# Function to find options with premium closest to 60 Rs, within the range 50 to 70 Rs
def find_strike_nifty(instrument_type, atm_strike):
    # Fetch instruments and filter for Nifty options
    instruments_df = fetch_instruments()
    options_df = instruments_df[
        (instruments_df['segment'] == 'NFO-OPT') & 
        (instruments_df['name'] == 'NIFTY') &
        (instruments_df['instrument_type'] == instrument_type) &
        (instruments_df['strike'].between(atm_strike - 1000, atm_strike + 1000))
    ]

    # Sort options by expiry and get the earliest expiry date
    options_df['expiry'] = pd.to_datetime(options_df['expiry'])
    earliest_expiry_date = options_df['expiry'].min()

    print("Earliest expiry date:", earliest_expiry_date)

    # Filter for options with the earliest expiry date
    options_df = options_df[options_df['expiry'] == earliest_expiry_date]

    # Get the instrument symbols with "NFO:" prefix
    instrument_symbols = ["NFO:" + symbol for symbol in options_df['tradingsymbol'].tolist()]

    # Fetch the market prices for these instruments
    ltp_data = kite.ltp(instrument_symbols)
    
    # Find the option with premium closest to 60 Rs, within 50 to 70 Rs
    closest_option = None
    min_diff = float('inf')

    for symbol, data in ltp_data.items():
        print("Symbol:", symbol, "Data:", data)
        ltp = data['last_price']
        if 50 <= ltp <= 70:  # Check if LTP is within the desired range
            diff = abs(ltp - 60)
            option = options_df[options_df['tradingsymbol'] == symbol.split(':')[1]]

            if not option.empty and diff < min_diff:
                closest_option = option.iloc[0].to_dict()
                closest_option['last_price'] = ltp
                min_diff = diff

    return closest_option

# Function to find options with premium closest to 60 Rs, within the range 50 to 70 Rs
def find_strike_banknifty(instrument_type, atm_strike):
    # Fetch instruments and filter for Nifty options
    instruments_df = fetch_instruments()
    options_df = instruments_df[
        (instruments_df['segment'] == 'NFO-OPT') & 
        (instruments_df['name'] == 'BANKNIFTY') &
        (instruments_df['instrument_type'] == instrument_type) &
        (instruments_df['strike'].between(atm_strike - 2000, atm_strike + 2000))
    ]

    # Sort options by expiry and get the earliest expiry date
    options_df['expiry'] = pd.to_datetime(options_df['expiry'])
    earliest_expiry_date = options_df['expiry'].min()

    print("Earliest expiry date:", earliest_expiry_date)

    # Filter for options with the earliest expiry date
    options_df = options_df[options_df['expiry'] == earliest_expiry_date]
    print("Options with earliest expiry date:", options_df)

    # Get the instrument symbols with "NFO:" prefix
    instrument_symbols = ["NFO:" + symbol for symbol in options_df['tradingsymbol'].tolist()]

    # Fetch the market prices for these instruments
    ltp_data = kite.ltp(instrument_symbols)
    
    # Find the option with premium closest to 60 Rs, within 50 to 70 Rs
    closest_option = None
    min_diff = float('inf')

    for symbol, data in ltp_data.items():
        print("Symbol:", symbol, "Data:", data)
        ltp = data['last_price']
        if 110 <= ltp <= 130:  # Check if LTP is within the desired range
            diff = abs(ltp - 120)
            option = options_df[options_df['tradingsymbol'] == symbol.split(':')[1]]

            if not option.empty and diff < min_diff:
                closest_option = option.iloc[0].to_dict()
                closest_option['last_price'] = ltp
                min_diff = diff

    return closest_option


#Importing Libraries
from kiteconnect import KiteConnect
from datetime import datetime
import time
from bq_util import get_access_token
#Initialize deta
# deta = Deta('d0rwyaghixk_uew8hxaR9QMG689EeSYUQXME2q8qeCR2')
# key = deta.Base('keys')


#Fetch Access toekn
access_token = get_access_token()


#Fetch expiry date
# expiry = key.get('expiry')['value']



#Kite api
api = "wj9u6r4qnh594duq"
kite = KiteConnect(api_key=api)
kite.set_access_token(access_token)


#Login Url
# https://kite.zerodha.com/connect/login?api_key=wj9u6r4qnh594duq&v=3



#Fetch ltp of option (latest expiry)
def get_option_ltp(symbol,strike):
    #kite.ltp("NFO:BANKNIFTY23JAN42500CE") or kite.ltp("NFO:NIFTY2320216750CE")
    ce = f"NFO:{symbol}{expiry}{strike}CE"
    pe = f"NFO:{symbol}{expiry}{strike}PE"
    data = kite.ltp([ce,pe])
    if data == {}:
        raise ValueError('Something wrong with input')
    else:
        return (data[ce]['instrument_token'],data[ce]['last_price']),(data[pe]['instrument_token'],data[pe]['last_price'])
    
#Function to get ltp of CE for option
def get_option_ltp_ce(symbol,strike):
    ce = f"NFO:{symbol}{expiry}{strike}CE"
    data = kite.ltp(ce)
    if data == {}:
        raise ValueError('Something wrong with input')
    else:
        return data[ce]['last_price']

#Function to get ltp of PE for option 
def get_option_ltp_pe(symbol,strike):
    pe = f"NFO:{symbol}{expiry}{strike}PE"
    data = kite.ltp(pe)
    if data == {}:
        raise ValueError('Something wrong with input')
    else:
        return data[pe]['last_price']


#Fetch ohlc of option (latest expiry)
def get_option_ohlc(symbol,strike,option_type):
    #kite.ohlc("NFO:BANKNIFTY23JAN42500CE") or kite.ltp("NFO:NIFTY2320216750CE")
    a = f"NFO:{symbol}{expiry}{strike}{option_type}"
    data = kite.ohlc(a)
    return data[a]['ohlc']

#At The Money of Nifty
def atm_nifty():
    nifty = kite.ltp('NSE:NIFTY 50')['NSE:NIFTY 50']['last_price']
    return int(50 * round(nifty / 50))


#At The Money of BankNifty
def atm_banknifty():
    banknifty = kite.ltp('NSE:NIFTY BANK')['NSE:NIFTY BANK']['last_price']
    return int(100 * round(banknifty / 100))


def get_ltp(instruments):
    data = kite.ltp(list(map(lambda x :  f"NSE:{x}",instruments)))
    return data

if __name__ == "__main__":

    while 1:
        print(atm_nifty())
        time.sleep(0.2)
        # print(get_option_ltp('BANKNIFTY',42500))

        print(get_ltp(['MTARTECH','ITC']))




import streamlit as st
import urllib.parse
from bq_util import *
from kiteconnect import KiteConnect

def extract_request_token():
    st.title("Extract Request Token from URL")
    
    # Text area for user input
    url = st.text_area("Paste the URL here:")
    
    if url:
        # Parse the URL and extract parameters
        parsed_url = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        # Extract 'request_token' if it exists
        request_token = params.get('request_token', None)
        
        if request_token:
            st.success(f"Request Token: {request_token[0]}")
            kite = KiteConnect(api_key= st.secrets.zerodha.api_key)

    
            data = kite.generate_session(request_token[0], api_secret= st.secrets.zerodha.api_secret)
            update_access_token(data["access_token"])
            print(data['access_token'])

            st.write('Access Token Updated!')


        else:
            st.error("No request_token parameter found in the URL.")

if __name__ == "__main__":
    extract_request_token()
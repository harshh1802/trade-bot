from datetime import datetime
from google.cloud import bigquery, storage
import pandas as pd
import pytz
from google.oauth2 import service_account
import os
import hashlib
import time
import streamlit as st
import json



sa_file = 'creds.json'

service_account_info = json.loads(st.secrets["gcp_service_account"]["json"])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
# credentials = service_account.Credentials.from_service_account_file(sa_file)
bigquery_client = bigquery.Client(credentials=credentials)



def key():
    current_time = str(time.time()).encode('utf-8')
    return hashlib.sha256(current_time).hexdigest()[:16]  # Take the first 16 characters

#
def fetch_bq_to_df(query, cache):
    """
    Executes a BigQuery query and retrieves the results as a DataFrame.

    Parameters:
    - query (str): SQL query to be executed.
    - cache (bool): Use query cache if True, otherwise execute a fresh query.

    Returns:
    - pd.DataFrame: Query results as a Pandas DataFrame.
    """
    job_config = bigquery.QueryJobConfig(use_query_cache=cache)
    return bigquery_client.query(query, job_config=job_config).to_dataframe()

def get_trades():
    sql = 'SELECT * FROM `telegram-bot-340406.002.trades`'
    return bigquery_client.query(sql).to_dataframe().to_dict('records')

def insert_data(instrument_token, instrument_type, ltp, name, qty, sl, strike, symbol, time):
    
    sql = f"""INSERT INTO `telegram-bot-340406.002.trades` (
      instrument_token,
      instrument_type,
      ltp,
      name,
      qty,
      sl,
      strike,
      symbol,
      time,
      key
    )
    VALUES
      ({instrument_token}, '{instrument_type}', {ltp}, '{name}', {qty}, {sl}, {strike}, '{symbol}', '{time}', '{key()}');"""
    
    query_job = bigquery_client.query(sql)
    query_job.result()  # Wait for the job to complete.
    print("Data inserted successfully.")


def update_stop_loss_by_key(key, new_sl):
    
    sql = f"""UPDATE `telegram-bot-340406.002.trades`
    SET sl = {new_sl}
    WHERE key = '{key}';"""
    
    query_job = bigquery_client.query(sql)
    query_job.result()  # Wait for the job to complete.
    print(f"Stop loss updated successfully for key: {key}")

def delete_data_by_key(key):
    
    sql = f"""DELETE FROM `telegram-bot-340406.002.trades`
    WHERE key = '{key}';"""
    
    query_job = bigquery_client.query(sql)
    query_job.result()  # Wait for the job to complete.
    print(f"Data deleted successfully for key: {key}")

def get_access_token():
    sql = f"""SELECT value FROM `telegram-bot-340406.002.keys`
    WHERE key = 'ZERODHA_ACCESS_TOKEN';"""
    
    query_job = bigquery_client.query(sql)
    result = query_job.result()  # Fetch the results
    
    for row in result:
        access_token = row.value  # Extract the value from the row
    
    print(f"Access Token Fetched!")
    return access_token


def update_access_token(access_token):

    sql = f"""UPDATE `telegram-bot-340406.002.keys`
    SET value = '{access_token}'
    WHERE key = 'ZERODHA_ACCESS_TOKEN';"""
    query_job = bigquery_client.query(sql)
    query_job.result()  # Wait for the job to complete.
    print(f"Access Token Updated in BQ!")




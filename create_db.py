import pandas as pd
import numpy as np
import sqlite3

import datetime as dt
import pytz

from yahooquery import Ticker
from symbols import CAC_40
from functions import *

# For test only
ticker = "TTE.PA"
ticker2 = "AI.PA"
ticker3 = "FR.PA"
list_tickers = [ticker, ticker2, ticker3]
tickers = Ticker(list_tickers)

# Create db connection
con = sqlite3.connect("app_finance.db")
cur = con.cursor()

# Define timezone
timezone = pytz.timezone('CET')

# ---------------------------------------------------------------------------------------------------------------
# Create one table with stock prices at the end of day (1 row per day)

# Request API
stock_price_daily = tickers.history(period='max', interval='1d')

# Reset index and select columns
stock_price_daily = stock_price_daily.reset_index()
stock_price_daily.drop(['dividends', 'splits'], axis=1, inplace=True)

# Load database on RDBMS
stock_price_daily.to_sql('stock_price_daily', con=con, if_exists='replace', index=False)

# ---------------------------------------------------------------------------------------------------------------
# Create one table with real time (actually 15min delayed with yahoo) stock prices (1 row per minute)

# Request API
stock_price_minute = tickers.history(period='1mo', interval='1m')

# Reset index and select columns
stock_price_minute = stock_price_minute.reset_index()
stock_price_minute = stock_price_minute[['symbol', 'date', 'close']] 

# Load database on RDBMS
stock_price_minute.to_sql('stock_price_minute', con=con, if_exists='replace', index=False)

# ---------------------------------------------------------------------------------------------------------------
# Create one table with companies geneneral informations

# Request API
query_result_1 = tickers.asset_profile
selected_items_1 = ['sector', 'industry', 'country', 'fullTimeEmployees']

query_result_2 = tickers.price
selected_items_2 = ['regularMarketSource', 'exchange', 'exchangeName', 'exchangeDataDelayedBy', 'marketState', 'quoteType', 'currency', 'shortName']

query_result_list = [query_result_1, query_result_2]
selected_items_list = [selected_items_1, selected_items_2]

# Extract relevant datas
general_information = extract_data_multiple(query_result_list, selected_items_list, query_time=None)

# Load database on RDBMS
general_information.to_sql('general_information', con=con, if_exists='replace', index=False)

# ---------------------------------------------------------------------------------------------------------------
# Create one table with estimates

# Request API with current time
query_result = tickers.financial_data
query_time = dt.datetime.now(tz=timezone).replace(microsecond=0)
selected_items = ['targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice', 'recommendationMean', 'recommendationKey', 'numberOfAnalystOpinions']

# Extract relevant datas
estimates = pd.DataFrame(extract_data_single(query_result, selected_items, query_time=query_time))

# Load database on RDBMS
estimates.to_sql('estimates_daily', con=con, if_exists='replace', index=False)

# ---------------------------------------------------------------------------------------------------------------
# Close connexion

con.close()


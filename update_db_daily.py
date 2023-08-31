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
# Update end of day stock prices

# Request to get last available prices
last_prices = tickers.history(period='1d', interval='1d').reset_index()

# Transform results to make SQL update stock_price_daily
for row in last_prices.values.tolist():
    row[1] = str(row[1]) # Transform timestamp into string corresponding to table format
    update_query = "INSERT INTO stock_price_daily (symbol, date, open, high, low, close, volume, adjclose) VALUES {}".format(tuple(row))
    con.execute(update_query)

# ---------------------------------------------------------------------------------------------------------------
# Update estimates

# Request API with current time
query_result = tickers.financial_data
query_time = dt.datetime.now(tz=timezone).replace(microsecond=0)
selected_items = ['targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice', 'recommendationMean', 'recommendationKey', 'numberOfAnalystOpinions']

# Extract relevant datas
last_estimates = pd.DataFrame(extract_data_single(query_result, selected_items, query_time=query_time))

# Create a new table to store last estimates (queries for last price will be faster than querying the full table)
last_estimates.to_sql('last_estimates', con=con, if_exists='replace', index=False)

# Transform results to make SQL update on estimates_daily
for row in last_prices.values.tolist():
    row[1] = str(row[1]) # Transform timestamp into string corresponding to table format
    update_query = "INSERT INTO estimates_daily (symbol, date, targetHighPrice, targetMeanPrice, targetMedianPrice, recommendationMean, recommendationKey, numberOfAnalystOpinions) VALUES {}".format(tuple(row))
    con.execute(update_query)

    
# ---------------------------------------------------------------------------------------------------------------
# Close connexion

con.close()
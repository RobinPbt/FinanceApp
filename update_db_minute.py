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
# Update real time stock prices

# Request to get last available prices
last_prices = tickers.history(period='1d', interval='1d').reset_index()
last_prices = last_prices[['symbol', 'date', 'close']]

# Create a new table to store last prices (queries for last price will be faster than querying the full table)
last_prices.to_sql('last_stock_prices', con=con, if_exists='replace', index=False)

# Transform results to make SQL update on stock_price_minute
for row in last_prices.values.tolist():
    row[1] = str(row[1]) # Transform timestamp into string corresponding to table format
    update_query = "INSERT INTO stock_price_minute (symbol, date, close) VALUES {}".format(tuple(row))
    con.execute(update_query)

# ---------------------------------------------------------------------------------------------------------------
# Close connexion

con.close()
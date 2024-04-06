import streamlit as st
import streamlit.components.v1 as components
import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import keyboard
import os
import psutil
import time
import datetime as dt
import pytz

from app_functions import *

st.set_page_config(
    page_title="Company details",
    layout = "wide"
)

# Create db connection
con = st.connection(
    "app_finance",
    type="sql",
    url="postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

# -----------------------------Define general functions ---------------------------------

def get_single_symbol_info(table_name, symbol, order_by=None, limit=1):

    if order_by:
        query = """
            SELECT *
            FROM {}
            WHERE symbol = '{}'
            ORDER BY "{}" DESC
            LIMIT {};
        """.format(table_name, symbol, order_by, str(limit))
    else:
        query = """
            SELECT *
            FROM {}
            WHERE symbol = '{}';
        """.format(table_name, symbol)

    query_result = con.query(query)
    symbol_info = pd.DataFrame(query_result)
    return symbol_info

# -----------------------------Define caching functions ---------------------------------

@st.cache_data
def get_tickers():
    query = """
        SELECT DISTINCT(symbol) 
        FROM stock_price_minute;
    """

    tickers_list = con.query(query)
    return tickers_list

@st.cache_data
def get_day_stock_prices(symbol):
    
    # Get current day of week
    timezone = pytz.timezone('CET')
    query_day = dt.datetime.now(tz=timezone).isoweekday()

    # If we are saturday, get the stock prices of the day before (friday)
    if query_day == 6:
        query = """
            SELECT *
            FROM stock_price_minute
            WHERE symbol = '{}' AND date > current_date - INTERVAL '1 day'
            ORDER BY date DESC;
        """.format(symbol)

    # If we are sunday, get the stock prices of 2 days before (friday)
    elif query_day == 7:
        query = """
            SELECT *
            FROM stock_price_minute
            WHERE symbol = '{}' AND date > current_date - INTERVAL '2 day'
            ORDER BY date DESC;
        """.format(symbol)

    # Else get the stock prices of the current day 
    else:
        query = """
            SELECT *
            FROM stock_price_minute
            WHERE symbol = '{}' AND date > current_date
            ORDER BY date DESC;
        """.format(symbol)

    query_result = con.query(query)
    stock_prices = pd.DataFrame(query_result)
    return stock_prices

@st.cache_data
def get_all_symbol_info(symbol):

    general_info = get_single_symbol_info("general_information", symbol)
    ratings = get_single_symbol_info("ratings", symbol, order_by="date", limit=1)
    estimates = get_single_symbol_info("last_estimates", symbol)
    valuation = get_single_symbol_info("last_valuations", symbol)
    financials = get_single_symbol_info("last_financials", symbol)
    dividends = get_single_symbol_info("dividends_weekly", symbol, order_by="exDividendDate", limit=1)
    
    return general_info, ratings, estimates, valuation, financials, dividends

# -----------------------------Define sidebar -------------------------------------------

# Button to shutdown app (in development stage)
exit_app = st.sidebar.button("Shut Down")
if exit_app:
    # Give a bit of delay for user experience
    time.sleep(5)
    # Close streamlit browser tab
    keyboard.press_and_release('ctrl+w')
    # Terminate streamlit python process
    pid = os.getpid()
    p = psutil.Process(pid)
    p.terminate()

# Ticker selection    
tickers = get_tickers()
ticker_selection = st.sidebar.selectbox('Ticker selection', tickers)

# -----------------------------Dashboard ------------------------------------------------

# Load datas
stock_prices = get_day_stock_prices(ticker_selection)
general_info, ratings, estimates, valuation, financials, dividends = get_all_symbol_info(ticker_selection)

# Define containers
header = st.container()
overview = st.container()
prices = st.container()
grades = st.container()
est = st.container()
val = st.container()
fin = st.container()
div = st.container()

with header:
    st.write("""
    # Company information
    Detailled informations about a selected company
    """)

with overview:
    st.write("""## General information""")

    st.write("Symbol: {}".format(general_info['symbol'].values[0]))
    st.write("Company name: {}".format(general_info['shortName'].values[0]))
    st.write("Sector: {}".format(general_info['sector'].values[0]))
    st.write("Industry: {}".format(general_info['industry'].values[0]))
    st.write("Country: {}".format(general_info['country'].values[0]))
    st.write("Exchange: {}".format(general_info['exchangeName'].values[0]))

with prices:
    st.write("""
    ## Stock prices
    Stock price evolution over the day.
    """)

    fig = px.line(stock_prices, x='date', y="close")
    st.plotly_chart(fig)

with grades:
    st.write("""## Ratings""")
    st.dataframe(data=ratings)

with est:
    st.write("""## Estimates""")
    st.dataframe(data=estimates)

with val:
    st.write("""## Valuation""")
    st.dataframe(data=valuation)

with fin:
    st.write("""## Financials""")
    st.dataframe(data=financials)

with div:
    st.write("""## Dividends""")
    st.dataframe(data=dividends)
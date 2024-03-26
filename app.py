import streamlit as st
import streamlit.components.v1 as components
import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import keyboard
import os
import psutil
import time
import datetime as dt
import pytz


# Create db connection
con = st.connection(
    "app_finance",
    type="sql",
    url="postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

# -----------------------------Define caching functions ---------------------------------

@st.cache_data
def load_estimates():
    query = """
    SELECT
        g."symbol", g."shortName",
        p."close" AS "lastPrice",
        p."date" AS "dateLastPrice",
        e."targetMedianPrice",
        e."numberOfAnalystOpinions",
        (e."targetMedianPrice" - p."close") AS "absoluteDiff",
        ((e."targetMedianPrice" - p."close") / p."close") AS "relativeDiff"
    FROM general_information AS g
    LEFT JOIN last_stock_prices p ON g."symbol" = p."symbol"
    LEFT JOIN last_estimates e ON g."symbol" = e."symbol";
    """
    
    query_result = con.query(query)
    estimates = pd.DataFrame(query_result)
    return estimates

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


# -----------------------------Define containers ----------------------------------------
header = st.container()
prediction_container = st.container()
explanation = st.container()
visualization = st.container()

# -----------------------------Load datas -----------------------------------------------

estimates = load_estimates()

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

# Page selection
page_selection = st.sidebar.selectbox('Navigation', ['Estimates', 'Stock prices'])

if page_selection == 'Stock prices':
    
    tickers = get_tickers()
    ticker_selection = st.sidebar.selectbox('Ticker selection', tickers)

# -----------------------------Dashboard ------------------------------------------------
# Presentation of our application
if page_selection == 'Estimates':

    with header:
        st.write("""
        # Estimates vs. current price
        Difference between CAC 40 stock prices and the median target price set by analysts.
        Results are ordered from most undervalued to most overvalued companies on this criteria.
        """)
        
        st.dataframe(data=estimates)

elif page_selection == 'Stock prices':

    stock_prices = get_day_stock_prices(ticker_selection)

    with header:
        st.write("""
        # Stock prices
        Select a ticker to get its stock price evolution over the day.
        """)
        
        st.dataframe(data=stock_prices)
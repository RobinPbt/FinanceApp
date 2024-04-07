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
    page_title="Listed Peers",
    layout = "wide"
)

# Create db connection
con = st.connection(
    "app_finance",
    type="sql",
    url="postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

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
def get_multiples():
    query = """
    SELECT
        g."symbol", 
        g."shortName",
        g."sector",
        g."industry",
        v."priceToBook",
        v."enterpriseToRevenue",
        v."enterpriseToEbitda",
        v."trailingPE"
    FROM general_information AS g
    LEFT JOIN last_valuations v ON g."symbol" = v."symbol"
    """
    
    query_result = con.query(query)
    multiples = pd.DataFrame(query_result)
    return multiples

@st.cache_data
def get_last_valuations():
    query = """
        WITH last_stock_info 
        AS 
        (
        SELECT
            s."symbol",
            s."date",
            s."sharesOutstanding"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM stock_information
            GROUP BY "symbol"
            ) l
        LEFT JOIN stock_information AS s ON s."symbol" = l."symbol" AND s."date" = l."last_date"
        )
        SELECT
            v."symbol", 
            v."bookValue",
            v."enterpriseValue",
            (v."enterpriseValue" - v."marketCap") AS "bridge_enterpriseValue_marketCap",
            v."marketCap",
            last_stock_info."sharesOutstanding",
            (v."marketCap" / last_stock_info."sharesOutstanding") AS "stock_price"
        FROM last_valuations AS v
        LEFT JOIN last_stock_info ON last_stock_info."symbol" = v."symbol";
    """

    query_result = con.query(query)
    last_valuations = pd.DataFrame(query_result)
    return last_valuations

@st.cache_data
def get_financials():
    query = """
        SELECT
            f."symbol", 
            f."totalRevenue",
            f."ebitda",
            (f."totalRevenue" * f."profitMargins") AS "earnings"
        FROM last_financials AS f
    """

    query_result = con.query(query)
    financials = pd.DataFrame(query_result)
    return financials

@st.cache_data
def get_stock_price():
    query = """
    SELECT
        "symbol",
        "close" AS "lastPrice",
        "date" AS "dateLastPrice"
    FROM last_stock_prices;
    """
    
    query_result = con.query(query)
    stock_price = pd.DataFrame(query_result)
    return stock_price


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
multiples = get_multiples()
last_valuations = get_last_valuations()
financials = get_financials()
stock_price = get_stock_price()

# Transform datas
peers = peers_valuation(financials, multiples, last_valuations)
price_diff = price_differential(peers, stock_price)


# Define containers
header = st.container()
global_view = st.container()
detailed_view = st.container()

with header:
    st.write("""
    # Listed Peers
    XX
    """)

with global_view:
    st.write("""## Global view""")
    st.dataframe(data=price_diff)

with detailed_view:
    # Load datas from current selection
    selected_peers = peers[peers['symbol'] == ticker_selection]
    selected_price_diff = price_diff[price_diff['symbol'] == ticker_selection]
    
    x_axis_peers = ['stock_price_book', 'stock_price_revenue', 'stock_price_ebitda', 'stock_price_earnings']
    current_multiples = selected_peers[x_axis_peers]

    x_axis_price = ['mean_stock_price', 'lastPrice']
    current_price = selected_price_diff[x_axis_price]
    current_price.columns = ['Valuation price', 'Current price']

    # Display
    st.write("""## Selected ticker detailed view""")
    st.write("Company name: {}".format(selected_peers['shortName'].values[0]))
    st.write("Relative difference with current price: {:.2%}".format(selected_price_diff['relative_diff'].values[0]))
    st.write("Confidence valuation: {}".format(selected_price_diff['confidence'].values[0]))

    graph_multiples, compare_graph = st.columns(2)

    with graph_multiples:
        # Plot graph with stock prices
        fig = px.bar(
            x=current_multiples.columns, y=current_multiples.values[0], 
            title="Stock price per valuation method", labels={"x" : "", "y" : ""}
        )
        # Add a horizontal line with the mean
        fig.add_shape(
            type="line", line_color="red", line_width=3, opacity=1, line_dash="dot",
            x0=0, x1=1, xref="paper", y0=current_price['Valuation price'].values[0], y1=current_price['Valuation price'].values[0], yref="y"
        )
        st.plotly_chart(fig)

    with compare_graph:
        fig = px.bar(
            x=current_price.columns, y=current_price.values[0], 
            title="Compare valuation against current price", labels={"x" : "", "y" : ""}
        )
        st.plotly_chart(fig)
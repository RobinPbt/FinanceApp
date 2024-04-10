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
        FROM peers_valuation;
    """

    tickers_list = con.query(query)
    return tickers_list

@st.cache_data
def get_peers():
    query = """
        WITH last_peers 
        AS 
        (
        SELECT p.*
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM peers_valuation
            GROUP BY "symbol"
            ) l
        LEFT JOIN peers_valuation AS p ON p."symbol" = l."symbol" AND p."date" = l."last_date"
        )
        SELECT
            last_peers.*, 
            g."shortName",
            s."close" AS "lastPrice"
        FROM last_peers
        LEFT JOIN general_information AS g ON last_peers."symbol" = g."symbol"
        LEFT JOIN last_stock_prices AS s ON last_peers."symbol" = s."symbol" AND last_peers."date" = s."date";
    """
    
    query_result = con.query(query)
    peers_valuation = pd.DataFrame(query_result)
    return peers_valuation


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
peers = get_peers()

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

    st.dataframe(data=peers[['symbol', 'lastPrice', 'shortName', 'date', 'PeersMeanStockPrice', 'PeersRelativeStdStockPrice', 'PeersAbsoluteDiff', 'PeersRelativeDiff', 'PeersConfidence']])

with detailed_view:
    # Load datas from current selection
    selected_peers = peers[peers['symbol'] == ticker_selection]
    
    x_axis_peers = ['stockPriceBook', 'stockPriceRevenue', 'stockPriceEbitda', 'stockPriceEarnings']
    current_multiples = selected_peers[x_axis_peers]

    x_axis_price = ['PeersMeanStockPrice', 'lastPrice']
    current_price = selected_peers[x_axis_price]
    current_price.columns = ['Valuation price', 'Current price']

    # Display
    st.write("""## Selected ticker detailed view""")
    st.write("Company name: {}".format(selected_peers['shortName'].values[0]))
    st.write("Relative difference with current price: {:.2%}".format(selected_peers['PeersRelativeDiff'].values[0]))
    st.write("Confidence valuation: {}".format(selected_peers['PeersConfidence'].values[0]))

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
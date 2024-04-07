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

with header:
    st.write("""
    # Listed Peers
    XX
    """)

    st.dataframe(data=peers)

    st.dataframe(data=price_diff)
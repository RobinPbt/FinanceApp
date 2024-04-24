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
    page_title="Estimates",
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
def load_estimates():
    query = """
        WITH last_estimates_diff 
        AS 
        (
        SELECT
            s."symbol",
            s."EstimatesAbsoluteDiff",
            s."EstimatesRelativeDiff",
            s."EstimatesConfidence"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM estimates_diff
            GROUP BY "symbol"
            ) l
        LEFT JOIN estimates_diff AS s ON s."symbol" = l."symbol" AND s."date" = l."last_date"
        )
        SELECT
            g."symbol", g."shortName",
            p."close" AS "lastPrice",
            p."date" AS "dateLastPrice",
            le."targetMedianPrice",
            le."numberOfAnalystOpinions",
            ef."EstimatesAbsoluteDiff",
            ef."EstimatesRelativeDiff",
            ef."EstimatesConfidence"
        FROM general_information AS g
        LEFT JOIN last_stock_prices p ON g."symbol" = p."symbol"
        LEFT JOIN last_estimates le ON g."symbol" = le."symbol"
        LEFT JOIN last_estimates_diff ef ON g."symbol" = ef."symbol"
        ORDER BY "EstimatesRelativeDiff" DESC;
    """
    
    query_result = con.query(query)
    global_estimates = pd.DataFrame(query_result)
    return global_estimates

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
global_estimates = load_estimates()

# Define containers
header = st.container()

with header:
    st.write("""
    # Estimates vs. current price
    Difference between CAC 40 stock prices and the median target price set by analysts.
    Results are ordered from most undervalued to most overvalued companies on this criteria.
    """)

    st.dataframe(data=global_estimates)
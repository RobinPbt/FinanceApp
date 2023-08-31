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


# Create db connection
con = st.experimental_connection(
    "app_finance",
    type="sql",
    url="sqlite:///app_finance.db"
)

# -----------------------------Define caching functions ---------------------------------

@st.cache_data
def load_datas():
    query = "SELECT \
        g.symbol, g.shortName, \
        p.close AS lastPrice, \
        e.targetMedianPrice, \
        e.numberOfAnalystOpinions, \
        (e.targetMedianPrice - p.close) AS absoluteDiff, \
        ((e.targetMedianPrice - p.close) / p.close) AS relativeDiff \
    FROM general_information AS g \
    LEFT JOIN last_stock_prices p ON g.symbol = p.symbol \
    LEFT JOIN last_estimates e ON g.symbol = e.symbol"
    
    query_result = con.query(query)
    datas = pd.DataFrame(query_result)
    return datas

# -----------------------------Define containers ----------------------------------------
header = st.container()
prediction_container = st.container()
explanation = st.container()
visualization = st.container()

# -----------------------------Load datas -----------------------------------------------

datas = load_datas()

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
# Presentation of our application
with header:
    st.write("""
    # Estimates vs. current price
    Difference between CAC 40 stock prices and the median target price set by analysts.
    Results are ordered from most undervalued to most overvalued companies on this criteria.
    """)
    
    st.dataframe(data=datas)
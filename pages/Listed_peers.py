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
def load_valuations():
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
    global_valuations = pd.DataFrame(query_result)
    return global_valuations

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
global_valuations = load_valuations()

# Define containers
header = st.container()

with header:
    st.write("""
    # Listed Peers
    XX
    """)

    st.dataframe(data=global_valuations)
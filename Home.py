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
    page_title="Home",
    layout = "wide",
    page_icon="👋"
)

# Create db connection
con = st.connection(
    "app_finance",
    type="sql",
    url="postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

# -----------------------------Define general functions ---------------------------------


# -----------------------------Define caching functions ---------------------------------

@st.cache_data
def get_synthesis():
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
        ),
        last_sector_peers_valuation
        AS
        (
        SELECT 
            sp."symbol",
            sp."PeersAbsoluteDiffSector",
            sp."PeersRelativeDiffSector",
            sp."PeersConfidenceSector"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM sector_peers_valuation
            GROUP BY "symbol"
            ) lsp
        LEFT JOIN sector_peers_valuation AS sp ON sp."symbol" = lsp."symbol" AND sp."date" = lsp."last_date"
        ),
        last_clustering_peers_valuation
        AS
        (
        SELECT 
            cp."symbol",
            cp."PeersAbsoluteDiffCluster",
            cp."PeersRelativeDiffCluster",
            cp."PeersConfidenceCluster"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM clustering_peers_valuation
            GROUP BY "symbol"
            ) lcp
        LEFT JOIN clustering_peers_valuation AS cp ON cp."symbol" = lcp."symbol" AND cp."date" = lcp."last_date"
        ),
        last_regression
        AS
        (
        SELECT 
            r."symbol",
            r."RegressionAbsoluteDiff",
            r."RegressionRelativeDiff"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM regression_ML
            GROUP BY "symbol"
            ) lr
        LEFT JOIN regression_ML AS r ON r."symbol" = lr."symbol" AND r."date" = lr."last_date"
        ),
        last_synthesis
        AS
        (
        SELECT 
            sy."symbol",
            sy."GlobalMeanStockPrice",
            sy."GlobalAbsoluteDiff",
            sy."GlobalRelativeDiff"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM synthesis
            GROUP BY "symbol"
            ) lsy
        LEFT JOIN synthesis AS sy ON sy."symbol" = lsy."symbol" AND sy."date" = lsy."last_date"
        )
        SELECT
            g."symbol", g."shortName",
            p."close" AS "lastPrice",
            p."date" AS "dateLastPrice",
            ef."EstimatesAbsoluteDiff",
            ef."EstimatesRelativeDiff",
            ef."EstimatesConfidence",
            spv."PeersAbsoluteDiffSector",
            spv."PeersRelativeDiffSector",
            spv."PeersConfidenceSector",
            cpv."PeersAbsoluteDiffCluster",
            cpv."PeersRelativeDiffCluster",
            cpv."PeersConfidenceCluster",
            reg."RegressionAbsoluteDiff",
            reg."RegressionRelativeDiff",
            synt."GlobalMeanStockPrice",
            synt."GlobalAbsoluteDiff",
            synt."GlobalRelativeDiff"
        FROM general_information AS g
        LEFT JOIN last_stock_prices p ON g."symbol" = p."symbol"
        LEFT JOIN last_estimates_diff ef ON g."symbol" = ef."symbol"
        LEFT JOIN last_sector_peers_valuation spv ON g."symbol" = spv."symbol"
        LEFT JOIN last_clustering_peers_valuation cpv ON g."symbol" = cpv."symbol"
        LEFT JOIN last_regression reg ON g."symbol" = reg."symbol"
        LEFT JOIN last_synthesis synt ON g."symbol" = synt."symbol"
        ORDER BY "GlobalRelativeDiff" DESC;
    """
    
    query_result = con.query(query)
    global_results = pd.DataFrame(query_result)
    return global_results

# -----------------------------Define sidebar -------------------------------------------

# # Button to shutdown app (in development stage)
# exit_app = st.sidebar.button("Shut Down")
# if exit_app:
#     # Give a bit of delay for user experience
#     time.sleep(5)
#     # Close streamlit browser tab
#     keyboard.press_and_release('ctrl+w')
#     # Terminate streamlit python process
#     pid = os.getpid()
#     p = psutil.Process(pid)
#     p.terminate()

# -----------------------------Dashboard ------------------------------------------------

# Define containers
header = st.container()

# Load data
synthesis = get_synthesis()

with header:
    st.write("""
    # Fundamental investment strategy App - Home
    """)

    st.dataframe(data=synthesis)
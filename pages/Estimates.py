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
def get_tickers_names():
    query = """
        SELECT 
            DISTINCT(p.symbol),
            g."shortName"
        FROM general_information p
        LEFT JOIN general_information g ON p.symbol = g.symbol;
    """

    tickers_list = con.query(query)
    return tickers_list

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
            le."numberOfAnalystOpinions",
            le."targetLowPrice",
            le."targetMeanPrice",
            le."targetMedianPrice",
            le."targetHighPrice",
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

# Section selection
section_selection = st.sidebar.selectbox('Section selection', ["Global view", "Individual ticker view"])

# Ticker selection    
tickers_names = get_tickers_names()
company_names = tickers_names["shortName"]
if section_selection == "Individual ticker view":
    company_selection = st.sidebar.selectbox('Company selection', company_names)
    ticker_selection = tickers_names[tickers_names["shortName"] == company_selection]["symbol"].values[0]

# -----------------------------Dashboard ------------------------------------------------

# Load datas
global_estimates = load_estimates()

# Define containers
header = st.container()
global_view = st.container()
detailed_view = st.container()

with header:

    if section_selection == "Global view":
        header_text = """
        # Estimates
        Difference between stock prices and the median target price set by analysts.
        """
    else:
         header_text = """
        # Estimates
        """
    
    st.write(header_text)

if section_selection == "Global view":
    with global_view:
        st.write("""## Global view""")
        all_estimates_section = st.container()
        top_10_section = st.container()
        bottom_10_section = st.container()
        
        with all_estimates_section:
            # show_estimates = st.checkbox("Show all estimates")

            # if show_estimates:
            with st.expander("Show all estimates"):
                st.dataframe(data=global_estimates[["shortName", "lastPrice", "targetMedianPrice", "EstimatesAbsoluteDiff", "EstimatesRelativeDiff", "EstimatesConfidence"]])

        with top_10_section:
            top_10 = global_estimates[["shortName", "EstimatesRelativeDiff"]].dropna(subset="EstimatesRelativeDiff").sort_values(by=["EstimatesRelativeDiff"], ascending=False).head(10)

            # Plot chart with top 10 undervalued stocks
            fig = px.bar(
                data_frame=top_10,
                x="shortName", 
                y="EstimatesRelativeDiff", 
                title="Top 10 undervalued stocks",
                labels={"shortName" : "", "EstimatesRelativeDiff" : "Percentage (+)under/(-)over valuation"}
            )

            st.plotly_chart(fig, use_container_width=True)

        with bottom_10_section:
            bottom_10 = global_estimates[["shortName", "EstimatesRelativeDiff"]].dropna(subset="EstimatesRelativeDiff").sort_values(by=["EstimatesRelativeDiff"], ascending=True).head(10)

            # Plot chart with top 10 undervalued stocks
            fig = px.bar(
                data_frame=bottom_10,
                x="shortName", 
                y="EstimatesRelativeDiff", 
                title="Top 10 overvalued stocks",
                labels={"shortName" : "", "EstimatesRelativeDiff" : "Percentage (+)under/(-)over valuation"}
            )

            st.plotly_chart(fig, use_container_width=True)

elif section_selection == "Individual ticker view":
    with detailed_view:
        
        # Load datas from current selection
        selected_estimates = global_estimates[global_estimates['symbol'] == ticker_selection]

        x_axis_price = ['targetMedianPrice', 'lastPrice']
        current_price = selected_estimates[x_axis_price]
        current_price.columns = ['Selected estimates price', 'Current price']

        x_axis_estimates = ["targetLowPrice", "targetMeanPrice", "targetMedianPrice", "targetHighPrice"]
        current_estimates = selected_estimates[x_axis_estimates]

        # Display
        st.write("""## Selected company detailed view""")

        detailed_view_col1, detailed_view_col2, detailed_view_col3 = st.columns(3)

        with detailed_view_col1:
            st.write("Company name: {}".format(selected_estimates['shortName'].values[0]))
        with detailed_view_col2: 
            st.write("Relative difference with current price: {:.2%}".format(selected_estimates['EstimatesRelativeDiff'].values[0]))
        with detailed_view_col3:
            st.write("Confidence estimates: {}".format(selected_estimates['EstimatesConfidence'].values[0]))

        graph_estimates, compare_graph = st.columns(2)

        with graph_estimates:
           fig = px.bar(
                x=current_estimates.values[0], 
                y=current_estimates.columns, 
                title="Estimates distribution", 
                labels={"x" : "", "y" : ""},
            )
           st.plotly_chart(fig)        
        
        with compare_graph:
            fig = px.bar(
                x=current_price.columns, 
                y=current_price.values[0], 
                title="Compare valuation against current price", 
                labels={"x" : "", "y" : ""}
            )
            st.plotly_chart(fig)
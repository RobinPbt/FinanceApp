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
    page_title="Portfolio",
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
def get_daily_stock_prices():
    query = """
    SELECT *
    FROM stock_price_daily
    ORDER BY symbol ASC;
    """
    
    query_result = con.query(query)
    daily_stock_prices = pd.DataFrame(query_result)
    return daily_stock_prices

@st.cache_data
def get_expected_returns():
    query = """
    SELECT
        s."symbol",
        s."GlobalRelativeDiff"
    FROM (
        SELECT
            "symbol",
            MAX("date") AS last_date
        FROM synthesis
        GROUP BY "symbol"
        ) l
    LEFT JOIN synthesis AS s ON s."symbol" = l."symbol" AND s."date" = l."last_date"
    ORDER BY s."symbol" ASC;
    """
    
    query_result = con.query(query)
    temp_df = pd.DataFrame(query_result)
    temp_df.fillna(0, inplace=True) # If no value for the synthesis of valuation, put an expected return of 0
    expected_returns = temp_df.set_index('symbol')['GlobalRelativeDiff']

    return expected_returns


# -----------------------------Define sidebar -------------------------------------------

target_return = st.sidebar.slider(label="Efficient portfolio target return", min_value=0.01, max_value=1.0, value=0.6, step=0.01)

# -----------------------------Dashboard ------------------------------------------------

# Load datas
expected_returns = get_expected_returns()
daily_stock_prices = get_daily_stock_prices()

# Transform data
daily_stock_prices = daily_stock_prices[['symbol', 'date', 'adjclose']] # Select columns
daily_stock_prices['date'] = daily_stock_prices['date'].apply(lambda x: x.date()) # Convert timestamp into date

today = dt.datetime.now() # Get current date
delta = dt.timedelta(weeks=52) # Get desired delta
date_limit = today - delta # Compute date time delta from now
date_limit = date_limit.date() # Convert timestamp into date

daily_stock_prices = daily_stock_prices[daily_stock_prices['date'] > date_limit] # Delete all data before the caclculated date
daily_stock_prices.drop_duplicates(subset=['symbol', 'date'], inplace=True) # Drop duplicates

daily_stock_prices = daily_stock_prices.pivot(index='symbol', columns='date', values='adjclose') # Reshape dataframe

# Create a portfolio and computing optimal allocations
my_ptf = PortfolioAllocation(daily_stock_prices, expected_returns=expected_returns)
my_ptf.compute_GMVP_weights(display_results=False, only_positive=True)
my_ptf.compute_efficient_portfolio_weights(target_return, display_results=False, only_positive=True)
efficient_portfolio_weights, GMVP_weights = my_ptf.return_weights()
efficient_portfolio_weights = efficient_portfolio_weights[efficient_portfolio_weights != 0]
GMVP_weights = GMVP_weights[GMVP_weights != 0]

# Define containers
header = st.container()
weights = st.container()

with header:
    st.write("""
    # Portfolio allocation
    [Beta version with dirty computations]
    The objective of this module is to create a portfolio allocation optimizing the risk/return couple by using Markowitz efficient frontier with the following inputs:
    - The expected risk is the variance of historical returns for each stock over the preceding year
    - The expected returns are the percentages of under/over valuation given by the fundamental valuation performed in other modules
    """)

GMVP_col, efficient_col = st.columns(2)

with GMVP_col:   

    # Performance
    st.write("""## GMVP""")
        
    GMVP_performance_col1, GMVP_performance_col2 = st.columns(2)

    with GMVP_performance_col1:
        st.metric(
            label="Expected return GMVP", 
            value="{:.2%}".format(my_ptf.expected_return_GMVP)
        ) 
    with GMVP_performance_col2: 
        st.metric(
            label="Expected volatility GMVP", 
            value="{:.6f}".format(np.sqrt(my_ptf.expected_variance_GMVP))
        )
    
    # st.write("Expected return GMVP : {:.6f}".format(my_ptf.expected_return_GMVP))
    # st.write("Expected variance GMVP : {:.6f}".format(my_ptf.expected_variance_GMVP))
    # st.write("Expected volatility GMVP : {:.6f}".format(np.sqrt(my_ptf.expected_variance_GMVP)))

    # Weights pie chart
    fig = px.pie(values=GMVP_weights.values, names=GMVP_weights.index, title='GMVP weights')
    fig.update_traces(textposition='inside')
    fig.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')
    st.plotly_chart(fig)
    
with efficient_col:   
    
    # Performance
    st.write("""## Selected efficient portfolio""")

    efficient_performance_col1, efficient_performance_col2 = st.columns(2)

    with efficient_performance_col1:
        st.metric(
            label="Expected return efficient portfolio", 
            value="{:.2%}".format(my_ptf.expected_return_efficient_portfolio)
        )
    with efficient_performance_col2: 
        st.metric(
            label="Expected volatility efficient portfolio", 
            value="{:.6f}".format(np.sqrt(my_ptf.expected_variance_efficient_portfolio))
        )
    
    # st.write("Expected return efficient portfolio : {:.6f}".format(my_ptf.expected_return_efficient_portfolio))
    # st.write("Expected variance efficient portfolio : {:.6f}".format(my_ptf.expected_variance_efficient_portfolio))
    # st.write("Expected volatility efficient portfolio : {:.6f}".format(np.sqrt(my_ptf.expected_variance_efficient_portfolio)))

    # Weights pie chart
    fig = px.pie(values=efficient_portfolio_weights.values, names=efficient_portfolio_weights.index, title='Efficient portfolio weights')
    fig.update_traces(textposition='inside')
    fig.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')
    st.plotly_chart(fig)
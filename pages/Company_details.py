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
    page_title="Company details",
    layout = "wide"
)

# Create db connection
con = st.connection(
    "app_finance",
    type="sql",
    url="postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

# -----------------------------Define general functions ---------------------------------

def get_single_symbol_info(table_name, symbol, order_by=None, limit=1):

    if order_by:
        query = """
            SELECT *
            FROM {}
            WHERE symbol = '{}'
            ORDER BY "{}" DESC
            LIMIT {};
        """.format(table_name, symbol, order_by, str(limit))
    else:
        query = """
            SELECT *
            FROM {}
            WHERE symbol = '{}';
        """.format(table_name, symbol)

    query_result = con.query(query)
    symbol_info = pd.DataFrame(query_result)
    return symbol_info

# -----------------------------Define caching functions ---------------------------------

@st.cache_data
def get_tickers_names():
    query = """
        SELECT 
            DISTINCT(s.symbol),
            g."shortName"
        FROM stock_price_minute s
        LEFT JOIN general_information g ON s.symbol = g.symbol;
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

@st.cache_data
def get_all_symbol_info(symbol):

    general_info = get_single_symbol_info("general_information", symbol)
    ratings = get_single_symbol_info("ratings", symbol, order_by="date", limit=1)
    estimates = get_single_symbol_info("last_estimates", symbol)
    valuation = get_single_symbol_info("last_valuations", symbol)
    financials = get_single_symbol_info("last_financials", symbol)
    dividends = get_single_symbol_info("dividends_weekly", symbol, order_by="exDividendDate", limit=1)
    
    return general_info, ratings, estimates, valuation, financials, dividends

def millions_formatting(value):

    if not value:
        return value
    else:
        return "€{:,.0f} m".format(value / 10**6)
    
def percentage_formatting(value, format="growth"):

    if not value:
        return value
    else:
        if format == "growth":
            return "{:,.2%} growth".format(value)
        elif format == "margin":
            return "{:,.2%} of revenue".format(value)
        else:
            return "{:,.2%}".format(value)

def ratio_formatting(value):

    if not value:
        return value
    else:
        return "{:,.2f}".format(value)

# -----------------------------Define sidebar -------------------------------------------

# Ticker selection    
tickers_names = get_tickers_names()
company_names = tickers_names["shortName"]
company_selection = st.sidebar.selectbox('Company selection', company_names)
ticker_selection = tickers_names[tickers_names["shortName"] == company_selection]["symbol"].values[0]

# -----------------------------Dashboard ------------------------------------------------

# # CSS style
# text_bg = """
# <style>
# [class="st-emotion-cache-1wmy9hl e1f1d6gn0"] {
# background-color: grey
# }
# </style>
# """
# st.markdown(text_bg, unsafe_allow_html=True)

# Load datas
stock_prices = get_day_stock_prices(ticker_selection)
general_info, ratings, estimates, valuation, financials, dividends = get_all_symbol_info(ticker_selection)

header = st.container()

with header:
    st.write("""
    # Company information
    Detailled informations about a selected company
    """)

# Create tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["General information", "📈 Stock prices", "Key financials", "Estimates", "Ratings"])

with tab1:
    st.write("""## General information""")
    st.write("")
    st.write("Symbol: {}".format(general_info['symbol'].values[0]))
    st.write("")
    st.write("Company name: {}".format(general_info['shortName'].values[0]))
    st.write("")
    st.write("Sector: {}".format(general_info['sector'].values[0]))
    st.write("")
    st.write("Industry: {}".format(general_info['industry'].values[0]))
    st.write("")
    st.write("Country: {}".format(general_info['country'].values[0]))
    st.write("")
    st.write("Exchange: {}".format(general_info['exchangeName'].values[0]))

with tab2:
    st.write("""
    ## Stock prices
    Stock price evolution over the day.
    """)

    fig = px.line(stock_prices, x='date', y="close")
    st.plotly_chart(fig)

with tab3:
    st.write("""## Key financials""")

    st.write(
        """
        <style>
        [data-testid="stMetricDelta"] svg {
            display: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    income_statement = st.container()

    with income_statement:
        st.write("""### P&L""")

        # Get P&L items
        totalRevenue = financials['totalRevenue'].values[0]
        revenueGrowth = financials['revenueGrowth'].values[0]
        grossProfits = financials['grossProfits'].values[0]
        grossMargins = financials['grossMargins'].values[0]
        ebitda = financials['ebitda'].values[0]
        ebitdaMargins = financials['ebitdaMargins'].values[0]
        profitMargins = financials['profitMargins'].values[0]
        profit = totalRevenue * profitMargins

        if not grossProfits and grossMargins:
            grossProfits = totalRevenue * grossMargins

        income_statement_col1, income_statement_col2, income_statement_col3, income_statement_col4 = st.columns(4)

        with income_statement_col1:
            st.metric(
                label="Revenue", 
                value=millions_formatting(totalRevenue), 
                delta=percentage_formatting(revenueGrowth, format="growth"),
            )

        with income_statement_col2:
            st.metric(
                label="Gross profits", 
                value=millions_formatting(grossProfits), 
                delta=percentage_formatting(grossMargins, format="margin"),
                delta_color="off"
            )

        with income_statement_col3:
            st.metric(
                label="EBITDA", 
                value=millions_formatting(ebitda), 
                delta=percentage_formatting(ebitdaMargins, format="margin"),
                delta_color="off"
            )

        with income_statement_col4:
            st.metric(
                label="Profits", 
                value=millions_formatting(profit), 
                delta=percentage_formatting(profitMargins, format="margin"),
                delta_color="off"
            )

    st.divider()

    balance_sheet = st.container()

    with balance_sheet:
        st.write("""### Balance sheet""")

        # Get BS items
        totalCash = financials['totalCash'].values[0]
        totalCashPerShare = financials['totalCashPerShare'].values[0]
        totalDebt = financials['totalDebt'].values[0]
        quickRatio = financials['quickRatio'].values[0]
        currentRatio = financials['currentRatio'].values[0]
        debtToEquity = financials['debtToEquity'].values[0]

        balance_sheet_col1, balance_sheet_col2, balance_sheet_col3, balance_sheet_col4, balance_sheet_col5 = st.columns(5)

        
        with balance_sheet_col1:
            st.metric(
                label="Cash", 
                value=millions_formatting(totalCash)
            )

        with balance_sheet_col2:
            st.metric(
                label="Debt", 
                value=millions_formatting(totalDebt)
            )

        with balance_sheet_col3:
            st.metric(
                label="Quick Ratio", 
                value=ratio_formatting(quickRatio)
            )

        with balance_sheet_col4:
            st.metric(
                label="Current Ratio", 
                value=ratio_formatting(currentRatio)
            )

        with balance_sheet_col5:
            st.metric(
                label="Debt to equity", 
                value=ratio_formatting(debtToEquity)
            )

    st.divider()

    cash_flow_performance = st.container()

    with cash_flow_performance:
        
        cash_flow, performance = st.columns(2)

        with cash_flow:
        
            st.write("""### Cash flow""")

            # Get cash flow items
            freeCashflow = financials['freeCashflow'].values[0]
            operatingCashflow = financials['operatingCashflow'].values[0]

            cash_flow_col1, cash_flow_col2 = st.columns(2)

            with cash_flow_col1:
                st.metric(
                    label="Free cash flow", 
                    value=millions_formatting(freeCashflow)
                )
            
            with cash_flow_col2:
                st.metric(
                    label="Operating cash flow", 
                    value=millions_formatting(operatingCashflow)
                )
        
        with performance:
        
            st.write("""### Perfomance""")

            # Get performance ratios
            returnOnAssets = financials['returnOnAssets'].values[0]
            returnOnEquity = financials['returnOnEquity'].values[0]

            performance_col1, performance_col2 = st.columns(2)

            with performance_col1:
                st.metric(
                    label="Return on Assets", 
                    value=percentage_formatting(returnOnAssets, format="")
                )
            
            with performance_col2:
                st.metric(
                    label="Return on Equity", 
                    value=percentage_formatting(returnOnEquity, format="")
                )

    # selected_items = [
    #         # P&L items
    #         'revenuePerShare', 
    #         'operatingMargins', 
    #         'earningsGrowth', 
    #     ]

with tab4:
    st.write("""## Estimates""")
    
    estimates_col1, estimates_col2 = st.columns([2, 1])

    with estimates_col1:
    
        x_axis_estimates = ["targetLowPrice", "targetMeanPrice", "targetMedianPrice", "targetHighPrice"]
        current_estimates = estimates[x_axis_estimates]

        fig = px.bar(
                    x=current_estimates.columns, 
                    y=current_estimates.values[0], 
                    # title="Estimates distribution", 
                    labels={"x" : "", "y" : ""},
                    orientation="v"
                )
        st.plotly_chart(fig)

    with estimates_col2:

        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        
        st.metric(
            label="Recommandation key", 
            value=estimates['recommendationKey'].values[0]
        )

        st.metric(
            label="Recommandation mean", 
            value=estimates['recommendationMean'].values[0]
        )

        st.metric(
            label="Number of analysts opinion", 
            value=estimates['numberOfAnalystOpinions'].values[0]
        )

with tab5:
    st.write("""## Ratings""")
    st.write("Last rating: {:.0f}/{:.0f}".format(ratings["ratingMonth"].values[0], ratings["ratingYear"].values[0]))

    ten_scores = ["auditRisk", "boardRisk", "compensationRisk", "shareHolderRightsRisk", "overallRisk"]
    fifteen_scores = ["environmentScore", "socialScore", "governanceScore"]
    others = ["totalEsg", "ratingYear", "ratingMonth", "highestControversy"]

    ratings_col1, ratings_col2, ratings_col3 = st.columns(3)

    with ratings_col1:

        table_ten_scores = ratings[ten_scores]
        table_ten_scores = table_ten_scores.T
        table_ten_scores.columns = ["Scores"]

        st.write("""### Ratings over 10""")
        
        st.data_editor(
            table_ten_scores,
            column_config={
                "Scores": st.column_config.ProgressColumn(
                    "Scores",
                    help="A low score indicates a low risk",
                    format="%f",
                    min_value=0,
                    max_value=10,
                ),
            },
            hide_index=False,
        )

    with ratings_col2:

        table_fifteen_scores = ratings[fifteen_scores]
        table_fifteen_scores = table_fifteen_scores.T
        table_fifteen_scores.columns = ["Scores"]

        st.write("""### Ratings over 15""")
        
        st.data_editor(
            table_fifteen_scores,
            column_config={
                "Scores": st.column_config.ProgressColumn(
                    "Scores",
                    help="A high score indicates a good performance",
                    format="%f",
                    min_value=0,
                    max_value=15,
                ),
            },
            hide_index=False,
        )

    with ratings_col3:

        st.write("""### Other ratings items""")

        st.metric(
            label="ESG score", 
            value=ratings["totalEsg"].values[0]
        )

        st.metric(
            label="Highest Controversy", 
            value=ratings["highestControversy"].values[0]
        )


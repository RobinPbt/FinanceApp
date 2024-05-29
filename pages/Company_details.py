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
from PIL import Image

from langchain.chat_models import ChatOpenAI
from langchain.tools import DuckDuckGoSearchRun, WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.chains import LLMChain

from app_functions import *

# ---------------------------------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------------------------------

# Global layout
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

# ---------------------------------------------------------------------------------------------------
# General functions 

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
  
# ---------------------------------------------------------------------------------------------------
# Define caching functions

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
def get_intraday_stock_prices(symbol):
    
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
    dividends = get_single_symbol_info("dividends_weekly", symbol, order_by="exDividendDate", limit=1)

    query = """
        SELECT
            hf."symbol", 
            hf."TotalRevenue",
            hf."RevenueGrowth",
            hf."GrossProfit",
            hf."GrossMargin",
            hf."EBITDA",
            hf."EBITDAMargin",
            hf."EBIT",
            hf."EBITMargin",
            hf."NetIncome",
            hf."NetIncomeMargin",
            hf."OperatingCashFlow",
            hf."CapitalExpenditure",
            hf."ConversionEBITDAFreeCashFlow",
            hf."FreeCashFlow",
            hf."TotalDebt",
            hf."CashAndCashEquivalents",
            hf."Leverage",
            hf."WorkingCapital",
            hf."TotalEquityGrossMinorityInterest",
            hf."ReturnOnEquity",
            hf."ReturnOnAssets"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM historical_financials
            GROUP BY "symbol"
            ) l
        LEFT JOIN historical_financials AS hf ON hf."symbol" = l."symbol" AND hf."date" = l."last_date"
        WHERE hf."symbol" = '{}';
    """.format(symbol)


    query_result = con.query(query)
    financials = pd.DataFrame(query_result)
    
    return general_info, ratings, estimates, valuation, financials, dividends

@st.cache_data
def get_daily_stock_prices_and_valuations(symbol):

    daily_stock_prices = get_single_symbol_info("stock_price_daily", symbol)
    daily_valuations = get_single_symbol_info("valuations_daily", symbol)

    # Transform data
    daily_stock_prices = daily_stock_prices[['date', 'adjclose']] # Select columns
    daily_stock_prices['date'] = daily_stock_prices['date'].apply(lambda x: x.date()) # Convert timestamp into date

    daily_valuations = daily_valuations[['date', 'EnterpriseValueRevenueMultiple', 'EnterpriseValueEBITDAMultiple', 'PriceToBookRatio', 'PriceEarningsRatio']] # Select columns
    daily_valuations['date'] = daily_valuations['date'].apply(lambda x: x.date()) # Convert timestamp into date

    timezone = pytz.timezone('CET')
    query_time = dt.datetime.now(tz=timezone) # Get current time
    
    one_year_delta = dt.timedelta(weeks=52) # Get desired delta
    one_year_from_now = query_time - one_year_delta # Compute date time delta from now
    one_year_from_now = one_year_from_now.date() # Convert timestamp into date

    daily_stock_prices = daily_stock_prices[daily_stock_prices['date'] > one_year_from_now] # Delete all data before the caclculated date
    daily_stock_prices.drop_duplicates(subset=['date'], inplace=True) # Drop duplicates

    daily_valuations = daily_valuations[daily_valuations['date'] > one_year_from_now] # Delete all data before the caclculated date
    daily_valuations.drop_duplicates(subset=['date'], inplace=True) # Drop duplicates

    return daily_stock_prices, daily_valuations

# ---------------------------------------------------------------------------------------------------
# LLM model and functions

# Code deactivated to avoid incurring API costs during testing

# # Create a model
# chat_model = ChatOpenAI(
#     model_name="gpt-3.5-turbo", 
#     temperature=0.01, 
#     openai_api_key=os.getenv("OPENAI_API_KEY")
# )

# # setting up the script prompt templates
# script_template = PromptTemplate(
#     input_variables = ['company_name', 'wikipedia_research', 'web_search'], 
#     template='''Give me a 10 lines description of the activity of the company {company_name} 
#     including the location of its headquarter, its number of employees over the world, its underlying markets and its main competitors.
#     You will make use of the information and knowledge obtained from the Wikipedia research:{wikipedia_research}
#     and make use of the additional information from the web search:{web_search} ''',
# )

# # memory buffer
# memory = ConversationBufferMemory(
#     input_key='company_name', 
#     memory_key='chat_history')

# # LLM chain
# chain = LLMChain(
#     llm=chat_model, 
#     prompt=script_template, 
#     verbose=True, 
#     output_key='script', 
#     memory=memory)

# async def generate_script(company_name):
#     wikipedia_research = fetch_wikipedia_data(company_name)
#     web_search = fetch_web_search_results(company_name)
#     script = chain.run(
#         company_name=company_name, 
#         wikipedia_research=wikipedia_research, 
#         web_search=web_search
#     )
    
#     return script, wikipedia_research, web_search

# # This function is a wrapper around the async function 'generate_script'
# # It allows us to call the async function in a synchronous way
# # using 'asyncio.run'
# def run_generate_script(company_name):
#     """
#     Wrapper function to run the async function 'generate_script'
#     in a synchronous way
    
#     Args:
#         input_text (str): The input text passed to the language model

#     Returns:
#         tuple: A tuple containing the script, web search and wikipedia research
#     """
#     return asyncio.run(generate_script(company_name))

# def stream_data(script):
#     for word in script.split(" "):
#         yield word + " "
#         time.sleep(0.02)

# ---------------------------------------------------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------
# Sidebar

# Ticker selection    
tickers_names = get_tickers_names()
company_names = tickers_names["shortName"]
company_selection = st.sidebar.selectbox('Company selection', company_names)
ticker_selection = tickers_names[tickers_names["shortName"] == company_selection]["symbol"].values[0]

# ---------------------------------------------------------------------------------------------------
# Header

# Load datas depending on ticker_selection
intraday_stock_prices = get_intraday_stock_prices(ticker_selection)
daily_stock_prices, daily_valuations = get_daily_stock_prices_and_valuations(ticker_selection)
general_info, ratings, estimates, valuation, financials, dividends = get_all_symbol_info(ticker_selection)

header = st.container()

with header:
    st.write("""
    # Company information
    Detailled informations about a selected company
    """)

# Create tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["General information", "📈 Stock prices", "Key financials", "Estimates", "Ratings", "Multiples"])

# ---------------------------------------------------------------------------------------------------
# General information tab

with tab1:
    
    general_info_col, LLM_col = st.columns([1, 3])
    
    with general_info_col:
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

    with LLM_col:
        # LLM function call
        # script, wikipedia_research, web_search = run_generate_script(company_selection)

        # writing the title and script
        st.write("""## Activity description *(powered by ChatGPT)*""")
        image = Image.open("ChatGPT_logo.png")
        st.image(image, width=30)

        # st.write(script) 
        # st.write_stream(stream_data(script))
        
        # with st.expander('Wikipedia-based exploration: '): 
        #     st.info(wikipedia_research)

        # with st.expander('Web-based exploration: '):
        #     st.info(web_search)

# ---------------------------------------------------------------------------------------------------
# Stock prices tab

with tab2:
    st.write("""## Stock prices""")

    period_selection = st.radio(label="Period selection", options=["Intraday", "1 year"], label_visibility="collapsed")

    if period_selection == "Intraday":

        fig = px.line(
            intraday_stock_prices, 
            x='date', 
            y="close",
            labels= {"date" : "", "close" : ""}
        )
        st.plotly_chart(fig, use_container_width=True)

    elif period_selection == "1 year":

        fig = px.line(
            daily_stock_prices, 
            x='date', 
            y="adjclose",
            labels= {"date" : "", "adjclose" : ""}
        )
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------------------------------
# Financials tab

with tab3:
    st.write("""## Key financials""")

    # CSS to withdraw arrows from st.metric
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
        totalRevenue = financials['TotalRevenue'].values[0]
        revenueGrowth = financials['RevenueGrowth'].values[0]
        grossProfits = financials['GrossProfit'].values[0]
        grossMargins = financials['GrossMargin'].values[0]
        ebitda = financials['EBITDA'].values[0]
        ebitdaMargins = financials['EBITDAMargin'].values[0]
        profit = financials['NetIncome'].values[0]
        profitMargins = financials['NetIncomeMargin'].values[0]

        if not grossProfits and grossMargins:
            grossProfits = totalRevenue * grossMargins

        income_statement_col1, income_statement_col2, income_statement_col3, income_statement_col4 = st.columns(4)

        with income_statement_col1:
            st.metric(
                label="Revenue", 
                value=metrics_value_formatting(totalRevenue, value_type="millions"), 
                delta=metrics_value_formatting(revenueGrowth, value_type="percentage", percentage_format="growth"),
            )

        with income_statement_col2:
            st.metric(
                label="Gross profits", 
                value=metrics_value_formatting(grossProfits, value_type="millions"), 
                delta=metrics_value_formatting(grossMargins, value_type="percentage", percentage_format="margin"),
                delta_color="off"
            )

        with income_statement_col3:
            st.metric(
                label="EBITDA", 
                value=metrics_value_formatting(ebitda, value_type="millions"), 
                delta=metrics_value_formatting(ebitdaMargins, value_type="percentage", percentage_format="margin"),
                delta_color="off"
            )

        with income_statement_col4:
            st.metric(
                label="Profits", 
                value=metrics_value_formatting(profit, value_type="millions"), 
                delta=metrics_value_formatting(profitMargins, value_type="percentage", percentage_format="margin"),
                delta_color="off"
            )

    st.divider()

    balance_sheet = st.container()

    with balance_sheet:
        st.write("""### Balance sheet""")

        # Get BS items
        totalCash = financials['CashAndCashEquivalents'].values[0]
        totalDebt = financials['TotalDebt'].values[0]
        leverage = financials['Leverage'].values[0]
        WorkingCapital = financials['WorkingCapital'].values[0]
        totalEquity = financials['TotalEquityGrossMinorityInterest'].values[0]

        balance_sheet_col1, balance_sheet_col2, balance_sheet_col3, balance_sheet_col4, balance_sheet_col5 = st.columns(5)

        
        with balance_sheet_col1:
            st.metric(
                label="Cash", 
                value=metrics_value_formatting(totalCash, value_type="millions")
            )

        with balance_sheet_col2:
            st.metric(
                label="Debt", 
                value=metrics_value_formatting(totalDebt, value_type="millions")
            )

        with balance_sheet_col3:
            st.metric(
                label="Leverage", 
                value=metrics_value_formatting(leverage, value_type="ratio")
            )

        with balance_sheet_col4:
            st.metric(
                label="Working Capital", 
                value=metrics_value_formatting(WorkingCapital, value_type="millions")
            )

        with balance_sheet_col5:
            st.metric(
                label="Total Equity", 
                value=metrics_value_formatting(totalEquity, value_type="millions")
            )

    st.divider()

    cash_flow_performance = st.container()

    with cash_flow_performance:
        
        cash_flow, performance = st.columns(2)

        with cash_flow:
        
            st.write("""### Cash flow""")
            
            # Get cash flow items
            freeCashflow = financials['FreeCashFlow'].values[0]
            ConversionEBITDAFreeCashFlow = financials['ConversionEBITDAFreeCashFlow'].values[0]
            operatingCashflow = financials['OperatingCashFlow'].values[0]

            cash_flow_col1, cash_flow_col2 = st.columns(2)

            with cash_flow_col1:
                st.metric(
                    label="Free cash flow", 
                    value=metrics_value_formatting(freeCashflow, value_type="millions"),
                    delta=metrics_value_formatting(ConversionEBITDAFreeCashFlow, value_type="percentage", percentage_format="ebitda margin"),
                    delta_color="off"
                )
            
            with cash_flow_col2:
                st.metric(
                    label="Operating cash flow", 
                    value=metrics_value_formatting(operatingCashflow, value_type="millions")
                )
        
        with performance:
        
            st.write("""### Perfomance""")
 
            # Get performance ratios
            returnOnAssets = financials['ReturnOnAssets'].values[0]
            returnOnEquity = financials['ReturnOnEquity'].values[0]

            performance_col1, performance_col2 = st.columns(2)

            with performance_col1:
                st.metric(
                    label="Return on Assets", 
                    value=metrics_value_formatting(returnOnAssets, value_type="percentage", percentage_format="")
                )
            
            with performance_col2:
                st.metric(
                    label="Return on Equity", 
                    value=metrics_value_formatting(returnOnEquity, value_type="percentage", percentage_format="")
                )

# ---------------------------------------------------------------------------------------------------
# Estimates tab

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

# ---------------------------------------------------------------------------------------------------
# Ratings tab

with tab5:
    st.write("""## Ratings""")
    st.write("Last rating: {:.0f}/{:.0f}".format(ratings["ratingMonth"].values[0], ratings["ratingYear"].values[0]))

    ten_scores = ["auditRisk", "boardRisk", "compensationRisk", "shareHolderRightsRisk", "overallRisk"]
    fifteen_scores = ["environmentScore", "socialScore", "governanceScore"]

    ratings_col1, ratings_col2, ratings_col3 = st.columns(3)

    with ratings_col1:

        table_ten_scores = ratings[ten_scores]
        table_ten_scores = table_ten_scores.T
        table_ten_scores.columns = ["Scores"]

        st.write("""
        ### Governance ratings 
        *(1 low risk to 10 high risk)*
        """)
        
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

        st.write("""
        ### ESG ratings 
        *(0 low performance to 15 good performance)*
        """)
        
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

# ---------------------------------------------------------------------------------------------------
# Multiples tab

with tab6:
    st.write("""## Multiples""")

    multiple_selection = st.radio(label="Multiple selection", options=["xRevenue", "xEBITDA", "Price to Book", "Price Earnings"], label_visibility="collapsed")

    if multiple_selection == "xRevenue":

        fig = px.line(
            daily_valuations, 
            x='date', 
            y="EnterpriseValueRevenueMultiple",
            labels= {"date" : "", "EnterpriseValueRevenueMultiple" : ""}
        )
        st.plotly_chart(fig, use_container_width=True)

    elif multiple_selection == "xEBITDA":

        fig = px.line(
            daily_valuations, 
            x='date', 
            y="EnterpriseValueEBITDAMultiple",
            labels= {"date" : "", "EnterpriseValueEBITDAMultiple" : ""}
        )
        st.plotly_chart(fig, use_container_width=True)

    elif multiple_selection == "Price to Book":

        fig = px.line(
            daily_valuations, 
            x='date', 
            y="PriceToBookRatio",
            labels= {"date" : "", "PriceToBookRatio" : ""}
        )
        st.plotly_chart(fig, use_container_width=True)

    elif multiple_selection == "Price Earnings":

        fig = px.line(
            daily_valuations, 
            x='date', 
            y="PriceEarningsRatio",
            labels= {"date" : "", "PriceEarningsRatio" : ""}
        )
        st.plotly_chart(fig, use_container_width=True)


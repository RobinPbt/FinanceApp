import streamlit as st
import streamlit.components.v1 as components
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
import pickle
from PIL import Image

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from app_functions import *

# ---------------------------------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------------------------------

# Global layout
st.set_page_config(
    page_title="ML regression",
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
def get_regression():
    query = """
        WITH last_regression 
        AS 
        (
        SELECT r.*
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM regression_ML
            GROUP BY "symbol"
            ) l
        LEFT JOIN regression_ML AS r ON r."symbol" = l."symbol" AND r."date" = l."last_date"
        )
        SELECT
            last_regression.*, 
            g."shortName",
            s."close" AS "lastPrice"
        FROM last_regression
        LEFT JOIN general_information AS g ON last_regression."symbol" = g."symbol"
        LEFT JOIN last_stock_prices AS s ON last_regression."symbol" = s."symbol" AND last_regression."date" = s."date";
    """
    
    query_result = con.query(query)
    regression_ML = pd.DataFrame(query_result)
    return regression_ML

@st.cache_data
def get_model():

    model = pickle.load(open('./test_model_2.pkl', 'rb'))

    return model

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

# Load datas and model
model = get_model()
regression_ML = get_regression()

header = st.container()

with header:
    st.write("""
    # ML regression model
    Machine learning model trained to predict companies' intrinsic value based on its current financials informations
    """)

# Create tabs
tab1, tab2 = st.tabs(["Results", "Model features"])

# ---------------------------------------------------------------------------------------------------
# Global view

with tab1:

    st.write("""## Global view""")
    all_estimates_section = st.container()
    top_10_section = st.container()
    bottom_10_section = st.container()
    
    with all_estimates_section:
        
        with st.expander("Show all predictions"):
            st.dataframe(data=regression_ML[["shortName", "lastPrice", "RegressionPrediction", "RegressionAbsoluteDiff", "RegressionRelativeDiff"]])

    with top_10_section:
        top_10 = regression_ML[["shortName", "RegressionRelativeDiff"]].sort_values(by=["RegressionRelativeDiff"], ascending=False).head(10)

        # Plot chart with top 10 undervalued stocks
        fig = px.bar(
            data_frame=top_10,
            x="shortName", 
            y="RegressionRelativeDiff", 
            title="Top 10 undervalued stocks",
            labels={"shortName" : "", "RegressionRelativeDiff" : "Percentage (+)under/(-)over valuation"}
        )

        st.plotly_chart(fig, use_container_width=True)

    with bottom_10_section:
        bottom_10 = regression_ML[["shortName", "RegressionRelativeDiff"]].sort_values(by=["RegressionRelativeDiff"], ascending=True).head(10)

        # Plot chart with top 10 undervalued stocks
        fig = px.bar(
            data_frame=bottom_10,
            x="shortName", 
            y="RegressionRelativeDiff", 
            title="Top 10 overvalued stocks",
            labels={"shortName" : "", "RegressionRelativeDiff" : "Percentage (+)under/(-)over valuation"}
        )

        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------------------------------
# Model features

with tab2:
        
    # Plot feature importance
    feature_list = [
        "date", 
        "TotalRevenue", 
        "RevenueGrowth", 
        "GrossMargin", 
        "EBITDAMargin", 
        "EBITMargin", 
        "PretaxIncomeMargin", 
        "NetIncomeMargin", 
        "Leverage", 
        "PercentageCapitalExpenditureRevenue", 
        "ReturnOnEquity", 
        "ReturnOnAssets", 
        "FreeCashFlowMargin", 
        "ConversionEBITDAFreeCashFlow", 
        "ConversionNetIncomeFreeCashFlow", 
        "ConversionEBITDACash", 
        "ConversionNetIncomeCash", 
        "sector", 
        "industry", 
        "country",
        "fullTimeEmployees",
    ]
    
    feat_importance = model.feature_importances_

    feat_df = pd.DataFrame({'Features' : feature_list, 'Importance' : feat_importance})
    feat_df = feat_df.sort_values(by='Importance', ascending=True)

    fig = px.bar(
        data_frame=feat_df,
        x='Importance', 
        y='Features', 
        title="Features importance", 
        labels={"Importance" : "", "Features" : ""},
        orientation="h",
        height=1000
    )
    st.plotly_chart(fig, use_container_width=True)
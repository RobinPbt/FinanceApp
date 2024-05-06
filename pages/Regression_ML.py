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
def get_ML_features():
    
    query = """
    WITH last_stock_info 
    AS 
    (
    SELECT si.*
    FROM (
        SELECT
            "symbol",
            MAX("date") AS last_date
        FROM stock_information
        GROUP BY "symbol"
        ) lsi
    LEFT JOIN stock_information AS si ON si."symbol" = lsi."symbol" AND si."date" = lsi."last_date"
    ),
    last_ratings
    AS
    (
    SELECT ra.*
    FROM (
        SELECT
            "symbol",
            MAX("date") AS last_date
        FROM ratings
        GROUP BY "symbol"
        ) lra
    LEFT JOIN ratings AS ra ON ra."symbol" = lra."symbol" AND ra."date" = lra."last_date"
    )
    SELECT 
        gi."shortName",
        gi."sector",
        gi."industry",
        gi."fullTimeEmployees",
        gi."regularMarketSource",
        gi."exchange",
        gi."quoteType",
        gi."currency",
        last_ratings."auditRisk",
        last_ratings."boardRisk",
        last_ratings."compensationRisk",
        last_ratings."shareHolderRightsRisk",
        last_ratings."overallRisk",
        last_ratings."totalEsg",
        last_ratings."environmentScore",
        last_ratings."socialScore",
        last_ratings."governanceScore",
        last_ratings."highestControversy",
        last_stock_info."floatShares",
        last_stock_info."sharesOutstanding",
        last_stock_info."heldPercentInsiders",
        last_stock_info."heldPercentInstitutions",  
        last_financials."totalCash",
        last_financials."totalCashPerShare",
        last_financials."totalDebt",
        last_financials."quickRatio",
        last_financials."currentRatio",
        last_financials."debtToEquity",
        last_financials."totalRevenue",
        last_financials."revenuePerShare",
        last_financials."revenueGrowth",
        last_financials."grossProfits",
        last_financials."grossMargins",
        last_financials."operatingMargins",
        last_financials."ebitda",
        last_financials."ebitdaMargins",
        last_financials."earningsGrowth",
        last_financials."profitMargins",
        last_financials."freeCashflow",
        last_financials."operatingCashflow",
        last_financials."returnOnAssets",
        last_financials."returnOnEquity",
        last_estimates."targetHighPrice",
        last_estimates."targetLowPrice",
        last_estimates."targetMeanPrice",
        last_estimates."targetMedianPrice",
        last_estimates."recommendationMean",
        last_estimates."recommendationKey",
        last_estimates."numberOfAnalystOpinions",
        last_stock_prices."close"
    FROM general_information AS gi
    LEFT JOIN last_ratings ON gi."symbol" = last_ratings.symbol
    LEFT JOIN last_stock_info ON gi."symbol" = last_stock_info.symbol
    LEFT JOIN last_financials ON gi."symbol" = last_financials.symbol
    LEFT JOIN last_estimates ON gi."symbol" = last_estimates.symbol
    LEFT JOIN last_stock_prices ON gi."symbol" = last_stock_prices.symbol
    """

    query_result = con.query(query)
    ML_features = pd.DataFrame(query_result)
    return ML_features

@st.cache_data
def get_model():

    preprocessor = pickle.load(open('./test_preprocessor.pkl', 'rb'))
    model = pickle.load(open('./test_model.pkl', 'rb'))

    return preprocessor, model


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
ML_features = get_ML_features()
preprocessor, model = get_model()

# Preprocess features
names = ML_features['shortName']
current_price = ML_features['close']
X = ML_features.drop(['shortName', 'close'], axis=1)
X_prep = preprocessor.transform(X)

# Predict
predictions = model.predict(X_prep)

# Compute differences
compare = pd.DataFrame(data=current_price)
compare['predictions'] = predictions
compare['RegressionAbsoluteDiff'] = compare['predictions'] - compare['close']
compare['RegressionRelativeDiff'] = compare['RegressionAbsoluteDiff'] / compare['close']
compare['shortName'] = names
compare = compare[['shortName', 'close', 'predictions', 'RegressionAbsoluteDiff', 'RegressionRelativeDiff']]

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
            st.dataframe(data=compare)

    with top_10_section:
        top_10 = compare[["shortName", "RegressionRelativeDiff"]].sort_values(by=["RegressionRelativeDiff"], ascending=False).head(10)

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
        bottom_10 = compare[["shortName", "RegressionRelativeDiff"]].sort_values(by=["RegressionRelativeDiff"], ascending=True).head(10)

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
    feature_list = list(X.columns)
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
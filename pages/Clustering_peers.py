import streamlit as st
import streamlit.components.v1 as components
import sqlite3
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import keyboard
import os
import psutil
import time
import datetime as dt
import pytz
import pickle

from app_functions import *

# ---------------------------------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------------------------------

st.set_page_config(
    page_title="Clustering Peers",
    layout = "wide"
)

# Create db connection
con = st.connection(
    "app_finance",
    type="sql",
    url="postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

# ---------------------------------------------------------------------------------------------------
# Define caching functions

@st.cache_data
def get_tickers_names():
    query = """
        SELECT 
            DISTINCT(p.symbol),
            g."shortName"
        FROM peers_valuation p
        LEFT JOIN general_information g ON p.symbol = g.symbol;
    """

    tickers_list = con.query(query)
    return tickers_list

@st.cache_data
def get_peers_tables():
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
    LEFT JOIN last_valuations v ON g."symbol" = v."symbol";
    """

    query_result = con.query(query)
    multiples = pd.DataFrame(query_result)

    query = """
        WITH last_stock_info 
        AS 
        (
        SELECT
            s."symbol",
            s."date",
            s."sharesOutstanding"
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM stock_information
            GROUP BY "symbol"
            ) l
        LEFT JOIN stock_information AS s ON s."symbol" = l."symbol" AND s."date" = l."last_date"
        )
        SELECT
            v."symbol", 
            v."bookValue",
            v."enterpriseValue",
            (v."enterpriseValue" - v."marketCap") AS "BridgeEnterpriseValueMarketCap",
            v."marketCap",
            last_stock_info."sharesOutstanding",
            (v."marketCap" / last_stock_info."sharesOutstanding") AS "stock_price"
        FROM last_valuations AS v
        LEFT JOIN last_stock_info ON last_stock_info."symbol" = v."symbol";
    """

    query_result = con.query(query)
    last_valuations = pd.DataFrame(query_result)

    query = """
        SELECT
            "symbol", 
            "totalRevenue",
            "ebitda",
            ("totalRevenue" * "profitMargins") AS "earnings"
        FROM last_financials
    """

    query_result = con.query(query)
    financials = pd.DataFrame(query_result)

    query = """
    SELECT
        "symbol",
        "close" AS "lastPrice",
        "date"
    FROM last_stock_prices;
    """

    query_result = con.query(query)
    stock_price = pd.DataFrame(query_result)
    
    return multiples, last_valuations, financials, stock_price

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
    model = pickle.load(open('./test_clustering.pkl', 'rb'))

    return preprocessor, model

def create_bar_chart(cluster_multiples, multiple, title):
    """Function to plot horizontal bar chart with mean cluster multiples"""

    fig = px.bar(
        data_frame=cluster_multiples,
        x=multiple, 
        y="cluster", 
        title=title, 
        labels={multiple : "", "cluster" : ""},
        orientation="h"
    )

    return fig

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

# Load datas
multiples, last_valuations, financials, stock_price = get_peers_tables()
ML_features = get_ML_features()
preprocessor, model = get_model()

# Preprocess features
names = ML_features['shortName']
current_price = ML_features['close']
X = ML_features.drop(['shortName', 'close'], axis=1)
X_prep = preprocessor.transform(X)

# Predict clusters
clusters = model.predict(X_prep)

# Perform peers valuation based on clusters
multiples['cluster'] = clusters
mean_cluster_multiples, cluster_peers = peers_valuation("cluster", financials, multiples, last_valuations, stock_price)

# Define containers
header = st.container()
# global_view = st.container()
# multiples_view = st.container()
# detailed_view = st.container()

with header:
       
    st.write("""
    # CLustering Peers
    Difference between stock prices and the price estimated with a peers valuation approach by cluster resulting from a ML algorithm.
    """)

# Create tabs
tab1, tab2, tab3 = st.tabs(["Global view", "Mean cluster multiples", "Individual ticker view"])

# ---------------------------------------------------------------------------------------------------
# Global view

with tab1:

    st.write("""## Global view""")
    st.write("""
    Below are the main steps for the valuation approach:
    - We compute multiples for each company (revenue, ebitda, price to book, price earnings)
    - We train a clustering model to define homegeneous groups of companies
    - We compute mean multiples per cluster by aggregating previous results
    - We compute the theoritical valuation for each company by applying mean cluster multiples to company financial items
    - We compute the difference between the current price and the theoritical valuation """)

    all_peers_section = st.container()
    top_10_section = st.container()
    bottom_10_section = st.container()

    with all_peers_section:
        
        # show_peers = st.checkbox("Show all peers")
        # if show_peers:
        with st.expander("Show all peers"):
            st.dataframe(
                data=cluster_peers[[
                    'shortName',
                    'cluster',
                    'lastPrice',
                    'date', 
                    'PeersMeanStockPriceCluster', 
                    'PeersRelativeStdStockPriceCluster', 
                    'PeersAbsoluteDiffCluster', 
                    'PeersRelativeDiffCluster', 
                    'PeersConfidenceCluster'
                ]]
            )

    with top_10_section:
        top_10 = cluster_peers[["shortName", "PeersRelativeDiffCluster"]].dropna(subset="PeersRelativeDiffCluster").sort_values(by=["PeersRelativeDiffCluster"], ascending=False).head(10)

        # Plot chart with top 10 undervalued stocks
        fig = px.bar(
            data_frame=top_10,
            x="shortName", 
            y="PeersRelativeDiffCluster", 
            title="Top 10 undervalued stocks",
            labels={"shortName" : "", "PeersRelativeDiffCluster" : "Percentage (+)under/(-)over valuation"}
        )

        st.plotly_chart(fig, use_container_width=True)

    with bottom_10_section:
        bottom_10 = cluster_peers[["shortName", "PeersRelativeDiffCluster"]].dropna(subset="PeersRelativeDiffCluster").sort_values(by=["PeersRelativeDiffCluster"], ascending=True).head(10)

        # Plot chart with top 10 undervalued stocks
        fig = px.bar(
            data_frame=bottom_10,
            x="shortName", 
            y="PeersRelativeDiffCluster", 
            title="Top 10 overvalued stocks",
            labels={"shortName" : "", "PeersRelativeDiffCluster" : "Percentage (+)under/(-)over valuation"}
        )

        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------------------------------
# Sector multiples

with tab2:
    # Order multiples
    mean_cluster_multiples = mean_cluster_multiples.sort_values(by="cluster", ascending=False)

    # Section title
    st.write("""## Mean cluster multiples""")

    price_to_book_col, enterprise_to_revenue_col, enterprise_to_ebitda_col, trailing_PE_col = st.columns([1, 1, 1, 1])

    with price_to_book_col:
        fig = create_bar_chart(mean_cluster_multiples, "MeanClusterPriceToBook", "Price to Book")                
        st.plotly_chart(fig, use_container_width=True)
    with enterprise_to_revenue_col:
        fig = create_bar_chart(mean_cluster_multiples, "MeanClusterEnterpriseToRevenue", "Enterprise to Revenue")                
        st.plotly_chart(fig, use_container_width=True)
    with enterprise_to_ebitda_col:
        fig = create_bar_chart(mean_cluster_multiples, "MeanClusterEnterpriseToEbitda", "Enterprise to Ebitda")                
        st.plotly_chart(fig, use_container_width=True)
    with trailing_PE_col:
        fig = create_bar_chart(mean_cluster_multiples, "MeanClusterTrailingPE", "Trailing PE")                
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------------------------------
# Individual ticker view

with tab3:
    # Load datas from current selection
    selected_peers = cluster_peers[cluster_peers['symbol'] == ticker_selection]
    selected_cluster_multiples = mean_cluster_multiples[mean_cluster_multiples["cluster"] == selected_peers['cluster'].values[0]].drop("cluster", axis=1)

    x_axis_peers = ['stockPriceBookCluster', 'stockPriceRevenueCluster', 'stockPriceEbitdaCluster', 'stockPriceEarningsCluster']
    current_multiples = selected_peers[x_axis_peers]

    x_axis_price = ['PeersMeanStockPriceCluster', 'lastPrice']
    current_price = selected_peers[x_axis_price]
    current_price.columns = ['Valuation price', 'Current price']

    # Display
    st.write("""## Selected ticker detailed view""")
    
    detailed_view_col1, detailed_view_col2, detailed_view_col3, detailed_view_col4 = st.columns(4)

    with detailed_view_col1:
        st.metric(
            label="Company name", 
            value=selected_peers['shortName'].values[0]
        )
    with detailed_view_col2:
        st.metric(
            label="Difference with current price", 
            value=metrics_value_formatting(selected_peers['PeersRelativeDiffCluster'].values[0], value_type="percentage", percentage_format=""), 
        )    
    with detailed_view_col3: 
        st.metric(
            label="Cluster", 
            value=selected_peers['cluster'].values[0]
        )
    with detailed_view_col4:
        st.metric(
            label="Confidence valuation", 
            value=selected_peers['PeersConfidenceCluster'].values[0]
        )

    graph_multiples, compare_graph = st.columns(2)

    with graph_multiples:
        # Plot graph with stock prices
        fig = px.bar(
            x=current_multiples.columns, y=current_multiples.values[0], 
            title="Stock price per valuation method", labels={"x" : "", "y" : ""}
        )
        # Add a horizontal line with the mean
        fig.add_shape(
            type="line", line_color="red", line_width=3, opacity=1, line_dash="dot",
            x0=0, x1=1, xref="paper", y0=current_price['Valuation price'].values[0], y1=current_price['Valuation price'].values[0], yref="y"
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

    multiple_compare = st.container()

    with multiple_compare:
        
        x_axis_multiples = ["priceToBook", "enterpriseToRevenue", "enterpriseToEbitda", "trailingPE"]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=x_axis_multiples,
            y=selected_peers[x_axis_multiples].values[0],
            name='Company multiples',
        ))
        fig.add_trace(go.Bar(
            x=x_axis_multiples,
            y=selected_cluster_multiples.values[0],
            name='Cluster multiples',
            # marker_color='indianred'
        ))
        fig.update_layout(title_text='Compare company multiples against cluster')

        st.plotly_chart(fig, use_container_width=True)
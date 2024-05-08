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
        FROM clustering_peers_valuation p
        LEFT JOIN general_information g ON p.symbol = g.symbol;
    """

    tickers_list = con.query(query)
    return tickers_list

@st.cache_data
def get_peers():
    query = """
        WITH last_peers 
        AS 
        (
        SELECT p.*
        FROM (
            SELECT
                "symbol",
                MAX("date") AS last_date
            FROM clustering_peers_valuation
            GROUP BY "symbol"
            ) l
        LEFT JOIN clustering_peers_valuation AS p ON p."symbol" = l."symbol" AND p."date" = l."last_date"
        )
        SELECT
            last_peers.*, 
            g."shortName",
            s."close" AS "lastPrice",
            v."priceToBook",
            v."enterpriseToRevenue",
            v."enterpriseToEbitda",
            v."trailingPE"
        FROM last_peers
        LEFT JOIN general_information AS g ON last_peers."symbol" = g."symbol"
        LEFT JOIN last_stock_prices AS s ON last_peers."symbol" = s."symbol" AND last_peers."date" = s."date"
        LEFT JOIN last_valuations AS v ON last_peers."symbol" = v."symbol";
    """
    
    query_result = con.query(query)
    peers_valuation = pd.DataFrame(query_result)
    return peers_valuation

@st.cache_data
def get_cluster_multiples():
    query = """
        SELECT *
        FROM mean_cluster_multiples;
    """

    query_result = con.query(query)
    cluster_multiples = pd.DataFrame(query_result)
    return cluster_multiples

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
peers = get_peers()

# Define containers
header = st.container()

with header:
       
    st.write("""
    # Clustering Peers
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
        
        with st.expander("Show all peers"):
            st.dataframe(
                data=peers[[
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
        top_10 = peers[["shortName", "PeersRelativeDiffCluster"]].dropna(subset="PeersRelativeDiffCluster").sort_values(by=["PeersRelativeDiffCluster"], ascending=False).head(10)

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
        bottom_10 = peers[["shortName", "PeersRelativeDiffCluster"]].dropna(subset="PeersRelativeDiffCluster").sort_values(by=["PeersRelativeDiffCluster"], ascending=True).head(10)

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
    # Load data
    cluster_multiples = get_cluster_multiples()
    cluster_multiples = cluster_multiples.sort_values(by="cluster", ascending=False)

    # Section title
    st.write("""## Mean cluster multiples""")

    price_to_book_col, enterprise_to_revenue_col, enterprise_to_ebitda_col, trailing_PE_col = st.columns([1, 1, 1, 1])

    with price_to_book_col:
        fig = create_bar_chart(cluster_multiples, "MeanClusterPriceToBook", "Price to Book")                
        st.plotly_chart(fig, use_container_width=True)
    with enterprise_to_revenue_col:
        fig = create_bar_chart(cluster_multiples, "MeanClusterEnterpriseToRevenue", "Enterprise to Revenue")                
        st.plotly_chart(fig, use_container_width=True)
    with enterprise_to_ebitda_col:
        fig = create_bar_chart(cluster_multiples, "MeanClusterEnterpriseToEbitda", "Enterprise to Ebitda")                
        st.plotly_chart(fig, use_container_width=True)
    with trailing_PE_col:
        fig = create_bar_chart(cluster_multiples, "MeanClusterTrailingPE", "Trailing PE")                
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------------------------------
# Individual ticker view

with tab3:
    # Load datas from current selection
    selected_peers = peers[peers['symbol'] == ticker_selection]
    cluster_multiples = get_cluster_multiples()
    selected_cluster_multiples = cluster_multiples[cluster_multiples["cluster"] == selected_peers['cluster'].values[0]].drop("cluster", axis=1)
    
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
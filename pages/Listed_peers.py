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
            FROM peers_valuation
            GROUP BY "symbol"
            ) l
        LEFT JOIN peers_valuation AS p ON p."symbol" = l."symbol" AND p."date" = l."last_date"
        )
        SELECT
            last_peers.*, 
            g."shortName",
            g."sector",
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
def get_sector_multiples():
    query = """
        SELECT *
        FROM mean_sector_multiples;
    """

    query_result = con.query(query)
    sector_multiples = pd.DataFrame(query_result)
    return sector_multiples

def create_bar_chart(sector_multiples, multiple, title):
    """Function to plot horizontal bar chart with mean sector multiples"""

    fig = px.bar(
        data_frame=sector_multiples,
        x=multiple, 
        y="sector", 
        title=title, 
        labels={multiple : "", "sector" : ""},
        orientation="h"
    )

    return fig

# -----------------------------Define sidebar -------------------------------------------

# Section selection
section_selection = st.sidebar.selectbox('Section selection', ["Global view", "Sector multiples", "Individual ticker view"])

# Ticker selection    
tickers_names = get_tickers_names()
company_names = tickers_names["shortName"]
if section_selection == "Individual ticker view":
    company_selection = st.sidebar.selectbox('Company selection', company_names)
    ticker_selection = tickers_names[tickers_names["shortName"] == company_selection]["symbol"].values[0]

# -----------------------------Dashboard ------------------------------------------------

# Load datas
peers = get_peers()

# Define containers
header = st.container()
global_view = st.container()
multiples_view = st.container()
detailed_view = st.container()

with header:
    
    if section_selection == "Global view":
        header_text = """
        # Listed Peers
        Difference between stock prices and the price estimated with a traditional peers valuation approach by sector.
        Below are the main steps for this approach:
        - We compute multiples for each company (revenue, ebitda, price to book, price earnings)
        - We compute mean multiples per sector by aggregating previous results
        - We compute the theoritical valuation for each company by applying mean sector multiples to company financial items
        - We compute the difference between the current price and the theoritical valuation 
        """
    else:
         header_text = """
        # Listed Peers
        """
    
    st.write(header_text)

if section_selection == "Global view":
    with global_view:
        st.write("""## Global view""")
        all_peers_section = st.container()
        top_10_section = st.container()
        bottom_10_section = st.container()

        with all_peers_section:
            # show_peers = st.checkbox("Show all peers")

            # if show_peers:
            with st.expander("Show all peers"):
                st.dataframe(
                    data=peers[[
                        'shortName',
                        'sector',
                        'lastPrice',
                        'date', 
                        'PeersMeanStockPrice', 
                        'PeersRelativeStdStockPrice', 
                        'PeersAbsoluteDiff', 
                        'PeersRelativeDiff', 
                        'PeersConfidence'
                    ]]
                )

        with top_10_section:
            top_10 = peers[["shortName", "PeersRelativeDiff"]].dropna(subset="PeersRelativeDiff").sort_values(by=["PeersRelativeDiff"], ascending=False).head(10)

            # Plot chart with top 10 undervalued stocks
            fig = px.bar(
                data_frame=top_10,
                x="shortName", 
                y="PeersRelativeDiff", 
                title="Top 10 undervalued stocks",
                labels={"shortName" : "", "PeersRelativeDiff" : "Percentage (+)under/(-)over valuation"}
            )

            st.plotly_chart(fig, use_container_width=True)

        with bottom_10_section:
            bottom_10 = peers[["shortName", "PeersRelativeDiff"]].dropna(subset="PeersRelativeDiff").sort_values(by=["PeersRelativeDiff"], ascending=True).head(10)

            # Plot chart with top 10 undervalued stocks
            fig = px.bar(
                data_frame=bottom_10,
                x="shortName", 
                y="PeersRelativeDiff", 
                title="Top 10 overvalued stocks",
                labels={"shortName" : "", "PeersRelativeDiff" : "Percentage (+)under/(-)over valuation"}
            )

            st.plotly_chart(fig, use_container_width=True)

elif section_selection == "Sector multiples":
    with multiples_view:
        # Load data
        sector_multiples = get_sector_multiples()
        sector_multiples = sector_multiples.sort_values(by="sector", ascending=False)

        # Section title
        st.write("""## Mean sector multiples""")

        price_to_book_col, enterprise_to_revenue_col, enterprise_to_ebitda_col, trailing_PE_col = st.columns([1, 1, 1, 1])

        with price_to_book_col:
            fig = create_bar_chart(sector_multiples, "MeanSectorPriceToBook", "Price to Book")                
            st.plotly_chart(fig, use_container_width=True)
        with enterprise_to_revenue_col:
            fig = create_bar_chart(sector_multiples, "MeanSectorEnterpriseToRevenue", "Enterprise to Revenue")                
            st.plotly_chart(fig, use_container_width=True)
        with enterprise_to_ebitda_col:
            fig = create_bar_chart(sector_multiples, "MeanSectorEnterpriseToEbitda", "Enterprise to Ebitda")                
            st.plotly_chart(fig, use_container_width=True)
        with trailing_PE_col:
            fig = create_bar_chart(sector_multiples, "MeanSectorTrailingPE", "Trailing PE")                
            st.plotly_chart(fig, use_container_width=True)

elif section_selection == "Individual ticker view":
    with detailed_view:
        # Load datas from current selection
        selected_peers = peers[peers['symbol'] == ticker_selection]
        sector_multiples = get_sector_multiples()
        selected_sector_multiples = sector_multiples[sector_multiples["sector"] == selected_peers['sector'].values[0]].drop("sector", axis=1)
        
        x_axis_peers = ['stockPriceBook', 'stockPriceRevenue', 'stockPriceEbitda', 'stockPriceEarnings']
        current_multiples = selected_peers[x_axis_peers]

        x_axis_price = ['PeersMeanStockPrice', 'lastPrice']
        current_price = selected_peers[x_axis_price]
        current_price.columns = ['Valuation price', 'Current price']

        # Display
        st.write("""## Selected ticker detailed view""")
        
        detailed_view_col1, detailed_view_col2 = st.columns(2)

        with detailed_view_col1:
            st.write("Company name: {}".format(selected_peers['shortName'].values[0]))
        with detailed_view_col2:
            st.write("Relative difference with current price: {:.2%}".format(selected_peers['PeersRelativeDiff'].values[0]))
        
        detailed_view_col3, detailed_view_col4 = st.columns(2)
        
        with detailed_view_col3: 
            st.write("Sector: {}".format(selected_peers['sector'].values[0]))
        with detailed_view_col4:
            st.write("Confidence valuation: {}".format(selected_peers['PeersConfidence'].values[0]))

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
                y=selected_sector_multiples.values[0],
                name='Sector multiples',
                # marker_color='indianred'
            ))
            fig.update_layout(title_text='Compare company multiples against sector')

            st.plotly_chart(fig, use_container_width=True)
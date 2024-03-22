import logging
import sys
import tempfile
import time
import pendulum
import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import ExternalPythonOperator, PythonOperator, PythonVirtualenvOperator, is_venv_installed
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd
import numpy as np
import datetime as dt
import pytz

from yahooquery import Ticker
# from symbols import CAC_40

# log = logging.getLogger(__name__)
# PATH_TO_PYTHON_BINARY = sys.executable
# BASE_DIR = tempfile.gettempdir()

def extract_data_single(query_result, selected_items, query_time=None):
    """
    Extracts items from a query with yahooquery library. 
    Returns a list with one dict per ticker in the query containing query_time and selected_items datas 
       
    Arguments
    ----------
    query_result: dict
        Result of a query with yahooquery library with a method applied (ex: yahooquery.Ticker(ticker).financial_data)
    selected_items: list of str
        Names of items to extract (ex: ['regularMarketPrice', 'regularMarketDayHigh'])
    query_time: timestamp or None
        Time at which the query has been performed, adds a column with this information for each row in the query
    """
    
    tickers = list(query_result.keys())   
    results_list = []
    
    # For each ticker extract selected items
    for ticker in tickers:
        
        # Get query result for the current ticker
        query_result_ticker = query_result[ticker]
        
        # Instantiante result with time and ticker
        if query_time:
            ticker_result = {'symbol': ticker, 'date': query_time}
        else:
            ticker_result = {'symbol': ticker}
        
        # Collect name of available items for the ticker (yahooquery doesn't return the same items depending on the ticker)
        if isinstance(query_result_ticker, str): # If the query doesn't find any results it resturns a string
            available_items = []
        
        else: # Else get names of items returned
            available_items = query_result_ticker.keys()
        
        # Now extract items if available
        for item in selected_items:
        
            # Check if data is available, and append items
            if item in available_items:
                ticker_result[item] = query_result_ticker[item]

            # If not available, fill with NaN
            else:
                ticker_result[item] = np.NaN
              
        # Append results for the current ticker to the final list          
        results_list.append(ticker_result)
    
    return results_list

def extract_data_multiple(query_result_list, selected_items_list, query_time=None):
    """   
    Extracts items from a query with yahooquery library. 
    Returns a list with one dict per ticker in the query containing query_time and selected_items_list datas 
       
    Arguments
    ----------
    query_result: list containing dicts
        Result of a queries with yahooquery library with a method applied (ex: yahooquery.Ticker(ticker).financial_data)
    selected_items: list containing list of str
        Names of items to extract (ex: [['priceHint', 'previousClose', 'open'], ['regularMarketChangePercent', 'regularMarketChange']])
    query_time: timestamp
        Time at which the query has been performed
    """
       
    # Extract datas
    i = 0
    
    for query_result, selected_items in zip(query_result_list, selected_items_list):
        
        extract = pd.DataFrame(extract_data_single(query_result, selected_items, query_time))
        
        # If it is the first loop we need to create a DataFrame with the extract
        if i == 0:
            combined_extract = extract.copy()
            i += 1
        
        # Else we merge the new extract with the existing DataFrame from first loop
        else:
            if query_time:
                combined_extract = pd.merge(combined_extract, extract, on=['symbol', 'date'])
            else:
                combined_extract = pd.merge(combined_extract, extract, on=['symbol'])
    
    return combined_extract

default_args = {
    'start_date': dt.datetime.now(),
    'schedule_interval': dt.timedelta(minutes=5),
}

@dag(
    dag_id="update_db_monthly",
    default_args=default_args,
    catchup=False,
)
def update_db_monthly():
  
    create_general_information_table = PostgresOperator(
        task_id="create_general_information_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS general_information (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "sector" TEXT,
                "industry" TEXT,
                "country" TEXT,
                "fullTimeEmployees" FLOAT,
                "regularMarketSource" TEXT,
                "exchange" TEXT,
                "exchangeName" TEXT,
                "exchangeDataDelayedBy" SMALLINT,
                "marketState" TEXT,
                "quoteType" TEXT,
                "currency" TEXT,
                "shortName" TEXT
            );""",
    )

    create_temp_info_table = PostgresOperator(
        task_id="create_temp_info_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS temp_info;
            CREATE TABLE temp_info (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "sector" TEXT,
                "industry" TEXT,
                "country" TEXT,
                "fullTimeEmployees" FLOAT,
                "regularMarketSource" TEXT,
                "exchange" TEXT,
                "exchangeName" TEXT,
                "exchangeDataDelayedBy" SMALLINT,
                "marketState" TEXT,
                "quoteType" TEXT,
                "currency" TEXT,
                "shortName" TEXT
            );""",
    )

    @task(task_id="download_data")
    def download_data():

        # Create Ticker instance with symbols list
        ticker = "TTE.PA"
        ticker2 = "AI.PA"
        ticker3 = "FR.PA"
        list_tickers = [ticker, ticker2, ticker3]
        tickers = Ticker(list_tickers)

        # Request API and create a DataFrame
        timezone = pytz.timezone('CET')
        query_time = dt.datetime.now(tz=timezone).replace(microsecond=0)
        
        query_result_1 = tickers.asset_profile
        selected_items_1 = ['sector', 'industry', 'country', 'fullTimeEmployees']

        query_result_2 = tickers.price
        selected_items_2 = ['regularMarketSource', 'exchange', 'exchangeName', 'exchangeDataDelayedBy', 'marketState', 'quoteType', 'currency', 'shortName']

        query_result_list = [query_result_1, query_result_2]
        selected_items_list = [selected_items_1, selected_items_2]

        general_information = extract_data_multiple(query_result_list, selected_items_list, query_time=query_time)

        # Cast fullTimeEmployees in INT, impossible since NaN is incompatbale with INT type
        # general_information['fullTimeEmployees'] = general_information['fullTimeEmployees'].apply(lambda x : int(x) if not np.isnan(x) else np.NaN)

        # Save results in csv file
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        file_path = os.path.join(files_dir_path, "general_information.csv")
        general_information.to_csv(file_path, index=False)
    
    @task(task_id="update_db")
    def update_db():       

        # Create a connexion to the database
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        # Copy the files previously saved in tables
        prices_file_path = "/opt/airflow/dags/files/general_information.csv"
        with open(prices_file_path, "r") as file:
            cur.copy_expert(
                "COPY temp_info FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the table stock_price_minute
        query_1 = """
            INSERT INTO general_information
            SELECT *
            FROM temp_info;
        """

        query_2 = """DROP TABLE IF EXISTS temp_info;"""

        try:
            cur.execute(query_1)
            cur.execute(query_2)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_general_information_table, create_temp_info_table] >> download_data() >> update_db()

dag = update_db_monthly()
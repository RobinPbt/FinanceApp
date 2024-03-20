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

default_args = {
    'start_date': dt.datetime.now(),
    'schedule_interval': dt.timedelta(minutes=5),
}

@dag(
    dag_id="update_db_daily",
    default_args=default_args,
    catchup=False,
)
def update_db_daily():
  
    create_stock_price_daily_table = PostgresOperator(
        task_id="create_stock_price_daily_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS stock_price_daily (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "open" FLOAT,
                "high" FLOAT,
                "low" FLOAT,
                "close" FLOAT,
                "volume" INT,
                "adjclose" FLOAT,
                "dividends" FLOAT
            );""",
    )

    create_temp_prices_table = PostgresOperator(
        task_id="create_temp_prices_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS temp_prices;
            CREATE TABLE temp_prices (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "open" FLOAT,
                "high" FLOAT,
                "low" FLOAT,
                "close" FLOAT,
                "volume" INT,
                "adjclose" FLOAT,
                "dividends" FLOAT
            );""",
    )

    create_estimates_daily_table = PostgresOperator(
        task_id="create_estimates_daily_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS estimates_daily (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "targetHighPrice" FLOAT,
                "targetLowPrice" FLOAT,
                "targetMeanPrice" FLOAT,
                "targetMedianPrice" FLOAT,
                "recommendationMean" FLOAT,
                "recommendationKey" TEXT,
                "numberOfAnalystOpinions" SMALLINT
            );""",
    )

    create_last_estimates_table = PostgresOperator(
        task_id="create_last_estimates_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS last_estimates;
            CREATE TABLE last_estimates (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "targetHighPrice" FLOAT,
                "targetLowPrice" FLOAT,
                "targetMeanPrice" FLOAT,
                "targetMedianPrice" FLOAT,
                "recommendationMean" FLOAT,
                "recommendationKey" TEXT,
                "numberOfAnalystOpinions" SMALLINT
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

        # Request to get last available prices
        last_prices = tickers.history(period='1d', interval='1d').reset_index()

        # Request API with current time to get last estimates
        query_result = tickers.financial_data
        timezone = pytz.timezone('CET')
        query_time = dt.datetime.now(tz=timezone).replace(microsecond=0)
        selected_items = ['targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice', 'recommendationMean', 'recommendationKey', 'numberOfAnalystOpinions']
        last_estimates = pd.DataFrame(extract_data_single(query_result, selected_items, query_time=query_time))

        # Save results in csv file
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        prices_file_path = os.path.join(files_dir_path, "daily_prices.csv")
        last_prices.to_csv(prices_file_path, index=False)

        estimates_file_path = os.path.join(files_dir_path, "daily_estimates.csv")
        last_estimates.to_csv(estimates_file_path, index=False)
    
    @task(task_id="update_db")
    def update_db():       

        # Copy the files previously saved in tables
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        prices_file_path = "/opt/airflow/dags/files/daily_prices.csv"
        with open(prices_file_path, "r") as file:
            cur.copy_expert(
                "COPY temp_prices FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        estimates_file_path = "/opt/airflow/dags/files/daily_estimates.csv"
        with open(estimates_file_path, "r") as file:
            cur.copy_expert(
                "COPY last_estimates FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the table stock_price_minute
        query_prices = """
            INSERT INTO stock_price_daily
            SELECT *
            FROM temp_prices
        """

        query_estimates = """
            INSERT INTO estimates_daily
            SELECT *
            FROM last_estimates
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query_prices)
            cur.execute(query_estimates)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_stock_price_daily_table, create_estimates_daily_table, create_last_estimates_table] >> download_data() >> update_db()

dag = update_db_daily()
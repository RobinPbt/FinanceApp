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
from functions import *

# log = logging.getLogger(__name__)
# PATH_TO_PYTHON_BINARY = sys.executable
# BASE_DIR = tempfile.gettempdir()

@dag(
    dag_id="update_db_daily",
    schedule_interval="0 16 * * 1-5", # Once a day (18h) after market closing on market opening days (monday to friday)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def update_db_daily():
  
    # Create stock prices tables
    
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
                "volume" FLOAT,
                "adjclose" FLOAT,
                "dividends" FLOAT,
                PRIMARY KEY ("symbol", "date")
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
                "volume" FLOAT,
                "adjclose" FLOAT,
                "dividends" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    # Create estimates tables

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
                "numberOfAnalystOpinions" FLOAT,
                PRIMARY KEY ("symbol", "date")
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
                "numberOfAnalystOpinions" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    # Create valuation tables

    create_valuations_daily_table = PostgresOperator(
        task_id="create_valuations_daily_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS valuations_daily (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "enterpriseValue" FLOAT,
                "forwardPE" FLOAT,
                "bookValue" FLOAT,
                "priceToBook" FLOAT,
                "enterpriseToRevenue" FLOAT,
                "enterpriseToEbitda" FLOAT,
                "beta" FLOAT,
                "pegRatio" FLOAT,
                "trailingPE" FLOAT,
                "marketCap" FLOAT,
                "priceToSalesTrailing12Months" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    create_last_valuations_table = PostgresOperator(
        task_id="create_last_valuations_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS last_valuations;
            CREATE TABLE last_valuations (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "enterpriseValue" FLOAT,
                "forwardPE" FLOAT,
                "bookValue" FLOAT,
                "priceToBook" FLOAT,
                "enterpriseToRevenue" FLOAT,
                "enterpriseToEbitda" FLOAT,
                "beta" FLOAT,
                "pegRatio" FLOAT,
                "trailingPE" FLOAT,
                "marketCap" FLOAT,
                "priceToSalesTrailing12Months" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    @task(task_id="download_data")
    def download_data():

        # Create Ticker instance with symbols list
        files_dir_path = "/opt/airflow/dags/files/symbol_list.csv"
        symbol_df = pd.read_csv(files_dir_path)
        symbol_list = list(symbol_df['symbol'])
        tickers = Ticker(symbol_list)

        # Get current time (CET timezone)
        timezone = pytz.timezone('CET')
        query_time = dt.datetime.now(tz=timezone).replace(microsecond=0)

        # Request to get last available prices. If no values for dividends add 0 
        last_prices = tickers.history(period='1d', interval='1d').reset_index()
        if 'dividends' not in last_prices.columns:
            last_prices['dividends'] = 0

        # Request API to get last estimates
        query_result = tickers.financial_data
        selected_items = ['targetHighPrice', 'targetLowPrice', 'targetMeanPrice', 'targetMedianPrice', 'recommendationMean', 'recommendationKey', 'numberOfAnalystOpinions']
        last_estimates = extract_data_single(query_result, selected_items, query_time=query_time)

        
        # Request API to get last valuations
        query_result_1 = tickers.key_stats
        selected_items_1 = ['enterpriseValue', 'forwardPE', 'bookValue', 'priceToBook', 'enterpriseToRevenue', 'enterpriseToEbitda', 'beta', 'pegRatio']

        query_result_2 = tickers.summary_detail
        selected_items_2 = ['trailingPE', 'marketCap', 'priceToSalesTrailing12Months']

        query_result_list = [query_result_1, query_result_2]
        selected_items_list = [selected_items_1, selected_items_2]

        valuations = extract_data_multiple(query_result_list, selected_items_list, query_time=query_time)

        # Save results in csv files
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        prices_file_path = os.path.join(files_dir_path, "daily_prices.csv")
        last_prices.to_csv(prices_file_path, index=False)

        estimates_file_path = os.path.join(files_dir_path, "daily_estimates.csv")
        last_estimates.to_csv(estimates_file_path, index=False)

        valuations_file_path = os.path.join(files_dir_path, "valuations.csv")
        valuations.to_csv(valuations_file_path, index=False)
    
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

        valuations_file_path = "/opt/airflow/dags/files/valuations.csv"
        with open(valuations_file_path, "r") as file:
            cur.copy_expert(
                "COPY last_valuations FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the table permanent table
        query_prices = """
            INSERT INTO stock_price_daily
            SELECT *
            FROM temp_prices
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS temp_prices;
        """

        query_estimates = """
            INSERT INTO estimates_daily
            SELECT *
            FROM last_estimates
            ON CONFLICT("symbol", "date")
            DO NOTHING;
        """

        query_valuations = """
            INSERT INTO valuations_daily
            SELECT *
            FROM last_valuations
            ON CONFLICT("symbol", "date")
            DO NOTHING;
        """

        cur.execute(query_prices)
        cur.execute(query_estimates)
        cur.execute(query_valuations)
        conn.commit()

    [
        create_stock_price_daily_table, 
        create_temp_prices_table, 
        create_estimates_daily_table, 
        create_last_estimates_table, 
        create_valuations_daily_table, 
        create_last_valuations_table
    ] >> download_data() >> update_db()

dag = update_db_daily()
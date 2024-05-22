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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    dag_id="instantiate_new_symbols",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def instantiate_new_symbols():
  
    @task.branch(task_id="branch_task")
    def branch_func(previous):
        
        if previous == 0:
            return None
        else:
            return [
                "create_new_prices_table",
                "create_new_valuations_table",
                "download_historical_prices", 
                "update_historical_prices",
                "trigger_update_weekly",
                "wait_for_financials_update",
                "compute_valuations",
                "update_historical_valuations",
                "trigger_update_month",
                "trigger_update_daily",
                "trigger_update_minute",  
            ]
      
    @task(task_id="check_new")
    def check_new():

        # Load the symbol list
        files_dir_path = "/opt/airflow/dags/files/symbol_list.csv"
        symbol_df = pd.read_csv(files_dir_path)
        new_symbol_list = list(symbol_df['symbol'])

        # Get symbols currently in the database
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        query = """
            SELECT DISTINCT(symbol) 
            FROM stock_price_daily;
        """

        cur.execute(query)
        query_result = cur.fetchall()
        current_symbol_list = [i[0] for i in query_result]

        # If there is no new symbol we can stop the DAG with the branch logic
        if set(new_symbol_list) == set(current_symbol_list):
            return 0
        else:
            return list(set(new_symbol_list) - set(current_symbol_list))
          
    # Create stock prices table
    create_new_prices_table = PostgresOperator(
        task_id="create_new_prices_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS new_prices;
            CREATE TABLE new_prices (
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

    # Create valuation table
    create_new_valuations_table = PostgresOperator(
        task_id="create_new_valuations_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS new_valuations;
            CREATE TABLE IF NOT EXISTS new_valuations (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "MarketCap" FLOAT,
                "EnterpriseValue" FLOAT,
                "EnterpriseValueRevenueMultiple" FLOAT,
                "EnterpriseValueEBITDAMultiple" FLOAT,
                "PriceToBookRatio" FLOAT,
                "PriceEarningsRatio" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    @task(task_id="download_historical_prices")
    def download_historical_prices(new_symbols):

        # Create Ticker instance with symbols list
        tickers = Ticker(new_symbols)

        # Request to get all daily history prices for new symbols. If no values for dividends add 0 
        prices = tickers.history(period='max', interval='1d').reset_index()
        if 'dividends' not in prices.columns:
            prices['dividends'] = 0

        # Select columns
        prices = prices[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjclose', 'dividends']]

        # Save results in csv files
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        prices_file_path = os.path.join(files_dir_path, "history_prices.csv")
        prices.to_csv(prices_file_path, index=False)
    
    @task(task_id="update_historical_prices")
    def update_historical_prices():       

        # Copy the file previously saved in table
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        prices_file_path = "/opt/airflow/dags/files/history_prices.csv"
        with open(prices_file_path, "r") as file:
            cur.copy_expert(
                "COPY new_prices FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the permanent table
        query_prices = """
            INSERT INTO stock_price_daily
            SELECT *
            FROM new_prices
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS new_prices;
        """

        cur.execute(query_prices)
        conn.commit()

    trigger_update_weekly = TriggerDagRunOperator(
        task_id="trigger_update_weekly",
        trigger_dag_id="update_db_weekly",
    )

    wait_for_financials_update = ExternalTaskSensor(
        task_id='wait_for_financials_update',
        external_dag_id='update_db_weekly',
        external_task_id='update_db',
        timeout=600,
        allowed_states=["success"]
    )

    @task(task_id="compute_valuations")
    def compute_valuations(new_symbols):

        # Query financials 
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        query = """
            SELECT *
            FROM historical_financials;
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        historical_financials = pd.DataFrame(query_result, columns=colnames)
        historical_financials = historical_financials[historical_financials['symbol'].isin(new_symbols)]
        historical_financials['date'] = historical_financials['date'].apply(lambda x: to_datetime(x))

        # Query prices
        query = """
            SELECT *
            FROM stock_price_daily;
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        stock_prices = pd.DataFrame(query_result, columns=colnames)
        stock_prices = stock_prices[stock_prices['symbol'].isin(new_symbols)]
        stock_prices = stock_prices[['symbol', 'date', 'adjclose']]

        # Compute valuations based on last available prices
        stock_prices['date'] = stock_prices['date'].apply(lambda x: x.date()) # Convert timestamp to date
        history_valuations = stock_prices.apply(lambda x: daily_valuation(x, historical_financials), axis=1) # Apply function to compute valuation based on last stock price and financials
        history_valuations.drop('adjclose', axis=1, inplace=True) # Drop data already in other tables
        history_valuations.dropna(axis=0, thresh=6, inplace=True) # Drop rows with NaN on each values except symbol and date (i.e. 6 values)

        # Save results in csv files
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        history_valuations_file_path = os.path.join(files_dir_path, "history_valuations.csv")
        history_valuations.to_csv(history_valuations_file_path, index=False)

    @task(task_id="update_historical_valuations")
    def update_historical_valuations():       

        # Copy the file previously saved in table
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        history_valuations_file_path = "/opt/airflow/dags/files/history_valuations.csv"
        with open(history_valuations_file_path, "r") as file:
            cur.copy_expert(
                "COPY new_valuations FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the permanent table
        query_valuations = """
            INSERT INTO valuations_daily
            SELECT *
            FROM new_valuations
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS new_valuations;
        """

        cur.execute(query_valuations)
        conn.commit()

    trigger_update_month = TriggerDagRunOperator(
        task_id="trigger_update_month",
        trigger_dag_id="update_db_monthly",
    )

    trigger_update_daily = TriggerDagRunOperator(
        task_id="trigger_update_daily",
        trigger_dag_id="update_db_daily",
    )

    trigger_update_minute = TriggerDagRunOperator(
        task_id="trigger_update_minute",
        trigger_dag_id="update_db_minute",
    )

    previous = check_new()
    branch_op = branch_func(previous)
    
    branch_op >> [create_new_prices_table, create_new_valuations_table] >> \
    download_historical_prices(previous) >> update_historical_prices() >> \
    trigger_update_weekly >> wait_for_financials_update >> compute_valuations(previous) >> \
    update_historical_valuations() >> trigger_update_month >> trigger_update_daily >> trigger_update_minute

dag = instantiate_new_symbols()
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
                "download_data", 
                "update_db", 
                "trigger_update_minute", 
                "trigger_update_daily", 
                "trigger_update_weekly", 
                "trigger_update_month"
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
            FROM stock_price_minute;
        """

        cur.execute(query)
        query_result = cur.fetchall()
        current_symbol_list = [i[0] for i in query_result]

        # If there is no new symbol we can stop the DAG with the branch logic
        if set(new_symbol_list) == set(current_symbol_list):
            return 0
        else:
            return list(set(new_symbol_list) - set(current_symbol_list))
          
    # Create stock prices tables
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
                "dividends" FLOAT
            );""",
    )

    @task(task_id="download_data")
    def download_data(new_symbols):

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
    
    @task(task_id="update_db")
    def update_db():       

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
            FROM new_prices;
            DROP TABLE IF EXISTS new_prices;
        """

        cur.execute(query_prices)
        conn.commit()

    trigger_update_minute = TriggerDagRunOperator(
        task_id="trigger_update_minute",
        trigger_dag_id="update_db_minute",
    )

    trigger_update_daily = TriggerDagRunOperator(
        task_id="trigger_update_daily",
        trigger_dag_id="update_db_daily",
    )

    trigger_update_weekly = TriggerDagRunOperator(
        task_id="trigger_update_weekly",
        trigger_dag_id="update_db_weekly",
    )

    trigger_update_month = TriggerDagRunOperator(
        task_id="trigger_update_month",
        trigger_dag_id="update_db_monthly",
    )

    previous = check_new()
    branch_op = branch_func(previous)
    branch_op >> create_new_prices_table >> download_data(previous) >> update_db() >> [trigger_update_minute, trigger_update_daily, trigger_update_weekly, trigger_update_month]

dag = instantiate_new_symbols()
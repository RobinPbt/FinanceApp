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

from yahooquery import Ticker
# from symbols import CAC_40

# log = logging.getLogger(__name__)
# PATH_TO_PYTHON_BINARY = sys.executable
# BASE_DIR = tempfile.gettempdir()

@dag(
    dag_id="update_db_minute",
    schedule_interval="*/5 8-18 * * 1-5", # Run on french market opening hours (monday to friday 9-17 + 1 hour extra before and after)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def update_db_minute():
  
    create_stock_price_minute_table = PostgresOperator(
        task_id="create_stock_price_minute_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS stock_price_minute (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "close" FLOAT
            );""",
    )

    create_last_stock_prices_table = PostgresOperator(
        task_id="create_last_stock_prices_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS last_stock_prices;
            CREATE TABLE last_stock_prices (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "close" FLOAT
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
        last_prices = last_prices[['symbol', 'date', 'close']]


        # Save results in csv file
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        file_path = os.path.join(files_dir_path, "prices.csv")
        
        last_prices.to_csv(file_path, index=False)
    
    @task(task_id="update_db")
    def update_db():       

        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        # Copy the result of the request in the table last_stock_prices
        file_path = "/opt/airflow/dags/files/prices.csv"
        with open(file_path, "r") as file:
            cur.copy_expert(
                "COPY last_stock_prices FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

        # Insert the result of the request in the table stock_price_minute
        query = """
            INSERT INTO stock_price_minute
            SELECT *
            FROM last_stock_prices
        """

        try:
            postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_stock_price_minute_table, create_last_stock_prices_table] >> download_data() >> update_db()

dag = update_db_minute()
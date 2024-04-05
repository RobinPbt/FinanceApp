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

from functions import *

# log = logging.getLogger(__name__)
# PATH_TO_PYTHON_BINARY = sys.executable
# BASE_DIR = tempfile.gettempdir()

@dag(
    dag_id="update_db_weekly",
    schedule_interval="0 19 * * 5", # Once a week at 19h on friday
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def update_db_weekly():
  
    # Create financials tables
    
    create_financials_weekly_table = PostgresOperator(
        task_id="create_financials_weekly_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS financials_weekly (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "totalCash" BIGINT,
                "totalCashPerShare" FLOAT,
                "totalDebt" BIGINT,
                "quickRatio" FLOAT,
                "currentRatio" FLOAT,
                "debtToEquity" FLOAT,
                "totalRevenue" BIGINT,
                "revenuePerShare" FLOAT,
                "revenueGrowth" FLOAT,
                "grossProfits" BIGINT,
                "grossMargins" FLOAT,
                "operatingMargins" FLOAT,
                "ebitda" BIGINT,
                "ebitdaMargins" FLOAT,
                "earningsGrowth" FLOAT,
                "profitMargins" FLOAT,
                "freeCashflow" BIGINT,
                "operatingCashflow" BIGINT,
                "returnOnAssets" FLOAT,
                "returnOnEquity" FLOAT
            );""",
    )

    create_last_financials_table = PostgresOperator(
        task_id="create_last_financials_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS last_financials;
            CREATE TABLE last_financials (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "totalCash" BIGINT,
                "totalCashPerShare" FLOAT,
                "totalDebt" BIGINT,
                "quickRatio" FLOAT,
                "currentRatio" FLOAT,
                "debtToEquity" FLOAT,
                "totalRevenue" BIGINT,
                "revenuePerShare" FLOAT,
                "revenueGrowth" FLOAT,
                "grossProfits" BIGINT,
                "grossMargins" FLOAT,
                "operatingMargins" FLOAT,
                "ebitda" BIGINT,
                "ebitdaMargins" FLOAT,
                "earningsGrowth" FLOAT,
                "profitMargins" FLOAT,
                "freeCashflow" BIGINT,
                "operatingCashflow" BIGINT,
                "returnOnAssets" FLOAT,
                "returnOnEquity" FLOAT
            );""",
    )

    # Create dividends tables

    create_dividends_weekly_table = PostgresOperator(
        task_id="create_dividends_weekly_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS dividends_weekly (
                "symbol" TEXT,
                "exDividendDate" TIMESTAMP,
                "dividendRate" FLOAT,
                "dividendYield" FLOAT,
                "payoutRatio" FLOAT,
                "fiveYearAvgDividendYield" FLOAT,
                "trailingAnnualDividendRate" FLOAT,
                "trailingAnnualDividendYield" FLOAT,
                "lastDividendValue" FLOAT,
                "lastDividendDate" TIMESTAMP,
                PRIMARY KEY ("symbol", "exDividendDate")
            );""",
    )

    create_temp_dividends_table = PostgresOperator(
        task_id="create_temp_dividends_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS temp_dividends;
            CREATE TABLE temp_dividends (
                "symbol" TEXT,
                "exDividendDate" TIMESTAMP,
                "dividendRate" FLOAT,
                "dividendYield" FLOAT,
                "payoutRatio" FLOAT,
                "fiveYearAvgDividendYield" FLOAT,
                "trailingAnnualDividendRate" FLOAT,
                "trailingAnnualDividendYield" FLOAT,
                "lastDividendValue" FLOAT,
                "lastDividendDate" TIMESTAMP,
                PRIMARY KEY ("symbol", "exDividendDate")
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

        # Get current time (CET timezone)
        timezone = pytz.timezone('CET')
        query_time = dt.datetime.now(tz=timezone).replace(microsecond=0)

        # Request API to get last estimates
        query_result = tickers.financial_data

        selected_items = [
            # Balance sheet items
            'totalCash',
            'totalCashPerShare',
            'totalDebt',
            'quickRatio', 
            'currentRatio', 
            'debtToEquity',
            # P&L items
            'totalRevenue',
            'revenuePerShare', 
            'revenueGrowth', 
            'grossProfits', 
            'grossMargins', 
            'operatingMargins', 
            'ebitda', 
            'ebitdaMargins', 
            'earningsGrowth', 
            'profitMargins',
            # Cash flow items
            'freeCashflow',
            'operatingCashflow',
            # Performance ratios
            'returnOnAssets', 
            'returnOnEquity'
        ]

        query_result_list = [query_result]
        selected_items_list = [selected_items]

        financials = extract_data_multiple(query_result_list, selected_items_list, query_time=query_time)

        # Request API to get last valuations
        query_result_1 = tickers.summary_detail
        selected_items_1 = [
            'exDividendDate',
            'dividendRate', 
            'dividendYield', 
            'payoutRatio', 
            'fiveYearAvgDividendYield', 
            'trailingAnnualDividendRate', 
            'trailingAnnualDividendYield'
        ]

        query_result_2 = tickers.key_stats
        selected_items_2 = ['lastDividendValue', 'lastDividendDate']

        query_result_list = [query_result_1, query_result_2]
        selected_items_list = [selected_items_1, selected_items_2]

        dividends = extract_data_multiple(query_result_list, selected_items_list, query_time=None)
        dividends['lastDividendDate'] = dividends['lastDividendDate'].apply(lambda x: pd.to_datetime(x))

        # Save results in csv files
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        financials_file_path = os.path.join(files_dir_path, "financials.csv")
        financials.to_csv(financials_file_path, index=False)
        
        dividends_file_path = os.path.join(files_dir_path, "dividends.csv")
        dividends.to_csv(dividends_file_path, index=False)
    
    @task(task_id="update_db")
    def update_db():       

        # Copy the files previously saved in tables
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        financials_file_path = "/opt/airflow/dags/files/financials.csv"
        with open(financials_file_path, "r") as file:
            cur.copy_expert(
                "COPY last_financials FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        dividends_file_path = "/opt/airflow/dags/files/dividends.csv"
        with open(dividends_file_path, "r") as file:
            cur.copy_expert(
                "COPY temp_dividends FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the table permanent table
        query_financials = """
            INSERT INTO financials_weekly
            SELECT *
            FROM last_financials;
        """

        query_dividends = """
            INSERT INTO dividends_weekly
            SELECT *
            FROM temp_dividends
            ON CONFLICT("symbol", "exDividendDate")
            DO NOTHING;
            DROP TABLE IF EXISTS temp_dividends;
        """

        cur.execute(query_financials)
        cur.execute(query_dividends)
        conn.commit()

    [
        create_financials_weekly_table, 
        create_last_financials_table, 
        create_dividends_weekly_table, 
        create_temp_dividends_table, 
    ] >> download_data() >> update_db()

dag = update_db_weekly()
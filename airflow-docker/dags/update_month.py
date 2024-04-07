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
    dag_id="update_db_monthly",
    schedule_interval="0 0 1 * *", # Once a month (midnight fisrt day of month)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def update_db_monthly():
  
    # General information tables
    
    create_general_information_table = PostgresOperator(
        task_id="create_general_information_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS general_information (
                "symbol" TEXT PRIMARY KEY,
                "sector" TEXT,
                "industry" TEXT,
                "country" TEXT,
                "fullTimeEmployees" FLOAT,
                "regularMarketSource" TEXT,
                "exchange" TEXT,
                "exchangeName" TEXT,
                "exchangeDataDelayedBy" FLOAT,
                "marketState" TEXT,
                "quoteType" TEXT,
                "currency" TEXT,
                "shortName" TEXT
            );""",
    )

    create_temp_general_info_table = PostgresOperator(
        task_id="create_temp_general_info_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS temp_general_info;
            CREATE TABLE temp_general_info (
                "symbol" TEXT PRIMARY KEY,
                "sector" TEXT,
                "industry" TEXT,
                "country" TEXT,
                "fullTimeEmployees" FLOAT,
                "regularMarketSource" TEXT,
                "exchange" TEXT,
                "exchangeName" TEXT,
                "exchangeDataDelayedBy" FLOAT,
                "marketState" TEXT,
                "quoteType" TEXT,
                "currency" TEXT,
                "shortName" TEXT
            );""",
    )

    # Stock information tables

    create_stock_information_table = PostgresOperator(
        task_id="create_stock_information_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS stock_information (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "floatShares" FLOAT,
                "sharesOutstanding" FLOAT,
                "heldPercentInsiders" FLOAT,
                "heldPercentInstitutions" FLOAT,
                "lastSplitFactor" TEXT,
                "lastSplitDate" TIMESTAMP,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    create_temp_stock_info_table = PostgresOperator(
        task_id="create_temp_stock_info_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS temp_stock_info;
            CREATE TABLE temp_stock_info (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "floatShares" FLOAT,
                "sharesOutstanding" FLOAT,
                "heldPercentInsiders" FLOAT,
                "heldPercentInstitutions" FLOAT,
                "lastSplitFactor" TEXT,
                "lastSplitDate" TIMESTAMP,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    # Ratings tables

    create_ratings_table = PostgresOperator(
        task_id="create_ratings_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS ratings (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "auditRisk" FLOAT,
                "boardRisk" FLOAT,
                "compensationRisk" FLOAT,
                "shareHolderRightsRisk" FLOAT,
                "overallRisk" FLOAT,
                "totalEsg" FLOAT,
                "environmentScore" FLOAT,
                "socialScore" FLOAT,
                "governanceScore" FLOAT,
                "ratingYear" FLOAT,
                "ratingMonth" FLOAT,
                "highestControversy" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    create_temp_ratings_table = PostgresOperator(
        task_id="create_temp_ratings_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS temp_ratings;
            CREATE TABLE temp_ratings (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "auditRisk" FLOAT,
                "boardRisk" FLOAT,
                "compensationRisk" FLOAT,
                "shareHolderRightsRisk" FLOAT,
                "overallRisk" FLOAT,
                "totalEsg" FLOAT,
                "environmentScore" FLOAT,
                "socialScore" FLOAT,
                "governanceScore" FLOAT,
                "ratingYear" FLOAT,
                "ratingMonth" FLOAT,
                "highestControversy" FLOAT,
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

        # Request API to get general informations        
        query_result_1 = tickers.asset_profile
        selected_items_1 = ['sector', 'industry', 'country', 'fullTimeEmployees']

        query_result_2 = tickers.price
        selected_items_2 = ['regularMarketSource', 'exchange', 'exchangeName', 'exchangeDataDelayedBy', 'marketState', 'quoteType', 'currency', 'shortName']

        query_result_list = [query_result_1, query_result_2]
        selected_items_list = [selected_items_1, selected_items_2]

        general_information = extract_data_multiple(query_result_list, selected_items_list, query_time=None)

        # Cast fullTimeEmployees in INT, impossible since NaN is incompatbale with INT type
        # general_information['fullTimeEmployees'] = general_information['fullTimeEmployees'].apply(lambda x : int(x) if not np.isnan(x) else np.NaN)

        # Request API to get stock informations
        query_result = tickers.key_stats
        selected_items = ['floatShares', 'sharesOutstanding', 'heldPercentInsiders', 'heldPercentInstitutions', 'lastSplitFactor', 'lastSplitDate']
        stock_information = extract_data_single(query_result, selected_items, query_time=query_time)

        # Request API to get ratings
        query_result_1 = tickers.asset_profile
        selected_items_1 = ['auditRisk', 'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 'overallRisk']

        query_result_2 = tickers.esg_scores
        selected_items_2 = ['totalEsg', 'environmentScore', 'socialScore', 'governanceScore', 'ratingYear', 'ratingMonth', 'highestControversy']

        query_result_list = [query_result_1, query_result_2]
        selected_items_list = [selected_items_1, selected_items_2]

        ratings = extract_data_multiple(query_result_list, selected_items_list, query_time=query_time)

        # Save results in csv files
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        general_file_path = os.path.join(files_dir_path, "general_information.csv")
        general_information.to_csv(general_file_path, index=False)

        stock_file_path = os.path.join(files_dir_path, "stock_information.csv")
        stock_information.to_csv(stock_file_path, index=False)

        ratings_file_path = os.path.join(files_dir_path, "ratings.csv")
        ratings.to_csv(ratings_file_path, index=False)
    
    @task(task_id="update_db")
    def update_db():       

        # Create a connexion to the database
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        # Copy the files previously saved in tables
        general_file_path = "/opt/airflow/dags/files/general_information.csv"
        with open(general_file_path, "r") as file:
            cur.copy_expert(
                "COPY temp_general_info FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )

        stock_file_path = "/opt/airflow/dags/files/stock_information.csv"
        with open(stock_file_path, "r") as file:
            cur.copy_expert(
                "COPY temp_stock_info FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )

        ratings_file_path = "/opt/airflow/dags/files/ratings.csv"
        with open(ratings_file_path, "r") as file:
            cur.copy_expert(
                "COPY temp_ratings FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the table permanent table
        query_general_information= """
            INSERT INTO general_information
            SELECT *
            FROM temp_general_info
            ON CONFLICT(symbol)
            DO UPDATE SET
                "sector" = EXCLUDED."sector",
                "industry" = EXCLUDED."industry",
                "country" = EXCLUDED."country",
                "fullTimeEmployees" = EXCLUDED."fullTimeEmployees",
                "regularMarketSource" = EXCLUDED."regularMarketSource",
                "exchange" = EXCLUDED."exchange",
                "exchangeName" = EXCLUDED."exchangeName",
                "exchangeDataDelayedBy" = EXCLUDED."exchangeDataDelayedBy",
                "marketState" = EXCLUDED."marketState",
                "quoteType" = EXCLUDED."quoteType",
                "currency" = EXCLUDED."currency",
                "shortName" = EXCLUDED."shortName";
                DROP TABLE IF EXISTS temp_general_info;
        """

        query_stock_information = """
            INSERT INTO stock_information
            SELECT *
            FROM temp_stock_info
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS temp_stock_info;
        """

        query_ratings = """
            INSERT INTO ratings
            SELECT *
            FROM temp_ratings
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS temp_ratings;
        """

        cur.execute(query_general_information)
        cur.execute(query_stock_information)
        cur.execute(query_ratings)
        conn.commit()

    [create_general_information_table, create_temp_general_info_table, create_stock_information_table, create_temp_stock_info_table, create_ratings_table, create_temp_ratings_table] >> download_data() >> update_db()

dag = update_db_monthly()
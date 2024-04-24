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
    dag_id="synthetize_metrics",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="CET"),
    catchup=False,
)
def synthetize_metrics():

    # Create synthesis tables
    create_synthesis_table = PostgresOperator(
        task_id="create_synthesis_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS synthesis (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "GlobalMeanStockPrice" FLOAT,
                "GlobalAbsoluteDiff" FLOAT,
                "GlobalRelativeDiff" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    create_synthesis_temp_table = PostgresOperator(
        task_id="create_synthesis_temp_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS synthesis_temp;
            CREATE TABLE synthesis_temp (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "GlobalMeanStockPrice" FLOAT,
                "GlobalAbsoluteDiff" FLOAT,
                "GlobalRelativeDiff" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    @task(task_id="download_data")
    def download_data():

        # Connect to the database
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Query valuation from each method and compute the mean and differences with current stock price
        query = """
            WITH last_peers_valuation
            AS
            (
            SELECT 
                p."symbol",
                p."PeersMeanStockPrice"
            FROM (
                SELECT
                    "symbol",
                    MAX("date") AS last_date
                FROM peers_valuation
                GROUP BY "symbol"
                ) la
            LEFT JOIN peers_valuation AS p ON p."symbol" = la."symbol" AND p."date" = la."last_date"
            )
            SELECT
                p."symbol",
                p."date",
                ((ef."targetMedianPrice" + pv."PeersMeanStockPrice") / 2) AS "GlobalMeanStockPrice",
                (((ef."targetMedianPrice" + pv."PeersMeanStockPrice") / 2) - p."close") AS "GlobalAbsoluteDiff",
                ((((ef."targetMedianPrice" + pv."PeersMeanStockPrice") / 2) - p."close") / p."close") AS "GlobalRelativeDiff"
            FROM last_stock_prices AS p
            LEFT JOIN last_estimates ef ON p."symbol" = ef."symbol"
            LEFT JOIN last_peers_valuation pv ON p."symbol" = pv."symbol";
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        synthesis = pd.DataFrame(query_result, columns=colnames)
       
        # Save results in csv files
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        synthesis_path = os.path.join(files_dir_path, "synthesis.csv")
        synthesis.to_csv(synthesis_path, index=False)
    
    @task(task_id="update_db")
    def update_db():       

        # Copy the file previously saved in table
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        synthesis_path = "/opt/airflow/dags/files/synthesis.csv"
        with open(synthesis_path, "r") as file:
            cur.copy_expert(
                "COPY synthesis_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the permanent table
        query = """
            INSERT INTO synthesis
            SELECT *
            FROM synthesis_temp
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS synthesis_temp;
        """

        cur.execute(query)
        conn.commit()    

    [create_synthesis_table, create_synthesis_temp_table] >> download_data() >> update_db()

dag = synthetize_metrics()
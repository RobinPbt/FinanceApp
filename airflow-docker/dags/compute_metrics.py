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
    dag_id="compute_metrics",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="CET"),
    catchup=False,
)
def compute_metrics():
                
    # Create estimates diff tables
    create_estimates_diff_table = PostgresOperator(
        task_id="create_estimates_diff_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS estimates_diff (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "EstimatesAbsoluteDiff" FLOAT,
                "EstimatesRelativeDiff" FLOAT,
                "EstimatesConfidence" TEXT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    create_estimates_diff_temp_table = PostgresOperator(
        task_id="create_estimates_diff_temp_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS estimates_diff_temp;
            CREATE TABLE estimates_diff_temp (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "EstimatesAbsoluteDiff" FLOAT,
                "EstimatesRelativeDiff" FLOAT,
                "EstimatesConfidence" TEXT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    # Create mean multiples table
    create_mean_sector_multiples_table = PostgresOperator(
        task_id="create_mean_sector_multiples_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS mean_sector_multiples;
            CREATE TABLE mean_sector_multiples (
                "sector" TEXT PRIMARY KEY,
                "MeanSectorPriceToBook" FLOAT,
                "MeanSectorEnterpriseToRevenue" FLOAT,
                "MeanSectorEnterpriseToEbitda" FLOAT,
                "MeanSectorTrailingPE" FLOAT
            );""",
    )

    # Create peers tables
    create_peers_valuation_table = PostgresOperator(
        task_id="create_peers_valuation_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS peers_valuation (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "BridgeEnterpriseValueMarketCap" FLOAT,
                "marketCapRevenue" FLOAT,
                "marketCapEbitda" FLOAT,
                "marketCapEarnings" FLOAT,
                "marketCapBook" FLOAT,
                "stockPriceRevenue" FLOAT,
                "stockPriceEbitda" FLOAT,
                "stockPriceEarnings" FLOAT,
                "stockPriceBook" FLOAT,
                "PeersMeanStockPrice" FLOAT,
                "PeersRelativeStdStockPrice" FLOAT,
                "PeersAbsoluteDiff" FLOAT,
                "PeersRelativeDiff" FLOAT,
                "PeersConfidence" TEXT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    create_peers_valuation_temp_table = PostgresOperator(
        task_id="create_peers_valuation_temp_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS peers_valuation_temp;
            CREATE TABLE peers_valuation_temp (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "BridgeEnterpriseValueMarketCap" FLOAT,
                "marketCapRevenue" FLOAT,
                "marketCapEbitda" FLOAT,
                "marketCapEarnings" FLOAT,
                "marketCapBook" FLOAT,
                "stockPriceRevenue" FLOAT,
                "stockPriceEbitda" FLOAT,
                "stockPriceEarnings" FLOAT,
                "stockPriceBook" FLOAT,
                "PeersMeanStockPrice" FLOAT,
                "PeersRelativeStdStockPrice" FLOAT,
                "PeersAbsoluteDiff" FLOAT,
                "PeersRelativeDiff" FLOAT,
                "PeersConfidence" TEXT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    @task(task_id="download_data")
    def download_data():

        # Connect to the database
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Query estimates and compute differential with last stock price
        query = """
            SELECT
                p."symbol",
                p."date",
                e."numberOfAnalystOpinions",
                (e."targetMedianPrice" - p."close") AS "EstimatesAbsoluteDiff",
                ((e."targetMedianPrice" - p."close") / p."close") AS "EstimatesRelativeDiff"
            FROM last_stock_prices AS p
            LEFT JOIN last_estimates e ON p."symbol" = e."symbol";
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        estimates_diff = pd.DataFrame(query_result, columns=colnames)

        # Query necessary tables for peers valuation
        query = """
            SELECT
                g."symbol", 
                g."shortName",
                g."sector",
                g."industry",
                v."priceToBook",
                v."enterpriseToRevenue",
                v."enterpriseToEbitda",
                v."trailingPE"
            FROM general_information AS g
            LEFT JOIN last_valuations v ON g."symbol" = v."symbol";
        """
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        multiples = pd.DataFrame(query_result, columns=colnames)

        query = """
            WITH last_stock_info 
            AS 
            (
            SELECT
                s."symbol",
                s."date",
                s."sharesOutstanding"
            FROM (
                SELECT
                    "symbol",
                    MAX("date") AS last_date
                FROM stock_information
                GROUP BY "symbol"
                ) l
            LEFT JOIN stock_information AS s ON s."symbol" = l."symbol" AND s."date" = l."last_date"
            )
            SELECT
                v."symbol", 
                v."bookValue",
                v."enterpriseValue",
                (v."enterpriseValue" - v."marketCap") AS "BridgeEnterpriseValueMarketCap",
                v."marketCap",
                last_stock_info."sharesOutstanding",
                (v."marketCap" / last_stock_info."sharesOutstanding") AS "stock_price"
            FROM last_valuations AS v
            LEFT JOIN last_stock_info ON last_stock_info."symbol" = v."symbol";
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        last_valuations = pd.DataFrame(query_result, columns=colnames)

        query = """
            SELECT
                "symbol", 
                "totalRevenue",
                "ebitda",
                ("totalRevenue" * "profitMargins") AS "earnings"
            FROM last_financials
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        financials = pd.DataFrame(query_result, columns=colnames)

        query = """
        SELECT
            "symbol",
            "close" AS "lastPrice",
            "date"
        FROM last_stock_prices;
        """
        
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        stock_price = pd.DataFrame(query_result, columns=colnames)

        # Transform data
        estimates_diff['EstimatesConfidence'] = estimates_diff['numberOfAnalystOpinions'].apply(lambda x: target_confidence_estimates(x))
        estimates_diff = estimates_diff[['symbol', 'date', 'EstimatesAbsoluteDiff', 'EstimatesRelativeDiff', 'EstimatesConfidence']]
        mean_sector_multiples, peers = peers_valuation(financials, multiples, last_valuations, stock_price)
        
        # Save results in csv files
        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        estimates_diff_file_path = os.path.join(files_dir_path, "estimates_diff.csv")
        estimates_diff.to_csv(estimates_diff_file_path, index=False)

        mean_sector_multiples_file_path = os.path.join(files_dir_path, "mean_sector_multiples.csv")
        mean_sector_multiples.to_csv(mean_sector_multiples_file_path, index=True)

        peers_file_path = os.path.join(files_dir_path, "peers_valuation.csv")
        peers.to_csv(peers_file_path, index=False)
    
    @task(task_id="update_db")
    def update_db():       

        # Copy the file previously saved in table
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        estimates_diff_file_path = "/opt/airflow/dags/files/estimates_diff.csv"
        with open(estimates_diff_file_path, "r") as file:
            cur.copy_expert(
                "COPY estimates_diff_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        mean_sector_multiples_file_path = "/opt/airflow/dags/files/mean_sector_multiples.csv"
        with open(mean_sector_multiples_file_path, "r") as file:
            cur.copy_expert(
                "COPY mean_sector_multiples FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )

        peers_file_path = "/opt/airflow/dags/files/peers_valuation.csv"
        with open(peers_file_path, "r") as file:
            cur.copy_expert(
                "COPY peers_valuation_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        conn.commit()

        # Insert the result of the request in the permanent table
        query_prices = """
            INSERT INTO estimates_diff
            SELECT *
            FROM estimates_diff_temp
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS estimates_diff_temp;
        """

        query_peers = """
            INSERT INTO peers_valuation
            SELECT *
            FROM peers_valuation_temp
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS peers_valuation_temp;
        """

        cur.execute(query_prices)
        cur.execute(query_peers)
        conn.commit()

    [
        create_estimates_diff_table, 
        create_estimates_diff_temp_table,
        create_mean_sector_multiples_table,
        create_peers_valuation_table,
        create_peers_valuation_temp_table
    ] >> download_data() >> update_db()

dag = compute_metrics()
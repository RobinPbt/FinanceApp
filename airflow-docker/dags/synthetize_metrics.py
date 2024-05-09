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

        # Query necessary tables
        # query = """
        #     WITH last_sector_peers_valuation
        #     AS
        #     (
        #     SELECT 
        #         sp."symbol",
        #         sp."PeersMeanStockPriceSector"
        #     FROM (
        #         SELECT
        #             "symbol",
        #             MAX("date") AS last_date
        #         FROM sector_peers_valuation
        #         GROUP BY "symbol"
        #         ) lsp
        #     LEFT JOIN sector_peers_valuation AS sp ON sp."symbol" = lsp."symbol" AND sp."date" = lsp."last_date"
        #     ),
        #     last_clustering_peers_valuation
        #     AS
        #     (
        #     SELECT 
        #         cp."symbol",
        #         cp."PeersMeanStockPriceCluster"
        #     FROM (
        #         SELECT
        #             "symbol",
        #             MAX("date") AS last_date
        #         FROM clustering_peers_valuation
        #         GROUP BY "symbol"
        #         ) lcp
        #     LEFT JOIN clustering_peers_valuation AS cp ON cp."symbol" = lcp."symbol" AND cp."date" = lcp."last_date"
        #     ),
        #     last_regression
        #     AS
        #     (
        #     SELECT 
        #         r."symbol",
        #         r."RegressionPrediction"
        #     FROM (
        #         SELECT
        #             "symbol",
        #             MAX("date") AS last_date
        #         FROM regression_ML
        #         GROUP BY "symbol"
        #         ) lr
        #     LEFT JOIN regression_ML AS r ON r."symbol" = lr."symbol" AND r."date" = lr."last_date"
        #     )
        #     SELECT
        #         p."symbol",
        #         p."date",
        #         ((ef."targetMedianPrice" + spv."PeersMeanStockPriceSector" + cpv."PeersMeanStockPriceCluster" + re."RegressionPrediction") / 4) AS "GlobalMeanStockPrice",
        #         (((ef."targetMedianPrice" + spv."PeersMeanStockPriceSector" + cpv."PeersMeanStockPriceCluster" + re."RegressionPrediction") / 4) - p."close") AS "GlobalAbsoluteDiff",
        #         ((((ef."targetMedianPrice" + spv."PeersMeanStockPriceSector" + cpv."PeersMeanStockPriceCluster" + re."RegressionPrediction") / 4) - p."close") / p."close") AS "GlobalRelativeDiff"
        #     FROM last_stock_prices AS p
        #     LEFT JOIN last_estimates ef ON p."symbol" = ef."symbol"
        #     LEFT JOIN last_sector_peers_valuation spv ON p."symbol" = spv."symbol"
        #     LEFT JOIN last_clustering_peers_valuation cpv ON p."symbol" = cpv."symbol"
        #     LEFT JOIN last_regression re ON p."symbol" = re."symbol";
        # """

        # cur.execute(query)
        # colnames = [desc[0] for desc in cur.description]
        # query_result = cur.fetchall()
        # last_sector_peers_valuation = pd.DataFrame(query_result, columns=colnames)

        query = """
            SELECT 
                sp."symbol",
                sp."PeersMeanStockPriceSector"
            FROM (
                SELECT
                    "symbol",
                    MAX("date") AS last_date
                FROM sector_peers_valuation
                GROUP BY "symbol"
                ) lsp
            LEFT JOIN sector_peers_valuation AS sp ON sp."symbol" = lsp."symbol" AND sp."date" = lsp."last_date"
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        last_sector_peers_valuation = pd.DataFrame(query_result, columns=colnames)

        query = """
            SELECT 
                cp."symbol",
                cp."PeersMeanStockPriceCluster"
            FROM (
                SELECT
                    "symbol",
                    MAX("date") AS last_date
                FROM clustering_peers_valuation
                GROUP BY "symbol"
                ) lcp
            LEFT JOIN clustering_peers_valuation AS cp ON cp."symbol" = lcp."symbol" AND cp."date" = lcp."last_date"
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        last_clustering_peers_valuation = pd.DataFrame(query_result, columns=colnames)

        query = """
            SELECT 
                r."symbol",
                r."RegressionPrediction"
            FROM (
                SELECT
                    "symbol",
                    MAX("date") AS last_date
                FROM regression_ML
                GROUP BY "symbol"
                ) lr
            LEFT JOIN regression_ML AS r ON r."symbol" = lr."symbol" AND r."date" = lr."last_date"
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        last_regression = pd.DataFrame(query_result, columns=colnames)

        query = """
            SELECT 
                "symbol",
                "date",
                "close"
            FROM last_stock_prices
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        last_stock_prices = pd.DataFrame(query_result, columns=colnames)

        query = """
            SELECT
                "symbol",
                "targetMedianPrice"
            FROM last_estimates
        """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        last_estimates = pd.DataFrame(query_result, columns=colnames)

        # Transform data
        synthesis = pd.DataFrame(data=last_stock_prices)
        synthesis = pd.merge(synthesis, last_estimates, on="symbol", how="left")
        synthesis = pd.merge(synthesis, last_regression, on="symbol", how="left")
        synthesis = pd.merge(synthesis, last_clustering_peers_valuation, on="symbol", how="left")
        synthesis = pd.merge(synthesis, last_sector_peers_valuation, on="symbol", how="left")
        synthesis["GlobalMeanStockPrice"] = synthesis.apply(lambda x: np.nanmean([x["targetMedianPrice"], x["PeersMeanStockPriceSector"], x["PeersMeanStockPriceCluster"], x["RegressionPrediction"]]), axis=1)
        synthesis["GlobalAbsoluteDiff"] = synthesis["GlobalMeanStockPrice"] - synthesis["close"]
        synthesis["GlobalRelativeDiff"] = synthesis["GlobalAbsoluteDiff"] / synthesis["close"]
        synthesis = synthesis[["symbol", "date", "GlobalMeanStockPrice", "GlobalAbsoluteDiff", "GlobalRelativeDiff"]]
       
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
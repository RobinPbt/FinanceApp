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
import pickle

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

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
                
    # ---------------------------------------------------------------------------------------------------
    # Create estimates diff tables

    # Permanent table
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

    # Temporary table
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

    # ---------------------------------------------------------------------------------------------------
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

    # ---------------------------------------------------------------------------------------------------
    # Create peers tables

    # Permanent table
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

    # Temporary table
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

    # ---------------------------------------------------------------------------------------------------
    # Create ML regression tables

    # Permanent table
    create_regression_ML_table = PostgresOperator(
        task_id="create_regression_ML_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS regression_ML (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "RegressionPrediction" FLOAT,
                "RegressionAbsoluteDiff" FLOAT,
                "RegressionRelativeDiff" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    # Temporary table
    create_regression_ML_temp_table = PostgresOperator(
        task_id="create_regression_ML_temp_table",
        postgres_conn_id="finapp_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS regression_ML_temp;
            CREATE TABLE regression_ML_temp (
                "symbol" TEXT,
                "date" TIMESTAMP,
                "RegressionPrediction" FLOAT,
                "RegressionAbsoluteDiff" FLOAT,
                "RegressionRelativeDiff" FLOAT,
                PRIMARY KEY ("symbol", "date")
            );""",
    )

    @task(task_id="download_data")
    def download_data():

        # ---------------------------------------------------------------------------------------------------
        # QUERIES
        # ---------------------------------------------------------------------------------------------------
        
        # ---------------------------------------------------------------------------------------------------
        # Connect to the database
        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # ---------------------------------------------------------------------------------------------------
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

        # ---------------------------------------------------------------------------------------------------
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

        # ---------------------------------------------------------------------------------------------------
        # Query ML features
        query = """
            WITH last_stock_info 
            AS 
            (
            SELECT si.*
            FROM (
                SELECT
                    "symbol",
                    MAX("date") AS last_date
                FROM stock_information
                GROUP BY "symbol"
                ) lsi
            LEFT JOIN stock_information AS si ON si."symbol" = lsi."symbol" AND si."date" = lsi."last_date"
            ),
            last_ratings
            AS
            (
            SELECT ra.*
            FROM (
                SELECT
                    "symbol",
                    MAX("date") AS last_date
                FROM ratings
                GROUP BY "symbol"
                ) lra
            LEFT JOIN ratings AS ra ON ra."symbol" = lra."symbol" AND ra."date" = lra."last_date"
            )
            SELECT 
                gi."symbol",
                gi."sector",
                gi."industry",
                gi."fullTimeEmployees",
                gi."regularMarketSource",
                gi."exchange",
                gi."quoteType",
                gi."currency",
                last_ratings."auditRisk",
                last_ratings."boardRisk",
                last_ratings."compensationRisk",
                last_ratings."shareHolderRightsRisk",
                last_ratings."overallRisk",
                last_ratings."totalEsg",
                last_ratings."environmentScore",
                last_ratings."socialScore",
                last_ratings."governanceScore",
                last_ratings."highestControversy",
                last_stock_info."floatShares",
                last_stock_info."sharesOutstanding",
                last_stock_info."heldPercentInsiders",
                last_stock_info."heldPercentInstitutions",  
                last_financials."totalCash",
                last_financials."totalCashPerShare",
                last_financials."totalDebt",
                last_financials."quickRatio",
                last_financials."currentRatio",
                last_financials."debtToEquity",
                last_financials."totalRevenue",
                last_financials."revenuePerShare",
                last_financials."revenueGrowth",
                last_financials."grossProfits",
                last_financials."grossMargins",
                last_financials."operatingMargins",
                last_financials."ebitda",
                last_financials."ebitdaMargins",
                last_financials."earningsGrowth",
                last_financials."profitMargins",
                last_financials."freeCashflow",
                last_financials."operatingCashflow",
                last_financials."returnOnAssets",
                last_financials."returnOnEquity",
                last_estimates."targetHighPrice",
                last_estimates."targetLowPrice",
                last_estimates."targetMeanPrice",
                last_estimates."targetMedianPrice",
                last_estimates."recommendationMean",
                last_estimates."recommendationKey",
                last_estimates."numberOfAnalystOpinions",
                last_stock_prices."close",
                last_stock_prices."date"
            FROM general_information AS gi
            LEFT JOIN last_ratings ON gi."symbol" = last_ratings.symbol
            LEFT JOIN last_stock_info ON gi."symbol" = last_stock_info.symbol
            LEFT JOIN last_financials ON gi."symbol" = last_financials.symbol
            LEFT JOIN last_estimates ON gi."symbol" = last_estimates.symbol
            LEFT JOIN last_stock_prices ON gi."symbol" = last_stock_prices.symbol
            """

        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        query_result = cur.fetchall()
        ML_features = pd.DataFrame(query_result, columns=colnames)

        files_dir_path = "/opt/airflow/dags/files/"

        if not os.path.exists(os.path.exists(files_dir_path)):
            os.mkdir(files_dir_path)

        # ---------------------------------------------------------------------------------------------------
        # Load ML models

        preprocessor_file_path = os.path.join(files_dir_path, "test_preprocessor.pkl")
        model_file_path = os.path.join(files_dir_path, "test_model.pkl")
        preprocessor = pickle.load(open(preprocessor_file_path, 'rb'))
        model = pickle.load(open(model_file_path, 'rb'))

        # ---------------------------------------------------------------------------------------------------
        # TRANSFORM
        # ---------------------------------------------------------------------------------------------------
        
        # ---------------------------------------------------------------------------------------------------
        # Estimates
        estimates_diff['EstimatesConfidence'] = estimates_diff['numberOfAnalystOpinions'].apply(lambda x: target_confidence_estimates(x))
        estimates_diff = estimates_diff[['symbol', 'date', 'EstimatesAbsoluteDiff', 'EstimatesRelativeDiff', 'EstimatesConfidence']]
        
        # ---------------------------------------------------------------------------------------------------
        # Peers
        mean_sector_multiples, peers = peers_valuation(financials, multiples, last_valuations, stock_price)

        # ---------------------------------------------------------------------------------------------------
        # Regression ML
        
        # Preprocess features
        symbols = ML_features['symbol']
        current_price = ML_features['close']
        dates = ML_features['date']
        X = ML_features.drop(['symbol', 'close', 'date'], axis=1)
        X_prep = preprocessor.transform(X)

        # Predict
        predictions = model.predict(X_prep)

        # Compute differences
        regression_ML = pd.DataFrame(data=current_price)
        regression_ML['RegressionPrediction'] = predictions
        regression_ML['date'] = dates
        regression_ML['RegressionAbsoluteDiff'] = regression_ML['RegressionPrediction'] - regression_ML['close']
        regression_ML['RegressionRelativeDiff'] = regression_ML['RegressionAbsoluteDiff'] / regression_ML['close']
        regression_ML['symbol'] = symbols
        regression_ML = regression_ML[['symbol', 'date', 'RegressionPrediction', 'RegressionAbsoluteDiff', 'RegressionRelativeDiff']]
        
        # ---------------------------------------------------------------------------------------------------
        # SAVE CSV FILES
        # ---------------------------------------------------------------------------------------------------

        estimates_diff_file_path = os.path.join(files_dir_path, "estimates_diff.csv")
        estimates_diff.to_csv(estimates_diff_file_path, index=False)

        mean_sector_multiples_file_path = os.path.join(files_dir_path, "mean_sector_multiples.csv")
        mean_sector_multiples.to_csv(mean_sector_multiples_file_path, index=True)

        peers_file_path = os.path.join(files_dir_path, "peers_valuation.csv")
        peers.to_csv(peers_file_path, index=False)

        regression_file_path = os.path.join(files_dir_path, "regression_ML.csv")
        regression_ML.to_csv(regression_file_path, index=False)    
    
    @task(task_id="update_db")
    def update_db():       

        # ---------------------------------------------------------------------------------------------------
        # Copy the file previously saved in a table

        postgres_hook = PostgresHook(postgres_conn_id="finapp_postgres_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # Estimates
        estimates_diff_file_path = "/opt/airflow/dags/files/estimates_diff.csv"
        with open(estimates_diff_file_path, "r") as file:
            cur.copy_expert(
                "COPY estimates_diff_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        # Sector multiples
        mean_sector_multiples_file_path = "/opt/airflow/dags/files/mean_sector_multiples.csv"
        with open(mean_sector_multiples_file_path, "r") as file:
            cur.copy_expert(
                "COPY mean_sector_multiples FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )

        # Peers valuation
        peers_file_path = "/opt/airflow/dags/files/peers_valuation.csv"
        with open(peers_file_path, "r") as file:
            cur.copy_expert(
                "COPY peers_valuation_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        
        # Regression ML
        regression_file_path = "/opt/airflow/dags/files/regression_ML.csv"
        with open(regression_file_path, "r") as file:
            cur.copy_expert(
                "COPY regression_ML_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )

        conn.commit()

        # ---------------------------------------------------------------------------------------------------
        # Insert the result of the request in the permanent table

        # Estimates
        query_estimates = """
            INSERT INTO estimates_diff
            SELECT *
            FROM estimates_diff_temp
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS estimates_diff_temp;
        """

        # Peers
        query_peers = """
            INSERT INTO peers_valuation
            SELECT *
            FROM peers_valuation_temp
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS peers_valuation_temp;
        """

        # Regression
        query_regression = """
            INSERT INTO regression_ML
            SELECT *
            FROM regression_ML_temp
            ON CONFLICT("symbol", "date")
            DO NOTHING;
            DROP TABLE IF EXISTS regression_ML_temp;
        """

        cur.execute(query_estimates)
        cur.execute(query_peers)
        cur.execute(query_regression)
        conn.commit()

    trigger_synthetize_metrics = TriggerDagRunOperator(
        task_id="trigger_synthetize_metrics",
        trigger_dag_id="synthetize_metrics",
    )

    [
        create_estimates_diff_table, 
        create_estimates_diff_temp_table,
        create_mean_sector_multiples_table,
        create_peers_valuation_table,
        create_peers_valuation_temp_table
    ] >> download_data() >> update_db() >> trigger_synthetize_metrics

dag = compute_metrics()
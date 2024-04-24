# Pending project: Finance Application
Please note that this project is for educational purpose only and should not be used for an actual investment strategy

## Overview
This project aims at creating a financial application for a fundamental investment strategy. The target structure should include 2 modules:
- Valuation: multi-criteria approach to assess if a company is under/over-valued (brokers consensus, listed peers, DCF, Machine Learning algorithm)
- Portfolio optimization: given the results provided by the valuation module, selection of bests stocks to build an optimal investment portfolio

## ETL
The first step of the project is the creation of an ETL pipeline to get data from Yahoo Finance, you will find below the current structure of the pipeline (development phase running locally)

![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/1944ac80-525b-4171-976b-9697442438d3)


### Airflow DAGs

4 DAGs to update data with various frequencies
- update_minute: 
  - Request the stock prices every 5 minutes and store it in the data warehouse
  - Trigger the DAG compute_metrics for further transformations
  - Stock prices with 5 minutes frequencies to get a recent stock price for all computations, to display intra-day prices evolution and could be later used in a time-series ML algorithm
- update_daily:
  - Request daily stock prices (end of day), estimates and valuation items and store it in the data warehouse
  - Daily stock-price could be later used in a time-series ML algorithm
  - Estimates are used to compare current price with estimates
  - Valuation items are used to perform a peers valuation and compare it to the current price. It will also be used as the target value in a ML algorithm trained to predict the price in a time horizon to be defined (which will be considered the "intrinsic value")
- update_weekly:
  - Request companies financials and dividends information and store it in the data warehouse
  - Financials are used to perform the peers valuation (by applying relevant aggregates to multiples) and will be used as features for the ML regression algorithm
  - Dividends could be later used to correct the effect of dividends on stock prices and as a feature for the ML regression algorithm
- update_monthly:
  - Request companies general information, ratings, and stock information and store it in the data warehouse
  - General information are used to perform peers valuation (to identify sector / industry), to provide the user some information about the company, and will also be used as features for the ML algorithm
  - Ratings will be used as features for the ML algorithm

1 DAG to apply data transformation
 - Triggered every 5 minutes by update_minute
 - Use the estimates to compute the difference between median estimates and current price
 - Use valuation items to compute mean multiples per sector / industry and apply them to relevant financial aggregates in order to get a peers valuation

1 DAG to instantiate new symbols in the data model
- Triggered manually
- If new symbols are added to the list, get all information about these symbols required to fill all tables in the data warehouse




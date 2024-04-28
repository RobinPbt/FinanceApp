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

![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/da5d405c-3f61-4e0e-837c-894946d9726e)


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

2 DAGs to apply data transformation
- compute_metrics
  - Triggered every 5 minutes by update_minute
  - Use the estimates to compute the difference between median estimates and current price
  - Use valuation items to compute mean multiples per sector / industry and apply them to relevant financial aggregates in order to get a peers valuation
- synthetize_metrics
  - Triggered every 5 minutes by compute_metrics
  - Create a synthetic table of results of all valuation approaches

1 DAG to instantiate new symbols in the data model
- Triggered manually
- If new symbols are added to the list, get all information about these symbols required to fill all tables in the data warehouse

## Current streamlit app screenshots (work in progress)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/db51c1d6-f332-4f7a-8e6e-6803a9fa8111)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/7d409e96-a3f4-48e4-b14e-29a63ea3676a)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/67bcab4e-6bae-4169-ab85-579192a9b335)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/5a3a9ae7-f151-47a1-bd7c-3a1af93ce536)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/76063a38-42ae-4e90-92f4-cd49dec45099)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/f14af4f0-255c-4602-a3f6-8083529aac11)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/301ae116-1a9e-4c27-98e3-ee0f3cfc1600)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/c81131fc-8ff4-47e7-8966-98a3ab26aecf)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/332fbc05-eca2-4997-90cc-c050d7b8c751)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/42a780d8-0732-490b-8a0b-813e1ece10c1)
![image](https://github.com/RobinPbt/FinanceApp/assets/104992181/0dfbd880-7b9f-44bb-9f66-ca9c33e41130)
















from yahooquery import Ticker
import pandas as pd
import numpy as np
import datetime as dt
import pytz

# ---------------------------------------------------------------------------------------------------
# Extraction API calls functions

def extract_data_single(query_result, selected_items, query_time=None):
    """
    Extracts items from a query with yahooquery library. 
    Returns a list with one dict per ticker in the query containing query_time and selected_items datas 
       
    Arguments
    ----------
    query_result: dict
        Result of a query with yahooquery library with a method applied (ex: yahooquery.Ticker(ticker).financial_data)
    selected_items: list of str
        Names of items to extract (ex: ['regularMarketPrice', 'regularMarketDayHigh'])
    query_time: timestamp or None
        Time at which the query has been performed, adds a column with this information for each row in the query
    """
    
    tickers = list(query_result.keys())   
    results_list = []
    
    # For each ticker extract selected items
    for ticker in tickers:
        
        # Get query result for the current ticker
        query_result_ticker = query_result[ticker]
        
        # Instantiante result with time and ticker
        if query_time:
            ticker_result = {'symbol': ticker, 'date': query_time}
        else:
            ticker_result = {'symbol': ticker}
        
        # Collect name of available items for the ticker (yahooquery doesn't return the same items depending on the ticker)
        if isinstance(query_result_ticker, str): # If the query doesn't find any results it resturns a string
            available_items = []
        
        else: # Else get names of items returned
            available_items = query_result_ticker.keys()
        
        # Now extract items if available
        for item in selected_items:
        
            # Check if data is available, and append items
            if item in available_items:
                ticker_result[item] = query_result_ticker[item]

            # If not available, fill with NaN
            else:
                ticker_result[item] = np.NaN
              
        # Append results for the current ticker to the final list          
        results_list.append(ticker_result)

    results_list = pd.DataFrame(results_list)
    
    return results_list

def extract_data_multiple(query_result_list, selected_items_list, query_time=None):
    """   
    Extracts items from a query with yahooquery library. 
    Returns a list with one dict per ticker in the query containing query_time and selected_items_list datas 
       
    Arguments
    ----------
    query_result: list containing dicts
        Result of a queries with yahooquery library with a method applied (ex: yahooquery.Ticker(ticker).financial_data)
    selected_items: list containing list of str
        Names of items to extract (ex: [['priceHint', 'previousClose', 'open'], ['regularMarketChangePercent', 'regularMarketChange']])
    query_time: timestamp
        Time at which the query has been performed
    """
       
    # Extract datas
    i = 0
    
    for query_result, selected_items in zip(query_result_list, selected_items_list):
        
        extract = pd.DataFrame(extract_data_single(query_result, selected_items, query_time))
        
        # If it is the first loop we need to create a DataFrame with the extract
        if i == 0:
            combined_extract = extract.copy()
            i += 1
        
        # Else we merge the new extract with the existing DataFrame from first loop
        else:
            if query_time:
                combined_extract = pd.merge(combined_extract, extract, on=['symbol', 'date'])
            else:
                combined_extract = pd.merge(combined_extract, extract, on=['symbol'])
    
    return combined_extract

# ---------------------------------------------------------------------------------------------------
# Peers valuation functions

def compute_mean_multiples(multiples, groupby_col):
    """Compute mean valuation multiples on a groupping column for a given set of multiples"""

    capitalized_groupby_col = groupby_col.capitalize()

    mean_multiples = multiples.groupby(by=groupby_col)[[
        'PriceToBookRatio', 
        'EnterpriseValueRevenueMultiple', 
        'EnterpriseValueEBITDAMultiple', 
        'PriceEarningsRatio'
    ]].mean()
    
    mean_multiples.columns = [
        f'Mean{capitalized_groupby_col}PriceToBookRatio', 
        f'Mean{capitalized_groupby_col}EnterpriseValueRevenueMultiple', 
        f'Mean{capitalized_groupby_col}EnterpriseValueEBITDAMultiple', 
        f'Mean{capitalized_groupby_col}PriceEarningsRatio'
    ]
    
    return mean_multiples

def revenue_valuation(enterpriseToRevenue, totalRevenue, bridge_enterpriseValue_marketCap):
    """Perform an enterprise valuation through the revenue multiple comparable method. Returns the market capitalization."""

    enterpriseValue = enterpriseToRevenue * totalRevenue
    marketCap = enterpriseValue - bridge_enterpriseValue_marketCap
    
    return marketCap

def ebitda_valuation(enterpriseToEbitda, ebitda, bridge_enterpriseValue_marketCap):
    """Perform an enterprise valuation through the ebidta multiple comparable method. Returns the market capitalization."""
    
    enterpriseValue = enterpriseToEbitda * ebitda
    marketCap = enterpriseValue - bridge_enterpriseValue_marketCap
    
    return marketCap

def earnings_valuation(trailingPE, earnings, bridge_enterpriseValue_marketCap):
    """Perform an enterprise valuation through the P/E ratio comparable method. Returns the market capitalization."""
    
    marketCap = trailingPE * earnings
    enterpriseValue = marketCap + bridge_enterpriseValue_marketCap
    
    return marketCap 

def book_valuation(priceToBook, bookValue, bridge_enterpriseValue_marketCap):
    """Perform an enterprise valuation through the price to book comparable method. Returns the market capitalization."""

    marketCap = priceToBook * bookValue
    enterpriseValue = marketCap + bridge_enterpriseValue_marketCap
    
    return marketCap

def relative_std(values):
    """Compute the relative standard deviation of a set of values"""

    std = np.nanstd(values)
    mean = np.nanmean(values)
    
    relative_std = (std / mean)
    
    return relative_std

def target_confidence_peers(relative_std_stock_price):
    """Define a confidence level on the target price --> rule based on dispersion between different prediction methods"""
    
    if relative_std_stock_price < 0: # Probably an error in valuation if stock price is negative
        confidence = "Low"
    
    elif relative_std_stock_price <= 0.25:
        confidence = "High"
    
    elif relative_std_stock_price <= 0.5:
        confidence = "Medium"
    
    else:
        confidence = "Low"
        
    return confidence

def peers_valuation(groupby_col, financials, valuations, stock_price):
    """
    For a set of companies information (financials, multiples) perform a comparable valuation :
    - Compute mean multiples by the column defined in groupby_col
    - Apply mean multiples to relevant financial (revenue, ebitda, earnings, book value)
    - Convert valuation into stock prices
    - Compute mean stock price over each valuation method
    - Compute difference with actual price
    Returns mean multiples and peer valuation details
    """

    capitalized_groupby_col = groupby_col.capitalize()

    # Compure mean sector multiples
    mean_multiples = compute_mean_multiples(valuations, groupby_col)

    # Select and Join
    # peers = pd.merge(multiples[['symbol', 'shortName', groupby_col]], financials, on='symbol')
    peers = pd.merge(valuations, financials, on='symbol')
    # peers = pd.merge(peers, last_valuations[['symbol', 'bookValue', 'BridgeEnterpriseValueMarketCap', 'sharesOutstanding']], on='symbol')
    peers = pd.merge(peers, mean_multiples, on=groupby_col)
    peers = pd.merge(peers, stock_price, on='symbol')
    
    # Apply valuation functions
    peers[f'MarketCapRevenue{capitalized_groupby_col}'] = peers.apply(lambda x: revenue_valuation(x[f'Mean{capitalized_groupby_col}EnterpriseValueRevenueMultiple'], x['TotalRevenue'], x['NetDebt']), axis=1)
    peers[f'MarketCapEbitda{capitalized_groupby_col}'] = peers.apply(lambda x: ebitda_valuation(x[f'Mean{capitalized_groupby_col}EnterpriseValueEBITDAMultiple'], x['EBITDA'], x['NetDebt']), axis=1)
    peers[f'MarketCapBook{capitalized_groupby_col}'] = peers.apply(lambda x: book_valuation(x[f'Mean{capitalized_groupby_col}PriceToBookRatio'], x['TotalEquityGrossMinorityInterest'], x['NetDebt']), axis=1)
    peers[f'MarketCapEarnings{capitalized_groupby_col}'] = peers.apply(lambda x: earnings_valuation(x[f'Mean{capitalized_groupby_col}PriceEarningsRatio'], x['NetIncome'], x['NetDebt']), axis=1)

    # Convert market capitalizations into stock prices (or the other way round)
    peers[f'StockPriceRevenue{capitalized_groupby_col}'] = peers[f'MarketCapRevenue{capitalized_groupby_col}'] / peers['OrdinarySharesNumber']
    peers[f'StockPriceEbitda{capitalized_groupby_col}'] = peers[f'MarketCapEbitda{capitalized_groupby_col}'] / peers['OrdinarySharesNumber']
    peers[f'StockPriceEarnings{capitalized_groupby_col}'] = peers[f'MarketCapEarnings{capitalized_groupby_col}'] / peers['OrdinarySharesNumber']
    peers[f'StockPriceBook{capitalized_groupby_col}'] = peers[f'MarketCapBook{capitalized_groupby_col}'] / peers['OrdinarySharesNumber']
    
    # Compute mean stock price over all valuation approaches
    peers[f'PeersMeanStockPrice{capitalized_groupby_col}'] = peers.apply(lambda x: np.nanmean([x[f'StockPriceBook{capitalized_groupby_col}'], x[f'StockPriceRevenue{capitalized_groupby_col}'], x[f'StockPriceEbitda{capitalized_groupby_col}'], x[f'StockPriceEarnings{capitalized_groupby_col}']]), axis=1)
    peers[f'PeersRelativeStdStockPrice{capitalized_groupby_col}'] = peers.apply(lambda x: relative_std([x[f'StockPriceBook{capitalized_groupby_col}'], x[f'StockPriceRevenue{capitalized_groupby_col}'], x[f'StockPriceEbitda{capitalized_groupby_col}'], x[f'StockPriceEarnings{capitalized_groupby_col}']]), axis=1)
    
    # Compute differential with actual stock prices
    peers[f'PeersAbsoluteDiff{capitalized_groupby_col}'] = peers[f'PeersMeanStockPrice{capitalized_groupby_col}'] - peers['lastPrice']
    peers[f'PeersRelativeDiff{capitalized_groupby_col}'] = peers[f'PeersAbsoluteDiff{capitalized_groupby_col}'] / peers['lastPrice']

    # Set a confidence level
    peers[f'PeersConfidence{capitalized_groupby_col}'] = peers[f'PeersRelativeStdStockPrice{capitalized_groupby_col}'].apply(lambda x: target_confidence_peers(x))

    # Final select
    peers = peers[[
        'symbol',
        'date',
        groupby_col,
        f'MarketCapRevenue{capitalized_groupby_col}',
        f'MarketCapEbitda{capitalized_groupby_col}',
        f'MarketCapEarnings{capitalized_groupby_col}',
        f'MarketCapBook{capitalized_groupby_col}',
        f'StockPriceRevenue{capitalized_groupby_col}', 
        f'StockPriceEbitda{capitalized_groupby_col}', 
        f'StockPriceEarnings{capitalized_groupby_col}',
        f'StockPriceBook{capitalized_groupby_col}',
        f'PeersMeanStockPrice{capitalized_groupby_col}', 
        f'PeersRelativeStdStockPrice{capitalized_groupby_col}',
        f'PeersAbsoluteDiff{capitalized_groupby_col}',
        f'PeersRelativeDiff{capitalized_groupby_col}',
        f'PeersConfidence{capitalized_groupby_col}'
    ]]

    return mean_multiples, peers

# ---------------------------------------------------------------------------------------------------
# Estimates functions

def target_confidence_estimates(numberOfAnalystOpinions):
    """Define a confidence level on the target price --> basic rule for now on number of analysts"""
    
    if not numberOfAnalystOpinions:
        confidence = "NaN"
    
    elif numberOfAnalystOpinions <= 10:
        confidence = "Low"
    
    elif numberOfAnalystOpinions <= 15:
        confidence = "Medium"
    
    else:
        confidence = "High"
        
    return confidence

# ---------------------------------------------------------------------------------------------------
# Financials extraction functions

def to_datetime(date):
    """
    Converts a numpy datetime64 object to a python datetime object 
    Input:
      date - a np.datetime64 object
    Output:
      DATE - a python datetime object
    """
    timestamp = ((date - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))
    
    return dt.datetime.fromtimestamp(timestamp).date()

def revenue_growth(row, historical_financials):
    
    row_symbol = row['symbol']
    row_revenue = row['TotalRevenue']
    symbol_financials = historical_financials[historical_financials['symbol'] == row_symbol]
    one_year = row['asOfDate'] - dt.timedelta(days=365)
    one_year_plus = row['asOfDate'] - dt.timedelta(days=366)
    
    if one_year in symbol_financials['asOfDate'].values:    
        last_year_revenue = symbol_financials[symbol_financials['asOfDate'] == one_year]['TotalRevenue'].values[0]
        return (row_revenue - last_year_revenue) / last_year_revenue
    
    elif one_year_plus in symbol_financials['asOfDate'].values:
        last_year_revenue = symbol_financials[symbol_financials['asOfDate'] == one_year_plus]['TotalRevenue'].values[0]
        return (row_revenue - last_year_revenue) / last_year_revenue
    
    else:
        return None
    
def compute_financial_ratios(historical_financials):

    historical_financials['asOfDate'] = historical_financials['asOfDate'].apply(lambda x: to_datetime(x))

    historical_financials["RevenueGrowth"] = historical_financials.apply(lambda x: revenue_growth(x, historical_financials), axis=1)
    historical_financials["GrossMargin"] = historical_financials["GrossProfit"] / historical_financials["TotalRevenue"]
    historical_financials["EBITDAMargin"] = historical_financials["EBITDA"] / historical_financials["TotalRevenue"]
    historical_financials["EBITMargin"] = historical_financials["EBIT"] / historical_financials["TotalRevenue"]
    historical_financials["PretaxIncomeMargin"] = historical_financials["PretaxIncome"] / historical_financials["TotalRevenue"]
    historical_financials["NetIncomeMargin"] = historical_financials["NetIncome"] / historical_financials["TotalRevenue"]

    historical_financials["NetDebt"] = historical_financials["TotalDebt"] - historical_financials["CashAndCashEquivalents"]
    historical_financials["Leverage"] = historical_financials.apply(lambda x: (x["NetDebt"] / x["EBITDA"]) if x["NetDebt"] > 0 else 0, axis=1) # Ou sinon levier négatif ?

    historical_financials["PercentageCapitalExpenditureRevenue"] = historical_financials["CapitalExpenditure"] / historical_financials["TotalRevenue"]

    historical_financials["ReturnOnEquity"] = historical_financials["NetIncome"] / historical_financials["TotalEquityGrossMinorityInterest"]
    historical_financials["ReturnOnAssets"] = historical_financials["NetIncome"] / historical_financials["TotalAssets"]

    historical_financials["FreeCashFlowMargin"] = historical_financials["FreeCashFlow"] / historical_financials["TotalRevenue"]
    historical_financials["ConversionEBITDAFreeCashFlow"] = historical_financials["FreeCashFlow"] / historical_financials["EBITDA"]
    historical_financials["ConversionNetIncomeFreeCashFlow"] = historical_financials["FreeCashFlow"] / historical_financials["NetIncome"]
    historical_financials["ConversionEBITDACash"] = historical_financials["ChangesInCash"] / historical_financials["EBITDA"]
    historical_financials["ConversionNetIncomeCash"] = historical_financials["ChangesInCash"] / historical_financials["NetIncome"]

    historical_financials["NetIncomePerShare"] = historical_financials["NetIncome"] / historical_financials["OrdinarySharesNumber"]
    historical_financials["FreeCashFlowPerShare"] = historical_financials["FreeCashFlow"] / historical_financials["OrdinarySharesNumber"]
    historical_financials["NetAssetPerShare"] = historical_financials["TotalEquityGrossMinorityInterest"] / historical_financials["OrdinarySharesNumber"]

    return historical_financials

def daily_valuation(row, historical_financials):
    """"Function using financials and stock prices info to compute valuation details. To be applied to history_prices"""
    
    # Filter financials for the row symbol
    row_symbol = row['symbol']
    symbol_financials = historical_financials[historical_financials['symbol'] == row_symbol]

    # Find the closest date of the row
    row_date = row['date']
    deltas = {}
    
    for date in symbol_financials['date'].values:
        
        if date < row_date: # We take only more ancient dates because we can't perform valuation based on financials we don't at the time of the row
            delta = row_date - date
            deltas[delta] = date

    if len(deltas) == 0: # If no financials available return None for all valuation measures
        row['MarketCap'] = None
        row['EnterpriseValue'] = None
        row['EnterpriseValueRevenueMultiple'] = None
        row['EnterpriseValueEBITDAMultiple'] = None
        row['PriceToBookRatio'] = None
        row['PriceEarningsRatio'] = None

        return row
    
    else:
        min_delta = min(deltas) # We take the closest date

        if min_delta < dt.timedelta(days=366): # If the date is less than 1 year difference we will use financials of this date
            selected_date = deltas[min_delta]
        
        else: # Else it's too ancient and we return None for all valuation measures
            row['MarketCap'] = None
            row['EnterpriseValue'] = None
            row['EnterpriseValueRevenueMultiple'] = None
            row['EnterpriseValueEBITDAMultiple'] = None
            row['PriceToBookRatio'] = None
            row['PriceEarningsRatio'] = None

            return row

    # Select financials of the closest date if available
    symbol_financials = symbol_financials[symbol_financials['date'] == selected_date]

    if "12M" in symbol_financials['periodType'].values: # We take in priority 12 months results if available
        symbol_financials = symbol_financials[symbol_financials['periodType'] == "12M"][["TotalRevenue", "EBITDA", "NetIncome", "NetDebt", "OrdinarySharesNumber", "TotalEquityGrossMinorityInterest"]]
    else:
        symbol_financials = symbol_financials[["TotalRevenue", "EBITDA", "NetIncome", "NetDebt", "OrdinarySharesNumber", "TotalEquityGrossMinorityInterest"]]

    # Use selected financials to compute valuation based on row stock price
    row['MarketCap'] = row['adjclose'] * symbol_financials['OrdinarySharesNumber'].values[0]
    row['EnterpriseValue'] = row['MarketCap'] + symbol_financials['NetDebt'].values[0]
    row['EnterpriseValueRevenueMultiple'] = row['EnterpriseValue'] / symbol_financials['TotalRevenue'].values[0]
    row['EnterpriseValueEBITDAMultiple'] = row['EnterpriseValue'] / symbol_financials['EBITDA'].values[0]
    row['PriceToBookRatio'] = row['MarketCap'] / symbol_financials['TotalEquityGrossMinorityInterest'].values[0]
    row['PriceEarningsRatio'] = row['MarketCap'] / symbol_financials['NetIncome'].values[0]
    
    return row
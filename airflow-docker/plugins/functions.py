from yahooquery import Ticker
import pandas as pd
import numpy as np
import datetime as dt
import pytz



# ----------------------------- Extraction API calls functions ---------------------------------

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

# ----------------------------- Valuation functions ---------------------------------

def compute_mean_multiples(multiples, groupby_col):
    """Compute mean valuation multiples on a groupping column for a given set of multiples"""

    capitalized_groupby_col = groupby_col.capitalize()

    mean_multiples = multiples.groupby(by=groupby_col)[[
        'priceToBook', 
        'enterpriseToRevenue', 
        'enterpriseToEbitda', 
        'trailingPE'
    ]].mean()
    
    mean_multiples.columns = [
        f'Mean{capitalized_groupby_col}PriceToBook', 
        f'Mean{capitalized_groupby_col}EnterpriseToRevenue', 
        f'Mean{capitalized_groupby_col}EnterpriseToEbitda', 
        f'Mean{capitalized_groupby_col}TrailingPE'
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

def peers_valuation(groupby_col, financials, multiples, last_valuations, stock_price):
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
    mean_multiples = compute_mean_multiples(multiples, groupby_col)

    # Select and Join
    # peers = pd.merge(multiples[['symbol', 'shortName', groupby_col]], financials, on='symbol')
    peers = pd.merge(multiples, financials, on='symbol')
    peers = pd.merge(peers, last_valuations[['symbol', 'bookValue', 'BridgeEnterpriseValueMarketCap', 'sharesOutstanding']], on='symbol')
    peers = pd.merge(peers, mean_multiples, on=groupby_col)
    peers = pd.merge(peers, stock_price, on='symbol')
    
    # Apply valuation functions
    peers[f'marketCapRevenue{capitalized_groupby_col}'] = peers.apply(lambda x: revenue_valuation(x[f'Mean{capitalized_groupby_col}EnterpriseToRevenue'], x['totalRevenue'], x['BridgeEnterpriseValueMarketCap']), axis=1)
    peers[f'marketCapEbitda{capitalized_groupby_col}'] = peers.apply(lambda x: ebitda_valuation(x[f'Mean{capitalized_groupby_col}EnterpriseToEbitda'], x['ebitda'], x['BridgeEnterpriseValueMarketCap']), axis=1)
    peers[f'stockPriceBook{capitalized_groupby_col}'] = peers.apply(lambda x: book_valuation(x[f'Mean{capitalized_groupby_col}PriceToBook'], x['bookValue'], x['BridgeEnterpriseValueMarketCap']), axis=1)
    peers[f'marketCapEarnings{capitalized_groupby_col}'] = peers.apply(lambda x: earnings_valuation(x[f'Mean{capitalized_groupby_col}TrailingPE'], x['earnings'], x['BridgeEnterpriseValueMarketCap']), axis=1)

    # Convert market capitalizations into stock prices (or the other way round)
    peers[f'stockPriceRevenue{capitalized_groupby_col}'] = peers[f'marketCapRevenue{capitalized_groupby_col}'] / peers['sharesOutstanding']
    peers[f'stockPriceEbitda{capitalized_groupby_col}'] = peers[f'marketCapEbitda{capitalized_groupby_col}'] / peers['sharesOutstanding']
    peers[f'stockPriceEarnings{capitalized_groupby_col}'] = peers[f'marketCapEarnings{capitalized_groupby_col}'] / peers['sharesOutstanding']
    peers[f'marketCapBook{capitalized_groupby_col}'] = peers[f'stockPriceBook{capitalized_groupby_col}'] * peers['sharesOutstanding']
    
    # Compute mean stock price over all valuation approaches
    peers[f'PeersMeanStockPrice{capitalized_groupby_col}'] = peers.apply(lambda x: np.nanmean([x[f'stockPriceBook{capitalized_groupby_col}'], x[f'stockPriceRevenue{capitalized_groupby_col}'], x[f'stockPriceEbitda{capitalized_groupby_col}'], x[f'stockPriceEarnings{capitalized_groupby_col}']]), axis=1)
    peers[f'PeersRelativeStdStockPrice{capitalized_groupby_col}'] = peers.apply(lambda x: relative_std([x[f'stockPriceBook{capitalized_groupby_col}'], x[f'stockPriceRevenue{capitalized_groupby_col}'], x[f'stockPriceEbitda{capitalized_groupby_col}'], x[f'stockPriceEarnings{capitalized_groupby_col}']]), axis=1)
    
    # Compute differential with actual stock prices
    peers[f'PeersAbsoluteDiff{capitalized_groupby_col}'] = peers[f'PeersMeanStockPrice{capitalized_groupby_col}'] - peers['lastPrice']
    peers[f'PeersRelativeDiff{capitalized_groupby_col}'] = peers[f'PeersAbsoluteDiff{capitalized_groupby_col}'] / peers['lastPrice']

    # Set a confidence level
    peers[f'PeersConfidence{capitalized_groupby_col}'] = peers[f'PeersRelativeStdStockPrice{capitalized_groupby_col}'].apply(lambda x: target_confidence_peers(x))

    # Final select
    peers = peers[[
        'symbol',
        'date',
        # 'shortName',
        groupby_col,
        # 'lastPrice',
        # 'priceToBook',
        # 'enterpriseToRevenue',
        # 'enterpriseToEbitda',
        # 'trailingPE',
        'BridgeEnterpriseValueMarketCap',
        f'marketCapRevenue{capitalized_groupby_col}',
        f'marketCapEbitda{capitalized_groupby_col}',
        f'marketCapEarnings{capitalized_groupby_col}',
        f'marketCapBook{capitalized_groupby_col}',
        f'stockPriceRevenue{capitalized_groupby_col}', 
        f'stockPriceEbitda{capitalized_groupby_col}', 
        f'stockPriceEarnings{capitalized_groupby_col}',
        f'stockPriceBook{capitalized_groupby_col}',
        f'PeersMeanStockPrice{capitalized_groupby_col}', 
        f'PeersRelativeStdStockPrice{capitalized_groupby_col}',
        f'PeersAbsoluteDiff{capitalized_groupby_col}',
        f'PeersRelativeDiff{capitalized_groupby_col}',
        f'PeersConfidence{capitalized_groupby_col}'
    ]]

    return mean_multiples, peers

# ----------------------------- Estimates functions ---------------------------------

def target_confidence_estimates(numberOfAnalystOpinions):
    """Define a confidence level on the target price --> basic rule for now on number of analysts"""
    
    if numberOfAnalystOpinions == np.NaN: # DOES NOT WORK ?
        confidence = "NaN"
    
    if numberOfAnalystOpinions <= 10:
        confidence = "Low"
    
    elif numberOfAnalystOpinions <= 15:
        confidence = "Medium"
    
    else:
        confidence = "High"
        
    return confidence
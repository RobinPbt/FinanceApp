import pandas as pd
import numpy as np

# ----------------------------- Valuation functions ---------------------------------

def compute_mean_sector_multiples(multiples):
    """Compute mean valuation multiples of sectors for a given set of multiples"""

    mean_sector_multiples = multiples.groupby(by='sector')[['priceToBook', 'enterpriseToRevenue', 'enterpriseToEbitda', 'trailingPE']].mean()
    mean_sector_multiples.columns = ['mean_sector_priceToBook', 'mean_sector_enterpriseToRevenue', 'mean_sector_enterpriseToEbitda', 'mean_sector_trailingPE']
    
    return mean_sector_multiples

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

def peers_valuation(financials, multiples, last_valuations):
    """
    For a set of companies information (financials, multiples) perform a comparable valuation :
    - Compute mean multiples by sector
    - Apply mean multiples to relevant financial (revenue, ebitda, earnings, book value)
    - Convert valuation into stock prices
    - Compute mean stock price over each valuation method
    """

    # Load
    mean_sector_multiples = compute_mean_sector_multiples(multiples)

    # Select and Join
    peers = pd.merge(multiples[['symbol', 'shortName', 'sector']], financials, on='symbol')
    peers = pd.merge(peers, last_valuations[['symbol', 'bookValue', 'bridge_enterpriseValue_marketCap', 'sharesOutstanding']], on='symbol')
    peers = pd.merge(peers, mean_sector_multiples, on='sector')
    
    # Apply valuation functions
    peers['marketCap_revenue'] = peers.apply(lambda x: revenue_valuation(x['mean_sector_enterpriseToRevenue'], x['totalRevenue'], x['bridge_enterpriseValue_marketCap']), axis=1)
    peers['marketCap_ebitda'] = peers.apply(lambda x: ebitda_valuation(x['mean_sector_enterpriseToEbitda'], x['ebitda'], x['bridge_enterpriseValue_marketCap']), axis=1)
    peers['stock_price_book'] = peers.apply(lambda x: book_valuation(x['mean_sector_priceToBook'], x['bookValue'], x['bridge_enterpriseValue_marketCap']), axis=1)
    peers['marketCap_earnings'] = peers.apply(lambda x: earnings_valuation(x['mean_sector_trailingPE'], x['earnings'], x['bridge_enterpriseValue_marketCap']), axis=1)

    # Convert market capitalizations into stock prices
    peers['stock_price_revenue'] = peers['marketCap_revenue'] / peers['sharesOutstanding']
    peers['stock_price_ebitda'] = peers['marketCap_ebitda'] / peers['sharesOutstanding']
    peers['stock_price_earnings'] = peers['marketCap_earnings'] / peers['sharesOutstanding']
    
    # Compute mean stock price over all valuation approaches
    peers['mean_stock_price'] = peers.apply(lambda x: np.nanmean([x['stock_price_book'], x['stock_price_revenue'], x['stock_price_ebitda'], x['stock_price_earnings']]), axis=1)
    peers['relative_std_stock_price'] = peers.apply(lambda x: relative_std([x['stock_price_book'], x['stock_price_revenue'], x['stock_price_ebitda'], x['stock_price_earnings']]), axis=1)
    
    # Final select
    peers = peers[['symbol', 'shortName', 'stock_price_book', 'stock_price_revenue', 'stock_price_ebitda', 'stock_price_earnings', 'mean_stock_price', 'relative_std_stock_price']]
    
    return peers

def target_confidence(relative_std_stock_price):
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

def price_differential(peers, stock_price):
    """Compute the difference between the mean stock price of valuations methods and the actual price. Set a confidence level of valuation"""
       
    # Select and join
    differential_df = pd.merge(peers[['symbol', 'shortName', 'mean_stock_price', 'relative_std_stock_price']], stock_price, on='symbol')
    
    # Compute diff
    differential_df['absolute_diff'] = differential_df['mean_stock_price'] - differential_df['lastPrice']
    differential_df['relative_diff'] = differential_df['absolute_diff'] / differential_df['lastPrice']
    
    # Set a confidence level
    differential_df['confidence'] = differential_df['relative_std_stock_price'].apply(lambda x: target_confidence(x))
    
    # Sort from most undervalued to most overvalued
    differential_df = differential_df.sort_values(by='relative_diff', ascending=False)
    
    return differential_df
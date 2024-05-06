import pandas as pd
import numpy as np
import time
import asyncio

from functools import wraps

from langchain.chat_models import ChatOpenAI
from langchain.tools import DuckDuckGoSearchRun, WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.chains import LLMChain

# ---------------------------------------------------------------------------------------------------
# General functions

def metrics_value_formatting(value, value_type="percentage", percentage_format="growth"):
    """
    Function for value formatting before displaying in st.metric.

    args:
    - value (numeric): value to be formatted
    - value_type (str: "percentage", "millions" or "ratio"): type of numeric value
    - percentage_format (str: "growth", "margin" or ""): format for percentages
    """

    if not value:
        return value
    else:
        if value_type == "percentage":
            if percentage_format == "growth":
                return "{:,.2%} growth".format(value)
            elif percentage_format == "margin":
                return "{:,.2%} of revenue".format(value)
            else:
                return "{:,.2%}".format(value)
        
        elif value_type == "millions":
            return "€{:,.0f} m".format(value / 10**6)
        
        elif value_type == "ratio":
            return "{:,.2f}".format(value)

# ---------------------------------------------------------------------------------------------------
# Valuation functions

def compute_mean_multiples(multiples, groupby_col):
    """Compute mean valuation multiples of clusters for a given set of multiples"""

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

    mean_multiples = mean_multiples.reset_index()
    
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
        'shortName',
        'cluster',
        'lastPrice',
        'priceToBook',
        'enterpriseToRevenue',
        'enterpriseToEbitda',
        'trailingPE',
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
# ---------------------------------------------------------------------------------------------------
# LLM functions

def retry(max_attempts=3, delay_seconds=2, check_return_value=False):
    """
    A decorator for retrying a function if it raises an exception or returns a falsy value (if check_return_value is True).

    Args:
        max_attempts (int): Maximum number of attempts.
        delay_seconds (int): Delay between attempts in seconds.
        check_return_value (bool): If True, also retry if the return value is falsy.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    result = func(*args, **kwargs)
                    if not check_return_value or result:
                        return result
                    else:
                        raise ValueError("Function returned a falsy value.")
                except Exception as e:
                    print(f"Attempt {attempts+1} failed with error: {e}")
                    attempts += 1
                    if attempts < max_attempts:
                        print(f"Retrying in {delay_seconds} seconds...")
                    time.sleep(delay_seconds)
            raise Exception(f"All {max_attempts} attempts failed.")
        return wrapper
    return decorator

@retry(max_attempts=5, delay_seconds=2, check_return_value=True)
def fetch_wikipedia_data(company_name):
    wikipedia = WikipediaAPIWrapper()
    return wikipedia.run(company_name)

@retry(max_attempts=5, delay_seconds=2, check_return_value=True)
def fetch_web_search_results(company_name):
    search = DuckDuckGoSearchRun()
    return search.run(company_name)

# ---------------------------------------------------------------------------------------------------
# Portfolio allocation functions

def compute_returns(stock_prices_df, continuous=True):
    """
    Compute daily returns for provided stocks prices.

    args:
    - stock_prices_df (pd.DataFrame): panel of stocks prices over a selected period. The shape must be with symbols / tickers as rows, dates as columns and prices as values
    - continuous (bool): True to compute continious returns, False for discrete returns
    """
    
    # Dividends not included --> TO BE IMPROVED
    returns_df = stock_prices_df.copy()
    
    if continuous: # Continuous returns
        for col in returns_df.columns:
            returns_df[col] = np.log(stock_prices_df[col] / stock_prices_df.shift(1, axis="columns")[col])
            
    else: # Discrete returns
        for col in returns_df.columns:
            stock_prices_df[col] = stock_prices_df[col].pct_change()
            
    return returns_df

def expected_return(weights_vector, returns_vector):
    """
    Compute the expected return of a given portfolio allocation.
    
    args:
    - weights_vector (np.array): weights allocated to each stock
    - returns_vector (np.array): expected returns for each stock
    """
    
    expected_return = weights_vector.T @ returns_vector
    expected_return = expected_return[0][0]
    
    return expected_return

def expected_variance(weights_vector, covariance_matrix):
    """
    Compute the expected variance of a given portfolio allocation.
    
    args:
    - weights_vector (np.array): weights allocated to each stock
    - covariance_matrix (np.array): covariance matrix of stock daily returns
    """
    
    expected_variance = weights_vector.T @ covariance_matrix @ weights_vector
    expected_variance = expected_variance.values[0][0]
    
    return expected_variance

def dirty_rebase_100(weights_vector):
    """Delete negative weights and rebase to 100%"""
    
    rebased_weights_vector = weights_vector.copy()

    rebased_weights_vector[rebased_weights_vector < 0] = 0
    new_total = rebased_weights_vector.sum()
    f = lambda x: x / new_total
    rebased_weights_vector = f(rebased_weights_vector)

    return rebased_weights_vector


class PortfolioAllocation():
    """
    Class allowing to compute best allocations of investments (weights) in a panel of stocks to optmize risk/return couple. 
    
    args:
    - historical_prices (pd.DataFrame): panel of stocks prices over a selected period. The shape must be with symbols / tickers as rows, dates as columns and prices as values
    - expected_returns (pd.Series): expected returns per symbol / ticker if you have an estimation of expected returns (1 value per ticker). If not use computed historical returns
    """
       
    def __init__(self, historical_prices, expected_returns=pd.Series()):
        
        self.symbols = historical_prices.index
        
        # Compute historical returns for each date and for the period
        self.daily_historical_returns = compute_returns(historical_prices, continuous=True).T
        self.total_historical_returns = self.daily_historical_returns.sum()
        
        # Choose expected returns, if empty serie use historical returns
        if expected_returns.empty:
            self.returns_vector = np.array(self.total_historical_returns).reshape(-1, 1)
        else:
            self.returns_vector = np.array(expected_returns).reshape(-1, 1)         
        
        # Compute covariance matrix and its inverse
        self.covariance_matrix = self.daily_historical_returns.cov()
        self.inv_covariance_matrix = np.linalg.inv(self.covariance_matrix)
        
        # Compute vector of ones and returns vector with ones column
        self.unit_vector = np.full((len(self.returns_vector), 1), 1)
        self.returns_unit = np.c_[self.returns_vector, self.unit_vector].T
        
        # Compute transormation matrix for efficient frontier --> compute directly in __init__ or in a dedicated function ?
        a = self.returns_unit @ self.inv_covariance_matrix @ self.returns_unit.T
        inv_a = np.linalg.inv(a)
        self.transfo_matrix = self.inv_covariance_matrix @ self.returns_unit.T @ inv_a
        
    def compute_GMVP_weights(self, display_results=True, only_positive=True):
        """
        Compute GMVP (Global minimum variance portfolio) weights allocation.
        
        args:
        - display_results (bool): decide to display or not expected return, variance and volatility
        - only_positive (bool): if True add a positive constraint for weights (dirty computation to be improved)
        """
    
        self.GMVP_weights = (self.inv_covariance_matrix @ self.unit_vector) / (self.unit_vector.T @ self.inv_covariance_matrix @ self.unit_vector)
        # Delete negative value and rebase to 100%
        if only_positive:
            self.GMVP_weights = dirty_rebase_100(self.GMVP_weights)
        
        self.expected_return_GMVP = expected_return(self.GMVP_weights, self.returns_vector)
        self.expected_variance_GMVP = expected_variance(self.GMVP_weights, self.covariance_matrix)

        if display_results:
            print("Expected return GMVP : {:.6f}".format(self.expected_return_GMVP))
            print("Expected variance GMVP : {:.6f}".format(self.expected_variance_GMVP))
            print("Expected volatility GMVP : {:.6f}".format(np.sqrt(self.expected_variance_GMVP)))
        
    def compute_efficient_portfolio_weights(self, target_return, display_results=True, only_positive=True):
        """
        Compute portfolio weights allocation which minimizes variance for a given target return.
        
        args:
        - display_results (bool): decide to display or not expected return, variance and volatility
        - only_positive (bool): if True add a positive constraint for weights (dirty computation to be improved)
        """

        self.efficient_portfolio_weights = self.transfo_matrix @ np.array([target_return, 1]).reshape(-1, 1)
        # Delete negative value and rebase to 100%
        if only_positive:
            self.efficient_portfolio_weights = dirty_rebase_100(self.efficient_portfolio_weights)
        
        self.expected_return_efficient_portfolio = expected_return(self.efficient_portfolio_weights, self.returns_vector)
        self.expected_variance_efficient_portfolio = expected_variance(self.efficient_portfolio_weights, self.covariance_matrix)
        
        if display_results:
            print("Expected return efficient portfolio : {:.6f}".format(self.expected_return_efficient_portfolio))
            print("Expected variance efficient portfolio : {:.6f}".format(self.expected_variance_efficient_portfolio))
            print("Expected volatility efficient portfolio : {:.6f}".format(np.sqrt(self.expected_variance_efficient_portfolio)))
        
    def plot_efficient_frontier(self):
        """
        Plot the line of efficient portfolio in the range [-50%, +50%] in a return/variance chart.
        """
        
        expected_returns = [i/100 for i in range(-50, 50, 1)] # Quid letting range as function parameter ? 
        expected_variances = []
        
        # Compute variances for each return specified in expected_returns
        for i in expected_returns:
            weights = self.transfo_matrix @ np.array([i, 1]).reshape(-1, 1)
            expected_var = expected_variance(weights, self.covariance_matrix)
            expected_variances.append(expected_var)
            
        # Plot the frontier and GMVP
        fig = plt.figure()
        
        plt.plot(expected_variances, expected_returns) # Frontier
        plt.scatter(self.expected_variance_GMVP, self.expected_return_GMVP) # GMVP
        plt.text(self.expected_variance_GMVP+(7*(10**-7)), self.expected_return_GMVP-(1*(10**-2)), "GMVP")
        
        plt.ylabel("Rentability")
        plt.xlabel("Variance")
        plt.show()

    def return_weights(self):
        """
        Return efficient portfolio and GMVP weights in two pd.Series with symbol names.
        """

        clean_efficient_portfolio_weights = pd.Series(data=self.efficient_portfolio_weights.reshape(1, -1)[0], index=self.symbols)
        clean_GMVP_weights = pd.Series(data=self.GMVP_weights.reshape(1, -1)[0], index=self.symbols)

        return clean_efficient_portfolio_weights, clean_GMVP_weights
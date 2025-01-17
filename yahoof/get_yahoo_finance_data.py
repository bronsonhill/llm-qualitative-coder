import yfinance as yf
import datetime
from dateutil.relativedelta import relativedelta

def get_yahoo_finance_data(ticker: str, date_str: str):
    """
    Pulls weekly historical price data (18 months from the given date)
    and basic valuation metrics from Yahoo Finance.
    
    Args:
        ticker (str): The stock ticker symbol (e.g. "AAPL").
        date_str (str): The start date in 'YYYY-MM-DD' format (e.g. "2023-01-15").
        
    Returns:
        prices_df (pd.DataFrame): Weekly historical prices (OHLC, Volume, etc.).
        valuation_dict (dict): Basic valuation metrics like Market Cap, P/E ratio, etc.
    """
    # 1. Parse the input date string
    start_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    
    # 2. Compute the end date as 18 months after start_date
    end_date = start_date + relativedelta(months=18)
    
    # 3. Download weekly price data from Yahoo Finance
    #    - interval='1mo' for weekly data
    prices_df = yf.download(
        ticker, 
        start=start_date, 
        end=end_date, 
        interval='1mo',
        progress=False
    )
    
    # 4. Retrieve basic valuation metrics
    #    - This uses yfinance's Ticker().info dictionary
    ticker_obj = yf.Ticker(ticker)
    info = ticker_obj.info  # returns a dict of metadata
    
    # Pick out some commonly used valuation metrics
    valuation_dict = {
        "MarketCap": info.get("marketCap", None),
        "TrailingPE": info.get("trailingPE", None),
        "ForwardPE": info.get("forwardPE", None),
        "PriceToBook": info.get("priceToBook", None),
        "EnterpriseValue": info.get("enterpriseValue", None),
    }
    
    return prices_df, valuation_dict

# Example usage:
weekly_prices, valuations = get_yahoo_finance_data("AAPL", "2023-01-01")
print(weekly_prices)
print(valuations)

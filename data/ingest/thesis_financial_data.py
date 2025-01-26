import json
import pandas as pd
import yfinance as yf
import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Thesis import Thesis

def get_yahoo_finance_data(ticker: str, date_str: str):
    """
    Pulls monthly historical price data (18 months from the given date),
    dividends, and basic valuation metrics from Yahoo Finance.
    
    Args:
        ticker (str): The stock ticker symbol (e.g. "AAPL").
        date_str (str): The start date in 'YYYY-MM-DD' format (e.g. "2023-01-15").
        
    Returns:
        prices_df (pd.DataFrame): Monthly historical prices (OHLC, Volume, etc.).
        dividends_df (pd.DataFrame): Historical dividends data.
        valuation_dict (dict): Basic valuation metrics like Market Cap, P/E ratio, etc.
    """
    # Predefined values in case of failure
    empty_prices_df = pd.DataFrame()
    empty_dividends_df = pd.DataFrame()
    empty_profile = {}

    try:
        # 1. Parse the input date string
        start_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
        
        # 2. Compute the end date as 18 months after start_date
        end_date = start_date + relativedelta(months=18)
        
        # 3. Download monthly price data from Yahoo Finance
        prices_df = yf.download(
            ticker, 
            start=start_date, 
            end=end_date, 
            interval='1d',
            progress=False
        )
        
        # 4. Download dividends data from Yahoo Finance
        dividends_df = yf.Ticker(ticker).dividends
        dividends_df = dividends_df[start_date:end_date]
        
        # 5. Retrieve basic valuation metrics
        ticker_obj = yf.Ticker(ticker)
        profile = ticker_obj.info  # returns a dict of metadata
        
        return prices_df, dividends_df, profile
    except Exception as e:
        print(f"Error retrieving data for {ticker}: {e}")
        return empty_prices_df, empty_dividends_df, empty_profile

def format_prices_df(prices_df):
    # Ensure all keys in the prices_df dictionary are strings
    prices_df = prices_df.reset_index()
    prices_df.columns = [str(col) for col in prices_df.columns]
    prices_dict = prices_df.to_dict(orient='records')
    
    # Convert Timestamp objects to strings
    for record in prices_dict:
        for key, value in record.items():
            if isinstance(value, (datetime.datetime, pd.Timestamp)):
                record[key] = value.isoformat()
    
    return prices_dict

def format_dividends_df(dividends_df):
    # Ensure all keys in the dividends_df dictionary are strings
    dividends_df = dividends_df.reset_index()
    dividends_df.columns = [str(col) for col in dividends_df.columns]
    dividends_dict = dividends_df.to_dict(orient='records')
    
    # Convert Timestamp objects to strings
    for record in dividends_dict:
        for key, value in record.items():
            if isinstance(value, (datetime.datetime, pd.Timestamp)):
                record[key] = value.isoformat()
    
    return dividends_dict

def ingest_yahoo_finance_data(batch_size=100):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        while True:
            # Query a batch of records with null finance data fields
            theses = session.query(Thesis).filter(
                Thesis.profile.is_(None) | 
                Thesis.daily_price.is_(None) | 
                Thesis.dividends.is_(None)
            ).limit(batch_size).all()
            
            if not theses:
                break  # Exit loop if no more records to process

            for thesis in theses:
                try:
                    prices_df, dividends_df, profile = get_yahoo_finance_data(thesis.ticker, thesis.date.strftime("%Y-%m-%d"))
                    print(f"Updating Yahoo Finance data for: {thesis.ticker}")
                    # Example of manually serializing JSON data
                    thesis.profile = json.dumps(profile)
                    thesis.daily_price = json.dumps(format_prices_df(prices_df))
                    thesis.dividends = json.dumps(format_dividends_df(dividends_df))
                    print(f"Profile: {profile}")
                    print(f"Prices: {format_prices_df(prices_df)}")
                    print(f"Dividends: {format_dividends_df(dividends_df)}")

                    session.commit()
                    print(f"Updated Yahoo Finance data for: {thesis.ticker}")
                except Exception as e:
                    error_message = str(e)
                    if 'possibly delisted; no timezone found' in error_message:
                        print(f"Error updating Yahoo Finance data for {thesis.ticker}: {e}")
                        thesis.profile = json.dumps({})
                        thesis.daily_price = json.dumps(['possibly delisted; no timezone found'])
                        thesis.dividends = json.dumps([])
                        session.commit()
                    else:
                        print(f"Error updating Yahoo Finance data for {thesis.ticker}: {e}")
                        session.rollback()
    finally:
        session.close()

ingest_yahoo_finance_data()
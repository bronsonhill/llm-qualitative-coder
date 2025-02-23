import yfinance as yf
import datetime
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Baseline import Baseline
import pandas as pd

def get_daily_price_data(ticker: str):
    """
    Pulls daily historical price data from Yahoo Finance.
    
    Args:
        ticker (str): The stock ticker symbol (e.g. "AAPL").
        
    Returns:
        prices_df (pd.DataFrame): Daily historical prices (OHLC, Volume, etc.).
    """
    # Download daily price data from Yahoo Finance
    prices_df = yf.download(
        ticker, 
        period="max",  # Get the maximum available historical data
        interval='1d',
        progress=False
    )
    
    return prices_df

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

def ingest_baseline_data(ticker: str):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        prices_df = get_daily_price_data(ticker)
        daily_performance = format_prices_df(prices_df)
        start_date = prices_df.index.min().date()  # Fix: Convert to Python date object

        # Check for existing record
        existing_baseline = session.query(Baseline).filter_by(ticker=ticker).first()
        if existing_baseline:
            # Update existing record
            existing_baseline.daily_performance = daily_performance
            existing_baseline.start_date = start_date
            existing_baseline.created_at = datetime.datetime.now(datetime.timezone.utc)
            print(f"Baseline data updated for ticker: {ticker}")
        else:
            # Create new record
            baseline = Baseline(
                ticker=ticker,
                daily_performance=daily_performance,
                start_date=start_date
            )
            session.add(baseline)
            print(f"Baseline data ingested for ticker: {ticker}")

        session.commit()
    except Exception as e:
        print(f"Error ingesting baseline data for {ticker}: {e}")
        session.rollback()
    finally:
        session.close()

# Main execution
if __name__ == "__main__":
    ticker = "^GSPC"  # Example ticker
    ingest_baseline_data(ticker)

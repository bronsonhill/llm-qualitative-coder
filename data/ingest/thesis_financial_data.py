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


def ingest_yahoo_finance_data(batch_size=100):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        while True:
            # Query a batch of records with null finance data fields
            theses = session.query(Thesis).filter(
                Thesis.profile.is_(None) | 
                Thesis.monthly_performance.is_(None) | 
                Thesis.dividends.is_(None)
            ).limit(batch_size).all()
            
            if not theses:
                break  # Exit loop if no more records to process

            for thesis in theses:
                try:
                    prices_df, dividends_df, profile = get_yahoo_finance_data(thesis.ticker, thesis.date.strftime("%Y-%m-%d"))
                    print(f"Updating Yahoo Finance data for: {thesis.ticker}")
                    thesis.profile = profile
                    thesis.monthly_performance = prices_df.reset_index().to_dict(orient='records')
                    thesis.dividends = dividends_df.reset_index().to_dict(orient='records')
                    session.commit()
                    print(f"Updated Yahoo Finance data for: {thesis.ticker}")
                except Exception as e:
                    print(f"Error updating Yahoo Finance data for {thesis.ticker}: {e}")
                    session.rollback()
    finally:
        session.close()

ingest_yahoo_finance_data()
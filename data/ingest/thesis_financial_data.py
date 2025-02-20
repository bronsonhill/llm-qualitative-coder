import json
import pandas as pd
import yfinance as yf
import datetime
import logging
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Thesis import Thesis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    processed_count = 0
    error_count = 0

    try:
        logger.info("Starting Yahoo Finance data ingestion...")
        
        # First, count total records that need processing
        total_records = session.query(Thesis).filter(
            (Thesis.daily_price.is_(None)) | (Thesis.daily_price == '\"[]\"'),
            ~(Thesis.ticker.like('PRIVATE'))
        ).count()
        
        logger.info(f"Found {total_records} records to process")
        
        if total_records == 0:
            logger.info("No records found that need updating. All records either have data or are marked as UNKNOWN/PRIVATE.")
            return

        while True:
            # Query a batch of records with either NULL or empty array daily_price
            theses = session.query(Thesis).filter(
                (Thesis.daily_price.is_(None)) | (Thesis.daily_price == '\"[]\"'),
                ~(Thesis.ticker.like('%UNKNOWN%')),
                ~(Thesis.ticker.like('PRIVATE'))
            ).limit(batch_size).all()
            
            if not theses:
                break

            logger.info(f"Processing batch of {len(theses)} records...")
            
            for thesis in theses:
                logger.debug(f"Ticker: {thesis.ticker}, Daily Price: {thesis.daily_price}")
                try:
                    prices_df, dividends_df, profile = get_yahoo_finance_data(thesis.ticker, thesis.date.strftime("%Y-%m-%d"))
                    logger.info(f"Updating Yahoo Finance data for: {thesis.ticker}")
                    
                    thesis.profile = json.dumps(profile)
                    thesis.daily_price = json.dumps(format_prices_df(prices_df))
                    thesis.dividends = json.dumps(format_dividends_df(dividends_df))

                    session.commit()
                    processed_count += 1
                    logger.info(f"Successfully updated Yahoo Finance data for: {thesis.ticker}")
                except Exception as e:
                    error_count += 1
                    error_message = str(e)
                    if 'possibly delisted; no timezone found' in error_message:
                        logger.warning(f"Ticker {thesis.ticker} possibly delisted: {e}")
                        thesis.profile = json.dumps({})
                        thesis.daily_price = json.dumps(['possibly delisted; no timezone found'])
                        thesis.dividends = json.dumps([])
                        session.commit()
                    else:
                        logger.error(f"Error updating Yahoo Finance data for {thesis.ticker}: {e}")
                        session.rollback()

            logger.info(f"Progress: {processed_count}/{total_records} records processed ({error_count} errors)")
    
    finally:
        session.close()
        logger.info(f"Finished processing. Total records: {processed_count}, Errors: {error_count}")

if __name__ == "__main__":
    ingest_yahoo_finance_data()


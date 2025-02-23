import sys
import os
import random
from datetime import datetime, timedelta
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import csv
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Thesis import Thesis
from data.models.Baseline import Baseline


def filter_baseline_performance(baseline_performance, start_date, end_date):
    """Filter the baseline performance to include only the data within the specified date range."""
    end_date = end_date.date()  # Convert end_date to date object
    filtered_performance = []
    for entry in baseline_performance:
        try:
            entry_date = datetime.strptime(entry["('Date', '')"], '%Y-%m-%dT%H:%M:%S').date()
            if start_date <= entry_date <= end_date:
                filtered_performance.append(entry)
        except (KeyError, ValueError) as e:
            logging.error(f"Error parsing date in entry: {entry}, Error: {e}")
    return filtered_performance


def get_price_keys(ticker):
    """Generate the keys for accessing price data based on the ticker."""
    return {
        'date_key': "('Date', '')",
        'close_key': f"('Close', '{ticker}')",
        'high_key': f"('High', '{ticker}')",
        'low_key': f"('Low', '{ticker}')",
        'open_key': f"('Open', '{ticker}')",
        'volume_key': f"('Volume', '{ticker}')"
    }


def get_next_available_date(prices, target_date, keys):
    """Get the next available date with price data."""
    while target_date.strftime('%Y-%m-%dT00:00:00') not in [entry[keys['date_key']] for entry in prices]:
        target_date += timedelta(days=1)
    return target_date


def calculate_percentage_change(prices, start_date, months, ticker, dividends=None):
    """Calculate the percentage price change over a given number of months, including dividends if provided."""
    target_date = start_date + timedelta(days=months*30)
    logging.info(f"Calculating percentage change from {start_date} to {target_date}")
    # Get the keys for the given ticker
    keys = get_price_keys(ticker)

    try:
        start_price = None
        attempts = 0
        while start_price is None and attempts < 10:
            start_price = next((entry[keys['close_key']] for entry in prices if keys['date_key'] in entry and keys['close_key'] in entry and entry[keys['date_key']] == start_date.strftime('%Y-%m-%dT00:00:00')), None)
            if start_price is None:
                start_date = get_next_available_date(prices, start_date, keys)
            attempts += 1

        end_price = None
        attempts = 0
        while end_price is None and attempts < 10:
            end_price = next((entry[keys['close_key']] for entry in prices if keys['date_key'] in entry and keys['close_key'] in entry and entry[keys['date_key']] == target_date.strftime('%Y-%m-%dT00:00:00')), None)
            if end_price is None:
                target_date = get_next_available_date(prices, target_date, keys)
            attempts += 1
        logging.info(f"Start price: {start_price}, End price: {end_price}")
        if start_price and end_price:
            price_change = ((end_price - start_price) / start_price) * 100
            if dividends:
                total_dividends = sum(float(dividend['Dividends']) for dividend in dividends if start_date <= datetime.strptime(dividend['Date'], '%Y-%m-%dT%H:%M:%S%z').date() <= target_date)
                price_change += (total_dividends / start_price) * 100
            return price_change
    except (KeyError, ValueError) as e:
        logging.error(f"Error calculating percentage change: {e}")
    except (Exception) as e:
        logging.error(f"Error calculating percentage change: {e}")
    return None


def get_thesis_records_with_data(limit):
    """Retrieve thesis records with financial daily price data and thesis text."""
    Session = sessionmaker(bind=engine)
    session = Session()

    eighteen_months_ago = datetime.now() - timedelta(days=18*30)

    logging.info('Querying thesis records from the database')
    try:
        thesis_records = session.query(Thesis).filter(
            Thesis.daily_price.isnot(None),
            Thesis.daily_price.isnot('"[]"'),
            Thesis.text.isnot(None),
            Thesis.daily_price != [],
            Thesis.daily_price.notlike('%delisted%'),
            Thesis.ticker.notlike('%UNKNOWN%'),
            Thesis.ticker.notlike('%PRIVATE%'),
            Thesis.date < eighteen_months_ago
        ).all()
        if len(thesis_records) > limit:
            thesis_records = random.sample(thesis_records, limit)

        logging.info(f'Filtered {len(thesis_records)} thesis records')

        # Retrieve baseline data for each thesis individually
        for thesis in thesis_records:
            baseline = session.query(Baseline).filter_by(ticker='^GSPC').first()
            baseline_performance = baseline.daily_performance if baseline else 'N/A'

            thesis.daily_price = json.loads(thesis.daily_price)
            thesis.dividends = json.loads(thesis.dividends) if thesis.dividends else []
            
            # Filter baseline performance to match the date range of the thesis data
            if baseline_performance != 'N/A':
                start_date = thesis.date
                end_date = datetime.now()
                thesis.baseline_performance = filter_baseline_performance(baseline_performance, start_date, end_date)

                # Calculate percentage price changes
                thesis.price_change_3m = calculate_percentage_change(thesis.daily_price, start_date, 3, thesis.ticker, thesis.dividends)
                thesis.price_change_6m = calculate_percentage_change(thesis.daily_price, start_date, 6, thesis.ticker, thesis.dividends)
                thesis.price_change_12m = calculate_percentage_change(thesis.daily_price, start_date, 12, thesis.ticker, thesis.dividends)
                thesis.price_change_18m = calculate_percentage_change(thesis.daily_price, start_date, 18, thesis.ticker, thesis.dividends)
                print(thesis.price_change_3m, thesis.price_change_6m, thesis.price_change_12m, thesis.price_change_18m)
                thesis.baseline_price_change_3m = calculate_percentage_change(thesis.baseline_performance, start_date, 3, '^GSPC')
                thesis.baseline_price_change_6m = calculate_percentage_change(thesis.baseline_performance, start_date, 6, '^GSPC')
                thesis.baseline_price_change_12m = calculate_percentage_change(thesis.baseline_performance, start_date, 12, '^GSPC')
                thesis.baseline_price_change_18m = calculate_percentage_change(thesis.baseline_performance, start_date, 18, '^GSPC')
                print(thesis.baseline_price_change_3m, thesis.baseline_price_change_6m, thesis.baseline_price_change_12m, thesis.baseline_price_change_18m)
            
                logging.info(f'Calculated percentage price changes for thesis with ticker {thesis.ticker}')

        return thesis_records
    except Exception as e:
        logging.error(f'Error retrieving thesis records: {e}')
        return []
    finally:
        session.close()


def split_data(thesis_records, test_size=0.2):
    """Split the data into training and test sets."""
    logging.info('Splitting data into training and test sets')
    random.shuffle(thesis_records)
    split_index = int(len(thesis_records) * (1 - test_size))
    logging.info(f'Split data into {len(thesis_records[:split_index])} training and {len(thesis_records[split_index:])} test records')
    return thesis_records[:split_index], thesis_records[split_index:]


def export_to_csv(thesis_records, filename):
    """Export thesis records to a CSV file."""
    logging.info(f'Exporting {len(thesis_records)} thesis records to {filename}')
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Date', 'Author', 'Ticker', 'Company Name', 'Link', 'Market Cap', 'Price', 'Text', 'Price Change 3M', 'Price Change 6M', 'Price Change 12M', 'Price Change 18M', 'Baseline Price Change 3M', 'Baseline Price Change 6M', 'Baseline Price Change 12M', 'Baseline Price Change 18M'])
        for thesis in thesis_records:
            # Ensure daily_price is serialized to JSON string before storing in the database
            thesis.daily_price = json.dumps(thesis.daily_price)
            writer.writerow([thesis.date, thesis.author, thesis.ticker, thesis.company_name, thesis.link, thesis.market_cap, thesis.price, thesis.text, thesis.price_change_3m, thesis.price_change_6m, thesis.price_change_12m, thesis.price_change_18m, thesis.baseline_price_change_3m, thesis.baseline_price_change_6m, thesis.baseline_price_change_12m, thesis.baseline_price_change_18m])
    logging.info(f'Exported thesis records to {filename}')


if __name__ == "__main__":
    limit = 1250  # Specify the number of records to retrieve
    logging.info('Starting to retrieve thesis records with financial data')
    thesis_records = get_thesis_records_with_data(limit)
    logging.info(f'Retrieved {len(thesis_records)} thesis records with financial data')
    train_set, test_set = split_data(thesis_records)
    export_to_csv(train_set, 'thesis_records_train.csv')
    export_to_csv(test_set, 'thesis_records_test.csv')
    logging.info(f'Exported {len(train_set)} training records to thesis_records_train.csv')
    logging.info(f'Exported {len(test_set)} test records to thesis_records_test.csv')

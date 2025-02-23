"""Command-line interface for ticker cleaning utility."""
import argparse
import logging
from data.utils.clean_tickers.cleaner import TickerCleaner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Clean and update tickers in the database'
    )
    parser.add_argument(
        '--to-csv', 
        action='store_true', 
        help='Output results to CSV instead of updating database'
    )
    parser.add_argument(
        '--from-csv', 
        type=str, 
        help='Apply updates from a CSV file', 
        metavar='CSV_PATH'
    )
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=100, 
        help='Number of records to process in each database query'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of records to process in total (default: all records)',
        default=None
    )
    
    args = parser.parse_args()
    cleaner = TickerCleaner()
    
    if args.from_csv:
        cleaner.apply_updates_from_csv(args.from_csv)
    else:
        cleaner.clean_tickers(
            batch_size=args.batch_size,
            total_limit=args.limit,
            output_to_csv=args.to_csv
        )

if __name__ == "__main__":
    main()
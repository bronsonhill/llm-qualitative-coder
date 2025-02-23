"""
Ticker cleaning utility for thesis records.
Handles searching, matching, and updating company tickers using Yahoo Finance and GPT.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional, Any
import logging
from pathlib import Path
import json

import yfinance as yf
import pandas as pd
from sqlalchemy.orm import Session, sessionmaker
from openai import OpenAI, RateLimitError
from dotenv import load_dotenv
import os

from data.db import engine
from data.models.Thesis import Thesis

# for 4o-mini model
COST_PER_INPUT_TOKEN = 0.15 / 10**6
COST_PER_OUTPUT_TOKEN = 0.6 / 10**6

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SearchResult:
    """Structured container for Yahoo Finance search results."""
    symbol: str
    name: str
    exchange: str

    @classmethod
    def from_yf_quote(cls, quote: Dict[str, Any]) -> Optional['SearchResult']:
        """Create SearchResult from Yahoo Finance quote object."""
        try:
            return cls(
                symbol=str(quote.get('symbol', '')),
                name=str(quote.get('longname', '')),
                exchange=str(quote.get('exchange', 'Unknown'))
            )
        except AttributeError:
            logger.warning(f"Invalid quote format: {quote}")
            return None

@dataclass
class TickerMatch:
    """Result of ticker matching process."""
    selected_ticker: str
    reasoning: str

@dataclass
class ThesisUpdate:
    """Container for thesis update information."""
    date: datetime
    author: str
    old_ticker: str
    new_ticker: str
    company_name: str
    reasoning: str
    processed_at: datetime = datetime.now()

class YahooFinanceSearcher:
    """Handles Yahoo Finance search operations."""
    
    def __init__(self, max_results: int = 5):
        self.max_results = max_results

    def search(self, query: str, is_ticker: bool = False) -> List[SearchResult]:
        """
        Search Yahoo Finance for company or ticker matches.
        """
        try:
            search = yf.Search(query, max_results=self.max_results)
            results = []
            for quote in search.quotes:
                if result := SearchResult.from_yf_quote(quote):
                    results.append(result)
            return results
        except Exception as e:
            query_type = 'ticker' if is_ticker else 'company'
            logger.error(f"Error searching for {query_type} {query}: {e}")
            return []

class GPTTickerMatcher:
    """Handles GPT-based ticker matching."""
    
    def __init__(self, model: str = "gpt-4o-mini"):
        load_dotenv()
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        self.model = model
        self.client = OpenAI(api_key=api_key)

    def get_best_match(self, company_name: str, ticker: str, 
                      search_results: List[SearchResult]) -> TickerMatch:
        """
        Use GPT to determine the best ticker match from search results.
        
        Args:
            company_name: Name of the company
            ticker: Current ticker symbol
            search_results: List of potential matches from Yahoo Finance
            
        Returns:
            TickerMatch with selected ticker and reasoning
        """
        simplified_results = [
            {
                'symbol': result.symbol,
                'name': result.name,
                'exchange': result.exchange
            }
            for result in search_results
        ]

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=self._create_messages(company_name, ticker, simplified_results),
                tools=[
                    {
                        "type": "function",
                        "function": self._get_function_schema()
                    }
                ],
                tool_choice={"type": "function", "function": {"name": "select_best_ticker_match"}}
            )

            try:
                # Get the function call arguments from the tool calls
                if (not response.choices or 
                    not response.choices[0].message or 
                    not response.choices[0].message.tool_calls):
                    raise ValueError("Invalid response structure")
                
                tool_call = response.choices[0].message.tool_calls[0]
                result = json.loads(tool_call.function.arguments)
                self._log_tokens_and_cost(response)
                return TickerMatch(**result)

            except (json.JSONDecodeError, AttributeError, IndexError, ValueError) as e:
                logger.error(f"Failed to parse GPT response: {e}")
                return TickerMatch(
                    selected_ticker=f"{ticker}_UNKNOWN",
                    reasoning=f"Invalid response format: {str(e)}"
                )

        except RateLimitError as e:
            logger.warning(f"Rate limit hit, waiting before retry: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in GPT processing: {e}")
            return TickerMatch(
                selected_ticker=f"{ticker}_UNKNOWN",
                reasoning=f"Error in processing: {str(e)}"
            )

    @staticmethod
    def _create_messages(company_name: str, ticker: str, 
                        search_results: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Create messages for GPT API call."""
        return [
            {"role": "system", "content": "You are a financial data matching assistant."},
            {"role": "user", "content": (
                f"Given the company name '{company_name}' and ticker '{ticker}', "
                f"select the best matching ticker from these results, or return 'PRIVATE' "
                f"if the company seems to have been privatised, or {ticker}_UNKNOWN if "
                f"no results appear to match. Select the largest exchange where you have "
                f"an option.\n{json.dumps(search_results)}"
            )}
        ]

    @staticmethod
    def _get_function_schema() -> Dict[str, Any]:
        """Get the function schema for GPT API."""
        return {
            "name": "select_best_ticker_match",
            "description": "Select the best matching ticker for a company",
            "parameters": {
                "type": "object",
                "properties": {
                    "selected_ticker": {
                        "type": "string",
                        "description": "The best matching ticker symbol, or 'PRIVATE' if company is private"
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Brief explanation of why this ticker was selected"
                    }
                },
                "required": ["selected_ticker", "reasoning"]
            }
        }

    @staticmethod
    def _log_tokens_and_cost(response: Any) -> None:
        """Log the tokens used and cost of the API call."""
        usage = response.usage
        total_tokens = usage.total_tokens
        prompt_tokens = usage.prompt_tokens
        completion_tokens = usage.completion_tokens
        cost = (prompt_tokens * COST_PER_INPUT_TOKEN) + (completion_tokens * COST_PER_OUTPUT_TOKEN)
        # Log the tokens used and cost to a file for easy calculation of total cost
        log_file_path = Path('data/utils/clean_tickers/token_usage.log')
        with log_file_path.open('a') as log_file:
            log_file.write(
            f"{datetime.now().isoformat()} - Total tokens: {total_tokens}, "
            f"Prompt tokens: {prompt_tokens}, Completion tokens: {completion_tokens}, "
            f"Cost: ${cost:.6f}\n"
            )
        logger.info(f"Total tokens: {total_tokens}, Prompt tokens: {prompt_tokens}, Completion tokens: {completion_tokens}, Cost: ${cost:.6f}")

class ThesisUpdater:
    """Handles thesis record updates in the database."""
    
    def __init__(self, session: Session):
        self.session = session

    def update_ticker(self, thesis: Thesis, new_ticker: str) -> bool:
        """
        Update the ticker for a thesis record.
        
        Args:
            thesis: Thesis record to update
            new_ticker: New ticker symbol
            
        Returns:
            bool indicating success
        """
        try:
            thesis.ticker = new_ticker
            self.session.commit()
            logger.info(f"Updated ticker for {thesis.company_name} to: {new_ticker}")
            return True
        except Exception as e:
            logger.error(f"Error updating thesis ticker: {e}")
            self.session.rollback()
            return False

class TickerCleaner:
    """Main class for cleaning and updating tickers."""
    
    def __init__(self):
        self.searcher = YahooFinanceSearcher()
        self.matcher = GPTTickerMatcher()
        
    def clean_tickers(self, batch_size: int = 100, total_limit: Optional[int] = None,
                     output_to_csv: bool = False) -> Optional[List[ThesisUpdate]]:
        """
        Clean and update tickers for thesis records.
        
        Args:
            batch_size: Number of records to process in each database query
            total_limit: Maximum number of records to process in total (None for all records)
            output_to_csv: Whether to output results to CSV
            
        Returns:
            List of ThesisUpdate if output_to_csv is True, None otherwise
        """
        session = sessionmaker(bind=engine)()
        updater = ThesisUpdater(session)
        results: List[ThesisUpdate] = []
        processed_count = 0

        try:
            while True:
                # Calculate how many records to fetch in this batch
                remaining_limit = None if total_limit is None else total_limit - processed_count
                if remaining_limit is not None and remaining_limit <= 0:
                    break
                
                current_batch_size = min(batch_size, remaining_limit) if remaining_limit else batch_size
                
                # Fetch next batch of records
                theses = self._get_theses_to_update(session, current_batch_size, processed_count)
                if not theses:
                    break  # No more records to process
                
                logger.info(f"Processing batch of {len(theses)} records (processed so far: {processed_count})")
                
                for thesis in theses:
                    result = self._process_thesis(thesis, updater, output_to_csv)
                    if result:
                        results.append(result)
                    processed_count += 1
                    
                    if total_limit and processed_count >= total_limit:
                        break

            if output_to_csv and results:
                self._save_to_csv(results)
                return results

        except Exception as e:
            logger.error(f"Error in clean_tickers: {e}")
        finally:
            session.close()
            logger.info(f"Completed processing {processed_count} records")
        
        return None

    @staticmethod
    def _get_theses_to_update(session: Session, batch_size: int, offset: int) -> List[Thesis]:
        """
        Get thesis records that need updating with pagination.
        
        Args:
            session: Database session
            batch_size: Number of records to fetch
            offset: Number of records to skip
            
        Returns:
            List of Thesis objects
        """
        return session.query(Thesis).filter(
            (Thesis.daily_price.is_(None)) | 
            (Thesis.daily_price == '\"[]\"'),
            ~(Thesis.ticker.like('%UNKNOWN%')),
            ~(Thesis.ticker.like('PRIVATE'))
        ).offset(offset).limit(batch_size).all()

    def apply_updates_from_csv(self, csv_path: str) -> None:
        """
        Apply ticker updates from a CSV file.
        
        Args:
            csv_path: Path to the CSV file containing updates
        """
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Found {len(df)} records to update")
            
            session = sessionmaker(bind=engine)()
            updater = ThesisUpdater(session)
            
            for _, row in df.iterrows():
                self._apply_update_row(row, session, updater)
                
            session.close()
            logger.info("Finished applying updates from CSV")
            
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")

    def _process_thesis(self, thesis: Thesis, updater: ThesisUpdater,
                       output_to_csv: bool) -> Optional[ThesisUpdate]:
        """Process a single thesis record."""
        logger.info(f"Processing: {thesis.company_name} (Current ticker: {thesis.ticker})")
        
        # First try with company name
        search_results = self.searcher.search(thesis.company_name)
        match_result = self.matcher.get_best_match(
            thesis.company_name, thesis.ticker, search_results
        )
        
        # Try with ticker if first search was inconclusive
        if match_result.selected_ticker.endswith("_UNKNOWN"):
            logger.info(f"Company name search inconclusive ({match_result.reasoning})")
            logger.info("Trying ticker search...")
            ticker_results = self.searcher.search(thesis.ticker, is_ticker=True)
            ticker_match = self.matcher.get_best_match(
                thesis.company_name, thesis.ticker, ticker_results
            )
            
            if not ticker_match.selected_ticker.endswith("_UNKNOWN"):
                match_result = ticker_match

        update = ThesisUpdate(
            date=thesis.date,
            author=thesis.author,
            old_ticker=thesis.ticker,
            new_ticker=match_result.selected_ticker,
            company_name=thesis.company_name,
            reasoning=match_result.reasoning
        )
        
        if not output_to_csv and update.new_ticker != thesis.ticker:
            logger.info(f"Updating ticker from {thesis.ticker} to {update.new_ticker}")
            logger.info(f"Reason: {update.reasoning}")
            if not updater.update_ticker(thesis, update.new_ticker):
                return None
        else:
            logger.info(f"Found new ticker: {update.new_ticker}")
            logger.info(f"Reason: {update.reasoning}")
            
        return update

    @staticmethod
    def _save_to_csv(results: List[ThesisUpdate]) -> None:
        """Save results to CSV file."""
        df = pd.DataFrame([vars(r) for r in results])
        output_file = f'data/utils/clean_tickers/ticker_updates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")

    @staticmethod
    def _apply_update_row(row: pd.Series, session: Session, 
                         updater: ThesisUpdater) -> None:
        """Apply a single update row from CSV."""
        try:
            thesis = session.query(Thesis).filter(
                Thesis.date == pd.to_datetime(row['date']).date(),
                Thesis.author == row['author'],
                Thesis.ticker == row['old_ticker']
            ).first()
            
            if thesis:
                logger.info(
                    f"Updating {thesis.company_name} from {row['old_ticker']} "
                    f"to {row['new_ticker']}"
                )
                updater.update_ticker(thesis, row['new_ticker'])
            else:
                logger.warning(
                    f"Could not find thesis record for {row['company_name']} "
                    f"({row['old_ticker']})"
                )
                
        except Exception as e:
            logger.error(f"Error updating record: {e}")
            session.rollback()

def main():
    """Main entry point for the script."""
    import argparse
    
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
    
    args = parser.parse_args()  # Changed from ArgumentParser() to parse_args()
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

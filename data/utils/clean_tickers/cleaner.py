"""Core ticker cleaning functionality."""
import logging
from typing import List, Optional
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session, sessionmaker

from data.db import engine
from data.models.Thesis import Thesis
from .models import ThesisUpdate
from .searcher import YahooFinanceSearcher
from .matcher import GPTTickerMatcher
from .updater import ThesisUpdater

logger = logging.getLogger(__name__)

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
                # Calculate records to fetch
                remaining_limit = None if total_limit is None else total_limit - processed_count
                if remaining_limit is not None and remaining_limit <= 0:
                    break
                
                current_batch_size = min(batch_size, remaining_limit) if remaining_limit else batch_size
                
                # Fetch and process batch
                theses = self._get_theses_to_update(session, current_batch_size, processed_count)
                if not theses:
                    break
                
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

    @staticmethod
    def _get_theses_to_update(session: Session, batch_size: int, offset: int) -> List[Thesis]:
        """Get thesis records that need updating with pagination."""
        return session.query(Thesis).filter(
            (Thesis.daily_price.is_(None)) | 
            (Thesis.daily_price == '\"[]\"')
        ).offset(offset).limit(batch_size).all()

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
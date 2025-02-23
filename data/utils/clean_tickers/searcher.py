"""Yahoo Finance search operations module."""
from typing import List
import logging
import yfinance as yf
from .models import SearchResult

logger = logging.getLogger(__name__)

class YahooFinanceSearcher:
    """Handles Yahoo Finance search operations."""
    
    def __init__(self, max_results: int = 5):
        self.max_results = max_results

    def search(self, query: str, is_ticker: bool = False) -> List[SearchResult]:
        """
        Search Yahoo Finance for company or ticker matches.
        
        Args:
            query: Company name or ticker to search for
            is_ticker: Whether the query is a ticker symbol
            
        Returns:
            List of SearchResult objects
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
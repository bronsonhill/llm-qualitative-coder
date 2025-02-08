"""Data models for ticker cleaning operations."""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Any

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
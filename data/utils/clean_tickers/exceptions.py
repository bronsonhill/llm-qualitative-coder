"""Custom exceptions for the ticker cleaning utility."""
from typing import Optional

class TickerCleanerError(Exception):
    """Base exception for ticker cleaner errors."""
    pass

class SearchError(TickerCleanerError):
    """Raised when Yahoo Finance search fails."""
    def __init__(self, query: str, is_ticker: bool, message: Optional[str] = None):
        self.query = query
        self.is_ticker = is_ticker
        self.message = message or f"Failed to search for {'ticker' if is_ticker else 'company'}: {query}"
        super().__init__(self.message)

class GPTMatchError(TickerCleanerError):
    """Raised when GPT matching fails."""
    def __init__(self, company: str, ticker: str, message: Optional[str] = None):
        self.company = company
        self.ticker = ticker
        self.message = message or f"Failed to match company {company} with ticker {ticker}"
        super().__init__(self.message)

class DatabaseError(TickerCleanerError):
    """Raised when database operations fail."""
    def __init__(self, operation: str, message: Optional[str] = None):
        self.operation = operation
        self.message = message or f"Database operation failed: {operation}"
        super().__init__(self.message)

class ConfigurationError(TickerCleanerError):
    """Raised when configuration is invalid or missing."""
    def __init__(self, config_item: str, message: Optional[str] = None):
        self.config_item = config_item
        self.message = message or f"Configuration error: {config_item}"
        super().__init__(self.message)
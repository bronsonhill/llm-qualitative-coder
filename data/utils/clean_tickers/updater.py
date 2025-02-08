"""Database operations for thesis updates."""
import logging
from sqlalchemy.orm import Session
from data.models.Thesis import Thesis

logger = logging.getLogger(__name__)

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
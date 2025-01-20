from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, Text, JSON, DateTime
)
import datetime
from db import Base

class Baseline(Base):
    __tablename__ = 'baseline'
    
    # Primary Key
    ticker = Column(String, primary_key=True)  # Stock ticker symbol
    
    # Attributes
    daily_performance = Column(JSON, nullable=True)  # List of date-price pairs
    start_date = Column(Date, nullable=True)  # Start date of the baseline
    created_at = Column(DateTime, default=datetime.utcnow)  # Record creation timestamp
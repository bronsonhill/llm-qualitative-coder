from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, Text, JSON, DateTime
)
import datetime
from db import Base

class Thesis(Base):
    __tablename__ = 'thesis'
    
    # Primary Key: Composite of date, author, and ticker
    date = Column(Date, primary_key=True)  # Date of publication
    author = Column(String, primary_key=True)  # Author name
    ticker = Column(String, primary_key=True)  # Stock ticker symbol
    
    # Additional Attributes
    company_name = Column(String, nullable=True)  # Company name
    link = Column(String, nullable=True)  # Link to the thesis
    market_cap = Column(Float, nullable=True)  # Market cap at the time of publication
    price = Column(Float, nullable=True)  # Stock price at the time of publication
    created_at = Column(DateTime, default=lambda: datetime.now(datetime.timezone.utc))  # Record creation timestamp

    # Text of thesis scraped from link
    text = Column(Text, nullable=True)  # Full text of the thesis
    
    # Yahoo finance data
    profile = Column(Text, nullable=True)  # Company profile data
    monthly_performance = Column(JSON, nullable=True)  # List of date-performance pairs
    dividends = Column(JSON, nullable=True)  # List of date-dividend pairs
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, Text, JSON, DateTime
)
from datetime import datetime, timezone
from data.db import Base
from sqlalchemy.orm import sessionmaker

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
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))  # Record creation timestamp

    # Text of thesis scraped from link
    text = Column(Text, nullable=True)  # Full text of the thesis
    
    # Yahoo finance data
    profile = Column(JSON, nullable=True)  # Company profile data
    daily_price = Column(JSON, nullable=True)  # List of date-performance pairs
    dividends = Column(JSON, nullable=True)  # List of date-dividend pairs

    # def update_yahoo_finance_data(self):
    #     prices_df, dividends_df, profile = get_yahoo_finance_data(self.ticker, self.date.strftime("%Y-%m-%d"))
    #     self.market_cap = profile.get("marketCap")
    #     self.profile = profile
    #     # Convert prices_df to a list of date-performance pairs
    #     self.monthly_performance = prices_df.reset_index().to_dict(orient='records')
    #     # Convert dividends_df to a list of date-dividend pairs
    #     self.dividends = dividends_df.reset_index().to_dict(orient='records')


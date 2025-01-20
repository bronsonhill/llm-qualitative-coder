# db.py

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database URL (SQLite in this example)
DATABASE_URL = "sqlite:///vic-project.db"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Base class for models
Base = declarative_base()

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
from db import Base, engine
from models import Baseline  # Import your models here

# Create all tables
Base.metadata.create_all(bind=engine)
print("Database tables created.")

# Run this script once to initialize the database.
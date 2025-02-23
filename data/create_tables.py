from data.db import Base, engine
from data.models import Baseline, Thesis

# Create all tables
Base.metadata.create_all(bind=engine)
print("Database tables created.")

# Run this script once to initialize the database.
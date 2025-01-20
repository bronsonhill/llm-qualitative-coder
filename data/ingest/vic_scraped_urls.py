import csv
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from data.db import engine, Thesis

# Create a session maker
Session = sessionmaker(bind=engine)
session = Session()

def ingest_vic_scraped_urls(file_path):
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            thesis = Thesis(
                date=datetime.strptime(row['idea_date'], '%A, %b %d, %Y'),
                author=row['author'],
                ticker=row['code'],
                company_name=row['company_name'],
                link=row['link'],
                market_cap=float(row['market_cap'].replace('$', '').replace('mn', '')) * 1e6,
                price=float(row['price']),
                text=row['excerpt']
            )
            session.add(thesis)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            print(f"Duplicate entry found for {row['code']} on {row['idea_date']}")

# Example usage
ingest_vic_scraped_urls('ideas_with_links_backup.csv')
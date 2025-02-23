import csv
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from data.db import engine
from data.models.Thesis import Thesis

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
                ticker=row['code'].split(' ')[-1],
                company_name=row['company_name'],
                link=row['link'],
                market_cap=float(row['market_cap'].replace('$', '').replace('mn', '').replace(',', '')) * 1e6,
                price=float(row['price'].replace(',', '')),
                text=None
            )
            print(f"Thesis(date={thesis.date}, author={thesis.author}, ticker={thesis.ticker}, company_name={thesis.company_name}, link={thesis.link}, market_cap={thesis.market_cap}, price={thesis.price}, text={thesis.text})")
            
            with session.no_autoflush:
                # Check if the entry already exists
                existing_thesis = session.query(Thesis).filter_by(date=thesis.date, author=thesis.author, ticker=thesis.ticker).first()
                if existing_thesis:
                    print(f"Duplicate entry found for {thesis.ticker} on {thesis.date}")
                    continue
                
                session.add(thesis)
                try:
                    session.commit()
                except IntegrityError as e:
                    print(f"IntegrityError: {e}")
                    session.rollback()

# Example usage
ingest_vic_scraped_urls('data/ideas_with_links_2021-2024_cleaned.csv')
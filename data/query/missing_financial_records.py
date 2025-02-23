import csv
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Thesis import Thesis

def get_entries_with_missing_daily_price():
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Query entries with null daily_price field
        missing_daily_price_entries = session.query(Thesis).filter(
            (Thesis.daily_price.is_(None)) | (Thesis.daily_price == '\"[]\"')
        ).all()
        return missing_daily_price_entries
    except Exception as e:
        print(f"Error retrieving entries with missing_daily_price data: {e}")
        return []
    finally:
        session.close()

def save_to_csv(entries, filename='missing_daily_price_entries.csv'):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Date', 'Author', 'Ticker', 'Company Name', 'Link', 'Market Cap', 'Price', 'Created At'])
        for entry in entries:
            print(f"Writing entry: {entry.date} {entry.author} {entry.ticker}")
            writer.writerow([
                entry.date,
                entry.author,
                entry.ticker,
                entry.company_name,
                entry.link,
                entry.market_cap,
                entry.price,
                entry.created_at
            ])

# Main execution for testing
if __name__ == "__main__":
    entries = get_entries_with_missing_daily_price()
    save_to_csv(entries)
    print(f"Saved {len(entries)} entries to missing_daily_price_entries.csv")


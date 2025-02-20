import datetime
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Baseline import Baseline

def pull_performance(ticker: str, start_date: str):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Parse the start date
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = start_date + datetime.timedelta(days=18*30)  # Approximate 18 months

        # Query the baseline entry
        baseline = session.query(Baseline).filter_by(ticker=ticker).first()
        if not baseline:
            print(f"No baseline data found for ticker: {ticker}")
            return
        
        # Filter the daily performance data
        performance_data = [
            record for record in baseline.daily_performance
            if start_date <= datetime.datetime.fromisoformat(record.index).date() <= end_date
        ]

        # Print the performance data
        for record in performance_data:
            print(record)

    except Exception as e:
        print(f"Error pulling performance data for {ticker}: {e}")
    finally:
        session.close()

# Main execution
if __name__ == "__main__":
    ticker = "^GSPC"  # Example ticker
    start_date = "2020-01-01"  # Example start date
    pull_performance(ticker, start_date)
    

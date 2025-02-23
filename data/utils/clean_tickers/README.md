# Ticker Cleaning Module

This module provides functionality to clean and update stock tickers in the database by validating them against Yahoo Finance data using GPT-powered matching.

## Overview

The ticker cleaner performs the following steps:
1. Retrieves thesis records with missing financial data from the database
2. Searches Yahoo Finance for each company name
3. Uses GPT to validate and select the best matching ticker
4. If no match is found using company name, tries searching with the current ticker
5. Updates the database with new tickers or marks companies as private

## Prerequisites

- Python 3.8+
- OpenAI API key set in `.env` file
- Required packages (install via `pip install -r requirements.txt`):
  - yfinance
  - openai
  - pandas
  - sqlalchemy
  - python-dotenv

## Usage

### Command Line Interface

The module can be run in two modes:

1. **CSV Output Mode** (safe mode - doesn't modify database):
```bash
python -m data.utils.clean_tickers.main --to-csv --batch-size 100 --limit 500
```

2. **Database Update Mode** (directly updates database):
```bash
python -m data.utils.clean_tickers.main --batch-size 100 --limit 500
```

3. **Apply CSV Updates** (apply previously generated updates):
```bash
python -m data.utils.clean_tickers.main --from-csv path/to/ticker_updates.csv
```

### Command Line Arguments

- `--to-csv`: Output results to CSV instead of updating database
- `--from-csv CSV_PATH`: Apply updates from a CSV file
- `--batch-size N`: Number of records to process in each database query (default: 100)
- `--limit N`: Maximum number of records to process in total (default: all records)

## Output

When running with `--to-csv`, the module generates a CSV file named `ticker_updates_YYYYMMDD_HHMMSS.csv` containing:
- Original ticker
- New ticker
- Company name
- Reasoning for the change
- Processing timestamp

## Best Practices

1. **Testing Changes**:
   - First run with `--to-csv` and a small `--limit` to review changes
   - Review the CSV output before applying to database

2. **Batch Processing**:
   - Use appropriate `--batch-size` to manage memory usage
   - For large datasets, process in smaller batches

3. **Validation**:
   - Review the reasoning provided for each change
   - Pay special attention to companies marked as "PRIVATE"

## Error Handling

- Failed searches or API calls are logged
- Database transactions are rolled back on error
- Processing continues even if individual records fail

## Example Workflow

1. Generate test output:
```bash
python -m data.utils.clean_tickers.main --to-csv --limit 10
```

2. Review the generated CSV file

3. Apply changes to database:
```bash
python -m data.utils.clean_tickers.main --from-csv ticker_updates_20240207_120000.csv
```

## Module Structure

- `YahooFinanceSearcher`: Handles Yahoo Finance API searches
- `GPTTickerMatcher`: Manages GPT-based ticker matching
- `ThesisUpdater`: Handles database updates
- `TickerCleaner`: Orchestrates the cleaning process

## Logging

The module logs all operations to console with timestamps and log levels. Important events like ticker updates and errors are clearly marked in the log output.
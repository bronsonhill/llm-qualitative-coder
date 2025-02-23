import csv
from datetime import datetime

def remove_duplicates(input_file, output_file):
    seen = set()
    unique_rows = []

    with open(input_file, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            identifier = (row['idea_date'], row['author'], row['code'])
            if identifier not in seen:
                seen.add(identifier)
                unique_rows.append(row)

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = ["company_name", "idea_date", "author", "link", "excerpt", "code", "price", "market_cap"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unique_rows)

# Example usage
remove_duplicates('data/ideas_with_links_2000-2008.csv', 'data/ideas_with_links_2000-2008_cleaned.csv')

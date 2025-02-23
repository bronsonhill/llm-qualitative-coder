import json
import pandas as pd

def convert_results_to_csv(json_file='classification_results.json', output_file='classification_results.csv'):
    # Load JSON data
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Create list to hold flattened data
    rows = []
    
    # Process each thesis
    for thesis_url, categories in data.items():
        for category, details in categories.items():
            if isinstance(details, dict):  # Skip empty categories
                # Extract value and justification directly from the response
                value = details.get('value', {})  # Get the boolean value
                justification = details.get('justification', '')
                
                rows.append({
                    'thesis_url': thesis_url,
                    'category': category,
                    'value': 1 if value else 0,  # Convert boolean to 1/0
                    'justification': justification
                })
    
    # Convert to DataFrame and save
    df = pd.DataFrame(rows)
    df.to_csv(output_file, index=False)
    print(f"Saved results to {output_file}")
    
    # Print summary stats
    print("\nSummary Statistics:")
    print(f"Total theses analyzed: {len(data)}")
    print(f"Total categories: {len(df['category'].unique())}")
    print("\nCategory distribution:")
    print(df.groupby('value')['category'].count())

if __name__ == "__main__":
    convert_results_to_csv()

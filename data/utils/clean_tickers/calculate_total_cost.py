import re

# Initialize total cost
cost_pattern = re.compile(r"Cost: \$(\d+\.\d+)")
total_cost = 0.0

# Read the log file
with open("/Users/home/Documents/GenAI Teaching Projects/Stock Screener/vic-project/data/utils/clean_tickers/token_usage.log", "r") as file:
    for line in file:
        match = cost_pattern.search(line)
        if match:
            total_cost += float(match.group(1))

print(f"Total cost: ${total_cost:.6f}")
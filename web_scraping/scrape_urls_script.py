import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import json
import csv



# Retry configuration
retry_strategy = Retry(
    total=1, 
    backoff_factor=2,  # Wait time between retries: 2^n seconds
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"], 
)

# Set up a session with retries
session = requests.Session()
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Base URL and headers
url = "https://valueinvestorsclub.com/ideas/loadideas"
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-AU,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://valueinvestorsclub.com",
    "Referer": "https://valueinvestorsclub.com/ideas",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15",
    "X-Requested-With": "XMLHttpRequest",
    "Cookie": "vic_session=eyJpdiI6IlZWVmZsdnJTSDVGRnppY242V1I2aEE9PSIsInZhbHVlIjoicGxvc1k3Tm1hcWtiQ0JKenllZEJqQjh1VUNsNURpa2JhbHVGak9jbnlwWStUbWNVdXdOeW1SWVVpYUxxNlBCYVNENVl2OEo1QlRpbmt4djZmaWhjRlFLMThtK2NqTW9FQlhBYWxVV1VpMGRRL2xZa09WQW5wakNOckVPUTZnN3YiLCJtYWMiOiIxMTcxNGRlZjkyODUwZmM2YmM0NTA2NTI0YzBjZjgyODAyNDAyMjgwZTY5MjQ2NDc5NTg3ZjRjODc2YWM3YzRhIiwidGFnIjoiIn0%3D"  # Replace with your session cookie
}

# Payload template
payload = {
    "show": "all",
    "daterange": "6",
    "ls": "all",
    "loc": "all",
    "sort": "new",
    "marketcap_l": "0",
    "marketcap_h": "",
    "rtr_l": "",
    "rtr_h": "",
    "country": "",
    "state": "",
    "aum": "",
    "yio": "",
    "gotodate": "10/09/2024",  # Adjust date as needed
    "page": 1,
    "end_page": 1,
    "is_login": 0,
}

# Base URL for idea links
idea_base_url = "https://valueinvestorsclub.com/idea/"

# List to store extracted data
ideas = []

# Fetch pages until no more data
while True:
    print(f"Fetching page {payload['page']}...")
    try:
        response = session.post(url, headers=headers, data=payload)
        print(f"Response: {response}")
        response.raise_for_status()
        data = response.json()

        if not data.get("result"):
            print("No more data to fetch!")
            break

        for date, ideas_list in data["result"].items():
            for idea in ideas_list:
                idea_url = f"{idea_base_url}{idea['encode_company_name']}/{idea['keyid']}"
                save_data = {
                    "date": date,
                    "company_name": idea.get("company_name"),
                    "symbol": idea.get("symbol"),
                    "market_cap": idea.get("market_cap"),
                    "price": idea.get("price"),
                    "rating": idea.get("rating"),
                    "votes": idea.get("rating_num_user_votes"),
                    "author": idea.get("display_name"),
                    "description": idea.get("description"),
                    "link": idea_url
                }

                ideas.append(save_data)
                print(f"Added: {save_data['company_name']}")
                print(f"Data: {save_data}")

        # Increment the page
        payload["page"] += 1
        payload["end_page"] = payload["page"]
        time.sleep(5)

    except requests.exceptions.Timeout:
        print(f"Timeout occurred on page {payload['page']}. Retrying...")
        continue
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        break
    if response.status_code != 200:
        print(f"Request failed with status {response.status_code}")
        break


# Save results to a CSV file
with open("ideas_with_links.csv", "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["date", "company_name", "symbol", "market_cap", "price", "rating", "votes", "author", "description", "link"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(ideas)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import csv

# Configure Selenium WebDriver
def create_driver():
    options = Options()
    # Uncomment for debugging to see the browser in action
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Scrape ideas from the page
def scrape_ideas(driver, url):
    print(f"Scraping ideas from: {url}")
    driver.get(url)

    # Add session cookie for authentication (if needed)
    driver.add_cookie({
        "name": "vic_session",
        "value": "eyJpdiI6IlBJK2pLM3FOand6RVJBaHFHWG9oK0E9PSIsInZhbHVlIjoiZ3oyenFkUjJ3blFTUHpQdWgrbittVFdIVmJDaWcwUVhXcE01L3RFVVFCa00yNjJTcU96U3FSd2owR0wzd1lXb2FCejFhM3RXWm9lTG1nUXJvWlp5eFg3cmRDYWlHKzNUQk9LR00zakVTeFFIcm9FY3hpV29nRDJZeXAzUEcrbzIiLCJtYWMiOiJkYjQ5ZmE0ZTM2MjM0NDVmYTdhODc1NTExYTg2OTk1MTE5NWRkMjBlNjY0NTI2Nzg0YmQ2NjhjZWM3MTg3OTE1IiwidGFnIjoiIn0%3D",  # Replace with the actual session cookie
        "domain": "valueinvestorsclub.com"
    })
    driver.refresh()  # Reload the page with the cookie
    time.sleep(6)  # Allow time for the page to load

    ideas = []
    idea_date = None  # Initialize variable to store the most recent date
    initial_row_count = 0  # Track the initial number of rows
    while True:
        try:
            # Locate all rows containing ideas
            rows = driver.find_elements(By.CSS_SELECTOR, "#ideas_body .row")
            # Only process the newly loaded rows
            rows = rows[initial_row_count:]

            for row in rows:
                try:
                    # Check if the row contains a date
                    date_element = row.find_elements(By.CSS_SELECTOR, ".header")
                    if date_element:
                        idea_date = date_element[0].text.strip()  # Update the idea_date
                        initial_row_count += 1
                        continue  # Skip to the next row since this row is a date row

                    # Extract the idea details
                    company_name = row.find_element(By.CSS_SELECTOR, ".entry-header a").text
                    author = row.find_element(By.CSS_SELECTOR, ".submitted-by span[title]").text
                    link = row.find_element(By.CSS_SELECTOR, ".entry-header a").get_attribute("href")
                    excerpt = row.find_element(By.CSS_SELECTOR, ".excerpt").text.strip()

                    # Extract additional details from the header
                    header_info = row.find_element(By.CSS_SELECTOR, ".entry-header").text.split(" • ")
                    code = header_info[0]
                    price = header_info[1]
                    market_cap = header_info[2]

                    # Append idea details to the list
                    ideas.append({
                        "company_name": company_name,
                        "idea_date": idea_date,  # Use the most recent date
                        "author": author,
                        "link": link,
                        "excerpt": excerpt,
                        "code": code,
                        "price": price,
                        "market_cap": market_cap
                    })

                    print(f"Added: {company_name}\n{ideas[-1]}")
                    initial_row_count += 1

                except Exception as e:
                    # Log and skip rows that don't match the structure
                    continue

            # Scroll to the "Load More" button and click
            try:
                load_more_button = driver.find_element(By.CSS_SELECTOR, ".load-more.load_more_ideas")
                load_more_button.click()
                time.sleep(6)  # Allow time for new content to load

                

            except:
                print("No more pages to load.")
                break

        except Exception as e:
            print(f"Error encountered: {e}")
            break

    return ideas

# Save data to CSV
def save_to_csv(ideas, filename="ideas_with_links.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["company_name", "idea_date", "author", "link", "excerpt", "code", "price", "market_cap"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ideas)

# Main execution
if __name__ == "__main__":
    URL = "https://valueinvestorsclub.com/ideas"
    
    driver = create_driver()
    try:
        ideas = scrape_ideas(driver, URL)
        save_to_csv(ideas)
        print(f"Scraped {len(ideas)} ideas and saved to CSV.")
    finally:
        driver.quit()
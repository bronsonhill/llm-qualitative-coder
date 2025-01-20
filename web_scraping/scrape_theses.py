from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Thesis import Thesis
import time

FAILURE_THRESHOLD = 2  # Maximum number of consecutive failures allowed

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

# Scrape text from a given URL
def scrape_text(driver, url):
    driver.get(url)
    time.sleep(20)  # Allow time for the page to load
    try:
        text_element = driver.find_element(By.ID, "description")
        return text_element.text.strip()
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

# Update records with scraped text
def update_thesis_texts(batch_size=100):
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    driver = create_driver()
    consecutive_failures = 0  # Counter for consecutive failures
    try:
        while True:
            # Query a batch of records with null text field
            theses = session.query(Thesis).filter(Thesis.text.is_(None)).limit(batch_size).all()
            if not theses:
                break  # Exit loop if no more records to process

            for thesis in theses:
                print(f"Scraping text for: {thesis.link}")
                text = scrape_text(driver, thesis.link)
                
                if text:
                    thesis.text = text
                    session.commit()
                    print(f"Updated text for: {thesis.link}")
                    consecutive_failures = 0  # Reset counter on success
                else:
                    consecutive_failures += 1
                    if consecutive_failures > FAILURE_THRESHOLD:
                        print("Too many consecutive failures, terminating script.")
                        time.sleep(600)  # Add a delay to avoid being blocked
                        return
                    time.sleep(360)  # Add a delay to avoid being blocked
                
                time.sleep(20) # Add a delay to avoid being blocked
    finally:
        driver.quit()
        session.close()

# Main execution
if __name__ == "__main__":
    update_thesis_texts(batch_size=100)


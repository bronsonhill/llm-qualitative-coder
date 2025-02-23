from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy.orm import sessionmaker
from data.db import engine
from data.models.Thesis import Thesis
import time
import requests

FAILURE_THRESHOLD = 2  # Maximum number of consecutive failures allowed
SLEEP_TIME = 10  # Time to sleep between requests
LOAD_TIMEOUT = 30  # Page load timeout in seconds

USERNAME = 'spfc3ysi11'
PASSWORD = 'X0CqErxT~1nxtwzk33'

scrape_count = 0

proxies = [
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10001",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10002",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10003",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10004",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10005",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10006",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10007",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10008",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10009",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10010",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10011",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10012",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10013",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10014",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10015",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10016",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10017",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10018",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10019",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10020",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10021",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10022",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10023",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10024",
    f"https://{USERNAME}:{PASSWORD}@gate.smartproxy.com:10025"
]

def create_driver(proxy=None):
    seleniumwire_options = {
        'proxy': {
            'http': proxy,
            'https': proxy,
            'no_proxy': 'localhost,127.0.0.1',  # Bypass localhost
        }
    }
    options = Options()
    # Uncomment for headless mode
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")

    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options, seleniumwire_options=seleniumwire_options)


proxy_index = 0  # Start with the first proxy
driver = None  # Global variable for the driver

def create_driver_with_rotation():
    global proxy_index, driver, scrape_count
    proxy = proxies[proxy_index % len(proxies)]  # Rotate through proxies
    print(f"Using proxy: {proxy}")
    driver = create_driver(proxy=proxy)  # Pass the proxy to the driver

# Scrape text from a given URL
def scrape_text(url):
    global proxy_index, driver, scrape_count
    max_retries = len(proxies)
    retries = 0
    while retries < max_retries:
        if driver is None or scrape_count % 10 or retries == 2:
            if driver:
                driver.quit()
            create_driver_with_rotation()
            driver.set_page_load_timeout(LOAD_TIMEOUT)
            retries = 0
        print(f"Scraping text for: {url}")
        try:
            driver.get(url)
            scrape_count += 1
            time.sleep(SLEEP_TIME)  # Allow time for the page to load and avoid being blocked
            try:
                text_element = driver.find_element(By.ID, "description")
                return text_element.text.strip()
            except Exception as e:
                print(f"Error scraping {url}: {e}")
                return None
        except Exception as e:
            print(f"Proxy failed for {url}: {e}")
            retries += 1
            proxy_index += 1  # Increment index for the next request
    print(f"All proxies failed for {url}")
    return None

# Update records with scraped text
def update_thesis_texts(batch_size=100):
    global driver
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    consecutive_failures = 0  # Counter for consecutive failures
    try:
        while True:
            # Query a batch of records with null text field
            theses = session.query(Thesis).filter(Thesis.text.is_(None)).limit(batch_size).all()
            if not theses:
                break  # Exit loop if no more records to process

            for thesis in theses:
                text = scrape_text(thesis.link)  # Rotate proxy on each request
                
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
    finally:
        if driver:
            driver.quit()  # Close the driver when done
        session.close()

# def test_proxies():
#     for proxy in proxies:
#         try:
#             url = 'https://ip.smartproxy.com/json'
#             username = 'spraw43a51'
#             password = 'rgm8vRk61Wvbgo0AT+'
#             proxy = f"http://{username}:{password}@gate.smartproxy.com:10001"
#             response = requests.get(url, proxies = {
#                 'http': proxy,
#                 'https': proxy
#             })
#             print(response.text)
#             print(f"Proxy {proxy} is working. Response: {response.json()}")
#         except Exception as e:
#             print(f"Proxy {proxy} failed: {e}")

# test_proxies()
# Main execution
if __name__ == "__main__":
    update_thesis_texts(batch_size=100)

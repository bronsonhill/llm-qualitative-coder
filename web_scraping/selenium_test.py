from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def create_driver():
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

if __name__ == "__main__":
    driver = create_driver()
    try:
        driver.get("https://www.google.com")
        print("Page Title:", driver.title)
    finally:
        driver.quit()
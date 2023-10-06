from selenium import webdriver
from selenium.webdriver.chrome.service import Service

def init(driver_path: str, headless: bool, detach: bool) -> webdriver.Chrome:
    service = Service(executable_path=driver_path)
    chrome_options = webdriver.ChromeOptions()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option("detach", detach) # to keep driver "alive"

    chrome_options.add_argument('--no-sandbox')

    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import random
import re
import concurrent.futures
import threading
import os
import zipfile
from tqdm import tqdm

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper_usa_multithread.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Try importing fake_useragent, fall back to default user agent if missing
try:
    from fake_useragent import UserAgent
    FAKE_USER_AGENT_AVAILABLE = True
except ImportError:
    FAKE_USER_AGENT_AVAILABLE = False
    logger.warning("fake-useragent not installed. Using default user agent.")

# Full list of U.S. state abbreviations
states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

base_url = "https://www.cars.com/dealers/buy/"

def extract_email(text):
    """Extract email addresses from text using regex."""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    return re.findall(email_pattern, text)

def save_state_dealers(state, dealers, start_index, end_index):
    """Save dealers to a CSV in the state-specific folder."""
    if not dealers:
        logger.warning(f"No dealers to save for state {state} at index {start_index} to {end_index}")
        return
    
    state_folder = os.path.join(os.getcwd(), state)
    os.makedirs(state_folder, exist_ok=True)
    
    filename = f"{state_folder}/{state}-{start_index}to{end_index}.csv"
    df = pd.DataFrame(dealers)
    df.to_csv(filename, index=False, encoding='utf-8')
    logger.info(f"Saved {len(dealers)} dealers to {filename} (Thread: {threading.current_thread().name})")
    return filename

def zip_state_files(state):
    """Zip all CSVs for a state."""
    state_folder = os.path.join(os.getcwd(), state)
    zip_filename = f"{state}.zip"
    try:
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(state_folder):
                for file in files:
                    if file.endswith('.csv'):
                        zipf.write(os.path.join(root, file), os.path.join(state, file))
        logger.info(f"Created zip file {zip_filename} for state {state}")
    except Exception as e:
        logger.error(f"Error creating zip for state {state}: {e}")

def initialize_driver():
    """Initialize WebDriver with optimized settings."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-images")  # Disable images for faster loading
    options.add_argument("--disable-gpu")  # Disable GPU for headless mode
    options.add_argument("--log-level=3")  # Reduce Chrome logging
    if FAKE_USER_AGENT_AVAILABLE:
        ua = UserAgent()
        options.add_argument(f'--user-agent={ua.random}')
    else:
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    max_retries = 3
    for attempt in range(max_retries):
        try:
            driver = webdriver.Chrome(options=options)
            logger.info(f"WebDriver initialized successfully (Thread: {threading.current_thread().name})")
            return driver
        except Exception as e:
            logger.error(f"WebDriver initialization attempt {attempt+1}/{max_retries} failed: {e} (Thread: {threading.current_thread().name})")
            time.sleep(1)
    logger.error(f"Failed to initialize WebDriver after {max_retries} attempts (Thread: {threading.current_thread().name})")
    return None

def scrape_inventory_page(driver, inventory_url, max_retries=3):
    """Attempt to scrape inventory page with retry logic."""
    for attempt in range(max_retries):
        try:
            driver.get(inventory_url)
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            inventory_soup = BeautifulSoup(driver.page_source, "html.parser")
            return inventory_soup
        except Exception as e:
            logger.error(f"Inventory page {inventory_url} attempt {attempt+1}/{max_retries} failed: {e} (Thread: {threading.current_thread().name})")
            time.sleep(1)
    logger.error(f"Failed to scrape inventory page {inventory_url} after {max_retries} attempts (Thread: {threading.current_thread().name})")
    return None

def scrape_dealer(driver, dealer, state, page):
    """Scrape a single dealer's data."""
    try:
        name = dealer.select_one(".dealer-heading").text.strip() if dealer.select_one(".dealer-heading") else None
        address = dealer.select_one(".dealer-address").text.strip() if dealer.select_one(".dealer-address") else None
        phones = [p.text.strip() for p in dealer.select(".phone-number")] if dealer.select(".phone-number") else []
        inventory = dealer.select_one("a.inventory-badge-link")["href"] if dealer.select_one("a.inventory-badge-link") else None

        contact_last_name = None
        email = None

        if inventory:
            inventory_url = f"https://www.cars.com{inventory}"
            inventory_soup = scrape_inventory_page(driver, inventory_url)
            if inventory_soup:
                try:
                    contact_elements = inventory_soup.select("p, div, span")
                    for element in contact_elements:
                        text = element.text.strip().lower()
                        if "contact" in text and len(text.split()) >= 2:
                            potential_name = text.split("contact")[-1].strip().split()
                            if potential_name:
                                contact_last_name = potential_name[-1].capitalize()
                                break

                    page_text = inventory_soup.get_text()
                    emails = extract_email(page_text)
                    email = emails[0] if emails else None
                except Exception as e:
                    logger.error(f"Error parsing inventory page {inventory_url}: {e} (Thread: {threading.current_thread().name})")

        dealer_data = {
            "Business Name": name,
            "Contact Last Name": contact_last_name,
            "Phone": ", ".join(phones) if phones else None,
            "Email": email,
            "Address": address,
            "State": state,
            "Inventory URL": inventory_url if inventory else None,
            "Page": page
        }
        logger.info(f"Scraped: {name}, State: {state}, Contact: {contact_last_name}, Email: {email} (Thread: {threading.current_thread().name})")
        return dealer_data
    except Exception as e:
        logger.error(f"Error on dealer card, page {page}, state {state}: {e} (Thread: {threading.current_thread().name})")
        return None

def scrape_state(state, pages_range=(1, 11)):
    """Function to scrape dealers for a single state with progress bar."""
    driver = initialize_driver()
    if not driver:
        logger.error(f"Skipping state {state} due to WebDriver initialization failure")
        return []
    
    local_dealers = []
    record_count = 0
    
    with tqdm(total=pages_range[1]-pages_range[0], desc=f"Scraping {state}", unit="page") as pbar:
        for page in range(*pages_range):
            url = f"{base_url}?page={page}&state={state}"
            logger.info(f"Scraping: {url} (Thread: {threading.current_thread().name})")
            
            try:
                driver.get(url)
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".dealer-card-content")))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                dealer_cards = soup.select(".dealer-card-content")

                logger.info(f"Found {len(dealer_cards)} dealers on page {page} for state {state} (Thread: {threading.current_thread().name})")
                if not dealer_cards:
                    logger.warning(f"No dealers on page {page} for state {state}")
                    pbar.update(1)
                    continue

                page_dealers = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as dealer_executor:
                    futures = [dealer_executor.submit(scrape_dealer, driver, dealer, state, page) for dealer in dealer_cards]
                    for future in concurrent.futures.as_completed(futures):
                        dealer_data = future.result()
                        if dealer_data:
                            page_dealers.append(dealer_data)
                            local_dealers.append(dealer_data)
                            record_count += 1

                if page_dealers:
                    start_index = record_count - len(page_dealers) + 1
                    end_index = record_count
                    save_state_dealers(state, page_dealers, start_index, end_index)

                time.sleep(random.uniform(1, 3))
                pbar.update(1)

            except Exception as e:
                logger.error(f"Page {page} for state {state} failed: {e} (Thread: {threading.current_thread().name})")
                time.sleep(3)
                pbar.update(1)
                continue
    
    if local_dealers and len(local_dealers) % 10 != 0:
        start_index = (len(local_dealers) // 10) * 10 + 1
        end_index = len(local_dealers)
        save_state_dealers(state, local_dealers[start_index-1:], start_index, end_index)
    
    zip_state_files(state)
    
    try:
        driver.quit()
        logger.info(f"WebDriver closed for state {state} (Thread: {threading.current_thread().name})")
    except Exception as e:
        logger.error(f"Error closing WebDriver for state {state}: {e}")
    
    return local_dealers

if __name__ == "__main__":
    max_threads = 8  # Increased from 5; test up to 10 if stable
    with tqdm(total=len(states), desc="Overall Progress", unit="state") as overall_pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_state = {}
            for state in states:
                future_to_state[executor.submit(scrape_state, state)] = state
                time.sleep(0.2)
            for future in concurrent.futures.as_completed(future_to_state):
                state = future_to_state[future]
                try:
                    state_dealers = future.result()
                    logger.info(f"Completed scraping for state {state} with {len(state_dealers)} dealers")
                except Exception as e:
                    logger.error(f"Error processing state {state}: {e}")
                overall_pbar.update(1)

    logger.info("Scraping completed for all states")
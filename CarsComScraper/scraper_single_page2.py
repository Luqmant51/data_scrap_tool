import time
import csv
import os
import threading
import json
from queue import Queue
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from datetime import datetime

# ----------------------
# CONFIG
# ----------------------
THREAD_COUNT = 10  # concurrent threads
MAX_RETRIES = 3   # retry attempts per ZIP

ZIP_FILE = input("Enter path to ZIP JSON file (e.g., zipcode/AK.json): ").strip()

# ----------------------
# DRIVER INIT (headless)
# ----------------------
def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--use-gl=desktop")   # Use desktop OpenGL
    options.add_argument("--enable-gpu")       # Force GPU
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_window_size(1920, 1080)
    return driver

# ----------------------
# SCRAPER FUNCTION
# ----------------------
def scrape_zip(zip_code, state_abbr, progress_lock, report_list, progress_bar):
    start_time = time.time()
    url = f"https://www.cars.com/dealers/buy/?page=1&page_size=200&zip={zip_code}"

    for attempt in range(1, MAX_RETRIES + 1):
        driver = None
        try:
            driver = get_driver()
            driver.get(url)
            time.sleep(5)

            all_dealers = []
            dealers = driver.find_elements(By.CSS_SELECTOR, "div.sds-container.dealer-card")

            for dealer in dealers:
                try:
                    name = dealer.find_element(By.CSS_SELECTOR, "h2.dealer-heading").text.strip()
                except:
                    name = "N/A"
                try:
                    address = dealer.find_element(By.CSS_SELECTOR, "div.dealer-address a.sds-link--ext").text.strip()
                except:
                    address = "N/A"
                phones = []
                try:
                    phone_elements = dealer.find_elements(By.CSS_SELECTOR, "a.phone-number, .desktop-phone-number")
                    for p in phone_elements:
                        ph = p.text.strip()
                        if ph and ph not in phones:
                            phones.append(ph)
                except:
                    pass
                phone_str = "; ".join(phones) if phones else "N/A"
                all_dealers.append([name, phone_str, address])

            # Save CSV
            folder_path = f"USA/{state_abbr}"
            os.makedirs(folder_path, exist_ok=True)
            output_file = os.path.join(folder_path, f"{state_abbr}_{zip_code}.csv")
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Business Name", "Phone(s)", "Address"])
                writer.writerows(all_dealers)

            elapsed = time.time() - start_time
            with progress_lock:
                completed_zipcodes.append(zip_code)
                report_list.append({
                    "zip": zip_code,
                    "records": len(all_dealers),
                    "file": output_file,
                    "time_sec": round(elapsed, 2),
                    "status": "success",
                    "attempts": attempt
                })
                remaining = zip_queue.qsize()
                print(f"[{zip_code}] ✅ {len(all_dealers)} records | Time: {round(elapsed,2)}s | Remaining: {remaining}")
                progress_bar.update(1)

            return  # ✅ success → exit function

        except Exception as e:
            if attempt == MAX_RETRIES:
                elapsed = time.time() - start_time
                with progress_lock:
                    report_list.append({
                        "zip": zip_code,
                        "records": 0,
                        "file": "ERROR",
                        "time_sec": round(elapsed, 2),
                        "status": f"failed ({e})",
                        "attempts": attempt
                    })
                    print(f"[{zip_code}] ❌ Error after {attempt} attempts: {e}")
                    progress_bar.update(1)
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

# ----------------------
# THREAD WORKER
# ----------------------
def scrape_worker(queue, progress_lock, state_abbr, report_list, progress_bar):
    while not queue.empty():
        zip_code = queue.get()
        try:
            zip_code = zip_code.strip()
            if zip_code:
                scrape_zip(zip_code, state_abbr, progress_lock, report_list, progress_bar)
        finally:
            queue.task_done()

# ----------------------
# MAIN
# ----------------------
if __name__ == "__main__":
    if not os.path.isfile(ZIP_FILE):
        print(f"File not found: {ZIP_FILE}")
        exit()

    with open(ZIP_FILE, "r", encoding="utf-8") as f:
        zip_json = json.load(f)

    if len(zip_json) != 1:
        print("JSON must contain exactly one state abbreviation as key")
        exit()

    state_abbr = list(zip_json.keys())[0]
    zip_codes = zip_json[state_abbr]

    zip_queue = Queue()
    for z in zip_codes:
        zip_queue.put(z)

    total_zipcodes = zip_queue.qsize()
    completed_zipcodes = []
    progress_lock = threading.Lock()
    report_list = []

    start_overall = time.time()

    progress_bar = tqdm(total=total_zipcodes, desc=f"Scraping {state_abbr}", ncols=100)

    threads = []
    for _ in range(min(THREAD_COUNT, zip_queue.qsize())):
        t = threading.Thread(target=scrape_worker, args=(zip_queue, progress_lock, state_abbr, report_list, progress_bar))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    progress_bar.close()

    overall_elapsed = time.time() - start_overall

    # Save report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"{state_abbr}_scrape_report_{timestamp}.csv"
    with open(report_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["zip", "records", "file", "time_sec", "status", "attempts"])
        writer.writeheader()
        for row in report_list:
            writer.writerow(row)
        writer.writerow({
            "zip": "TOTAL",
            "records": sum(r['records'] for r in report_list),
            "file": "-",
            "time_sec": round(overall_elapsed, 2),
            "status": "completed",
            "attempts": "-"
        })

    print(f"\n🎉 Scraping complete! Report saved → {report_file}")



# import time
# import csv
# import os
# import threading
# import json
# from queue import Queue
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
# from tqdm import tqdm
#
# # ----------------------
# # CONFIG
# # ----------------------
# THREAD_COUNT = 2  # concurrent threads
#
# # User selects JSON file
# ZIP_FILE = input("Enter path to ZIP JSON file (e.g., zipcode/AK.json): ").strip()
#
# # Map state names to abbreviation (for folder and filename)
# STATE_ABBR = {
#     'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
#     'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA',
#     'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
#     'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
#     'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
#     'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new-hampshire': 'NH',
#     'new-jersey': 'NJ', 'new-mexico': 'NM', 'new-york': 'NY', 'north-carolina': 'NC',
#     'north-dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA',
#     'rhode-island': 'RI', 'south-carolina': 'SC', 'south-dakota': 'SD', 'tennessee': 'TN',
#     'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
#     'west-virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY'
# }
#
# # ----------------------
# # DRIVER INIT (headless)
# # ----------------------
# def get_driver():
#     options = webdriver.ChromeOptions()
#     options.add_argument("--headless=new")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")
#     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
#     driver.set_window_size(1920, 1080)
#     return driver
#
# # ----------------------
# # SCRAPER FUNCTION
# # ----------------------
# def scrape_zip(zip_code, state_abbr, progress_lock, report_list, progress_bar):
#     start_time = time.time()
#     url = f"https://www.cars.com/dealers/buy/?page=1&page_size=200&zip={zip_code}"
#     try:
#         driver = get_driver()
#         driver.get(url)
#         time.sleep(5)
#
#         all_dealers = []
#         dealers = driver.find_elements(By.CSS_SELECTOR, "div.sds-container.dealer-card")
#
#         for dealer in dealers:
#             try:
#                 name = dealer.find_element(By.CSS_SELECTOR, "h2.dealer-heading").text.strip()
#             except:
#                 name = "N/A"
#             try:
#                 address = dealer.find_element(By.CSS_SELECTOR, "div.dealer-address a.sds-link--ext").text.strip()
#             except:
#                 address = "N/A"
#             phones = []
#             try:
#                 phone_elements = dealer.find_elements(By.CSS_SELECTOR, "a.phone-number, .desktop-phone-number")
#                 for p in phone_elements:
#                     ph = p.text.strip()
#                     if ph and ph not in phones:
#                         phones.append(ph)
#             except:
#                 pass
#             phone_str = "; ".join(phones) if phones else "N/A"
#             all_dealers.append([name, phone_str, address])
#
#         driver.quit()
#
#         # Save CSV
#         folder_path = f"USA/{state_abbr}"
#         os.makedirs(folder_path, exist_ok=True)
#         output_file = os.path.join(folder_path, f"{state_abbr}_{zip_code}.csv")
#         with open(output_file, "w", newline="", encoding="utf-8") as f:
#             writer = csv.writer(f)
#             writer.writerow(["Business Name", "Phone(s)", "Address"])
#             writer.writerows(all_dealers)
#
#         elapsed = time.time() - start_time
#         with progress_lock:
#             completed_zipcodes.append(zip_code)
#             report_list.append({
#                 "zip": zip_code,
#                 "records": len(all_dealers),
#                 "file": output_file,
#                 "time_sec": round(elapsed, 2)
#             })
#             remaining = zip_queue.qsize()
#             print(f"[{zip_code}] ✅ {len(all_dealers)} records | Time: {round(elapsed,2)}s | Remaining: {remaining}")
#             progress_bar.update(1)
#
#     except Exception as e:
#         with progress_lock:
#             print(f"[{zip_code}] ❌ Error: {e}")
#             progress_bar.update(1)
#     finally:
#         if 'driver' in locals():
#             try:
#                 driver.quit()
#             except:
#                 pass
#
# # ----------------------
# # THREAD WORKER
# # ----------------------
# def scrape_worker(queue, progress_lock, state_abbr, report_list, progress_bar):
#     while not queue.empty():
#         zip_code = queue.get()
#         try:
#             zip_code = zip_code.strip()
#             if zip_code:
#                 scrape_zip(zip_code, state_abbr, progress_lock, report_list, progress_bar)
#         finally:
#             queue.task_done()
#
# # ----------------------
# # MAIN
# # ----------------------
# if __name__ == "__main__":
#     if not os.path.isfile(ZIP_FILE):
#         print(f"File not found: {ZIP_FILE}")
#         exit()
#
#     # Load ZIP codes from JSON
#     with open(ZIP_FILE, "r", encoding="utf-8") as f:
#         zip_json = json.load(f)
#
#     if len(zip_json) != 1:
#         print("JSON must contain exactly one state abbreviation as key")
#         exit()
#
#     state_abbr = list(zip_json.keys())[0]
#     zip_codes = zip_json[state_abbr]
#
#     zip_queue = Queue()
#     for z in zip_codes:
#         zip_queue.put(z)
#
#     total_zipcodes = zip_queue.qsize()
#     completed_zipcodes = []
#     progress_lock = threading.Lock()
#     report_list = []
#
#     start_overall = time.time()
#
#     # Initialize progress bar
#     progress_bar = tqdm(total=total_zipcodes, desc="Scraping ZIPs", ncols=100)
#
#     # Start threads
#     threads = []
#     for _ in range(min(THREAD_COUNT, zip_queue.qsize())):
#         t = threading.Thread(target=scrape_worker, args=(zip_queue, progress_lock, state_abbr, report_list, progress_bar))
#         t.start()
#         threads.append(t)
#
#     # Wait for threads to finish
#     for t in threads:
#         t.join()
#
#     progress_bar.close()
#
#     overall_elapsed = time.time() - start_overall
#
#     # Save report
#     report_file = f"{state_abbr}_scrape_report.csv"
#     with open(report_file, "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=["zip", "records", "file", "time_sec"])
#         writer.writeheader()
#         for row in report_list:
#             writer.writerow(row)
#         # Overall summary
#         writer.writerow({"zip": "TOTAL", "records": sum(r['records'] for r in report_list),
#                          "file": "-", "time_sec": round(overall_elapsed,2)})
#
#     print(f"\n🎉 Scraping complete! Report saved → {report_file}")

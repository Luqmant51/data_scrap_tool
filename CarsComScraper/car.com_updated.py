import time
import csv
import os
import threading
import json
from queue import Queue
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from datetime import datetime
import multiprocessing

# ----------------------
# CONFIG
# ----------------------
THREAD_COUNT = 5  # Reduced to avoid contention
MAX_RETRIES = 2  # Reduced retries
ZIP_FOLDER = r"zipcode"
BATCH_SIZE = 20  # Increased batch size
HEADLESS = True  # True for headless, False to show browser
WAIT_TIMEOUT = 3  # Reduced wait time (seconds)

# ----------------------
# DRIVER INIT
# ----------------------
def get_driver(headless: bool = False):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")  # Enable headless mode if True
    options.add_argument("--disable-gpu")
    options.add_argument("--enable-accelerated-video-decode")
    options.add_argument("--enable-accelerated-mjpeg-decode")
    options.add_argument("--enable-accelerated-2d-canvas")
    options.add_argument("--ignore-gpu-blocklist")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=webdriver.chrome.service.Service(ChromeDriverManager().install()), options=options)
    driver.set_window_size(1920, 1080)
    return driver

# ----------------------
# CHECK EXISTING CSV
# ----------------------
def get_processed_zips(state_abbr):
    """Return set of ZIP codes already in the state's CSV file."""
    file_path = os.path.join(f"USA/{state_abbr}", f"{state_abbr}_dealers.csv")
    processed_zips = set()
    if not os.path.exists(file_path):
        return processed_zips
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if row and len(row) > 0:
                    processed_zips.add(row[0])  # ZIP is first column
        return processed_zips
    except:
        return processed_zips

# ----------------------
# SCRAPER FUNCTION
# ----------------------
def scrape_zip_batch(zip_codes, state_abbr, driver, progress_lock, report_list, progress_bar, all_data):
    batch_data = []
    start_time = time.time()

    for zip_code in zip_codes:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                url = f"https://www.cars.com/dealers/buy/?page=1&page_size=200&zip={zip_code}"
                driver.get(url)
                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.sds-container.dealer-card"))
                )

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
                        phones = [p.text.strip() for p in phone_elements if p.text.strip()]
                    except:
                        pass
                    phone_str = "; ".join(set(phones)) if phones else "N/A"
                    batch_data.append([zip_code, name, phone_str, address])

                elapsed = time.time() - start_time
                with progress_lock:
                    completed_zipcodes.append(zip_code)
                    report_list.append({
                        "zip": zip_code,
                        "records": len(dealers),
                        "file": f"USA/{state_abbr}/{state_abbr}_dealers.csv",
                        "time_sec": round(elapsed, 2),
                        "status": "success",
                        "attempts": attempt
                    })
                    remaining = zip_queue.qsize()
                    print(f"[{zip_code}] ✅ {len(dealers)} records | Time: {round(elapsed,2)}s | Remaining: {remaining}")
                    progress_bar.update(1)
                break

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

    with progress_lock:
        all_data.extend(batch_data)
    return batch_data

# ----------------------
# THREAD WORKER
# ----------------------
def scrape_worker(queue, progress_lock, state_abbr, report_list, progress_bar, all_data):
    driver = get_driver()
    try:
        while not queue.empty():
            batch = []
            for _ in range(min(BATCH_SIZE, queue.qsize())):
                try:
                    batch.append(queue.get_nowait())
                except Queue.Empty:
                    break

            if batch:
                scrape_zip_batch(batch, state_abbr, driver, progress_lock, report_list, progress_bar, all_data)
    finally:
        driver.quit()

# ----------------------
# MAIN
# ----------------------
if __name__ == "__main__":
    if not os.path.isdir(ZIP_FOLDER):
        print(f"ZIP folder not found: {ZIP_FOLDER}")
        exit()

    json_files = [f for f in os.listdir(ZIP_FOLDER) if f.endswith(".json")]

    for json_file in json_files:
        json_path = os.path.join(ZIP_FOLDER, json_file)
        with open(json_path, "r", encoding="utf-8") as f:
            zip_json = json.load(f)

        if len(zip_json) != 1:
            print(f"⚠️ Skipping {json_file} (invalid format)")
            continue

        state_abbr = list(zip_json.keys())[0]
        zip_codes = zip_json[state_abbr]

        print(f"\n🚀 Starting state: {state_abbr} ({len(zip_codes)} ZIPs)")

        # Check for processed ZIPs
        processed_zips = get_processed_zips(state_abbr)
        zip_queue = Queue()
        skipped_zips = []
        for z in zip_codes:
            zip_code = z.strip()
            if zip_code in processed_zips:
                skipped_zips.append(zip_code)
                print(f"[{zip_code}] ⏭️ Skipped (already processed)")
            else:
                zip_queue.put(zip_code)

        if skipped_zips:
            print(f"⏭️ Skipped {len(skipped_zips)} ZIP codes (already processed)")

        total_zipcodes = zip_queue.qsize()
        if total_zipcodes == 0:
            print(f"🎉 {state_abbr} complete! All ZIP codes already processed.")
            continue

        completed_zipcodes = []
        progress_lock = threading.Lock()
        report_list = []
        all_data = []  # Store all dealer data in memory

        start_overall = time.time()

        progress_bar = tqdm(total=total_zipcodes, desc=f"Scraping {state_abbr}", ncols=100)

        threads = []
        for _ in range(min(THREAD_COUNT, zip_queue.qsize())):
            t = threading.Thread(target=scrape_worker, args=(zip_queue, progress_lock, state_abbr, report_list, progress_bar, all_data))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        progress_bar.close()

        # Write all data to a single CSV file
        if all_data:
            folder_path = f"USA/{state_abbr}"
            os.makedirs(folder_path, exist_ok=True)
            output_file = os.path.join(folder_path, f"{state_abbr}_dealers.csv")
            with open(output_file, "a", newline="", encoding="utf-8") as f:  # Append mode
                writer = csv.writer(f)
                # Write header if file is new
                if os.path.getsize(output_file) == 0 if os.path.exists(output_file) else True:
                    writer.writerow(["Zip", "Business Name", "Phone(s)", "Address"])
                writer.writerows(all_data)

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
                "file": f"USA/{state_abbr}/{state_abbr}_dealers.csv",
                "time_sec": round(overall_elapsed, 2),
                "status": "completed",
                "attempts": "-"
            })

        print(f"🎉 {state_abbr} complete! Data saved to {output_file}, Report saved to {report_file}")


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
# from datetime import datetime
#
# # ----------------------
# # CONFIG
# # ----------------------
# THREAD_COUNT = 9  # concurrent threads
# MAX_RETRIES = 3   # retry attempts per ZIP
#
# ZIP_FOLDER = r"zipcode"   # folder containing all state JSONs
#
# # ----------------------
# # DRIVER INIT (headless)
# # ----------------------
# def get_driver():
#     options = webdriver.ChromeOptions()
#     # options.add_argument("--headless=new")   # ❌ comment this to see browser
#     options.add_argument("--disable-gpu")
#     options.add_argument("--enable-accelerated-video-decode")
#     options.add_argument("--enable-accelerated-mjpeg-decode")
#     options.add_argument("--enable-accelerated-2d-canvas")
#     options.add_argument("--ignore-gpu-blocklist")
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
#
#     for attempt in range(1, MAX_RETRIES + 1):
#         driver = None
#         try:
#             driver = get_driver()
#             driver.get(url)
#             time.sleep(5)
#
#             all_dealers = []
#             dealers = driver.find_elements(By.CSS_SELECTOR, "div.sds-container.dealer-card")
#
#             for dealer in dealers:
#                 try:
#                     name = dealer.find_element(By.CSS_SELECTOR, "h2.dealer-heading").text.strip()
#                 except:
#                     name = "N/A"
#                 try:
#                     address = dealer.find_element(By.CSS_SELECTOR, "div.dealer-address a.sds-link--ext").text.strip()
#                 except:
#                     address = "N/A"
#                 phones = []
#                 try:
#                     phone_elements = dealer.find_elements(By.CSS_SELECTOR, "a.phone-number, .desktop-phone-number")
#                     for p in phone_elements:
#                         ph = p.text.strip()
#                         if ph and ph not in phones:
#                             phones.append(ph)
#                 except:
#                     pass
#                 phone_str = "; ".join(phones) if phones else "N/A"
#                 all_dealers.append([name, phone_str, address])
#
#             # Save CSV
#             folder_path = f"USA/{state_abbr}"
#             os.makedirs(folder_path, exist_ok=True)
#             output_file = os.path.join(folder_path, f"{state_abbr}_{zip_code}.csv")
#             with open(output_file, "w", newline="", encoding="utf-8") as f:
#                 writer = csv.writer(f)
#                 writer.writerow(["Business Name", "Phone(s)", "Address"])
#                 writer.writerows(all_dealers)
#
#             elapsed = time.time() - start_time
#             with progress_lock:
#                 completed_zipcodes.append(zip_code)
#                 report_list.append({
#                     "zip": zip_code,
#                     "records": len(all_dealers),
#                     "file": output_file,
#                     "time_sec": round(elapsed, 2),
#                     "status": "success",
#                     "attempts": attempt
#                 })
#                 remaining = zip_queue.qsize()
#                 print(f"[{zip_code}] ✅ {len(all_dealers)} records | Time: {round(elapsed,2)}s | Remaining: {remaining}")
#                 progress_bar.update(1)
#
#             return  # ✅ success → exit function
#
#         except Exception as e:
#             if attempt == MAX_RETRIES:
#                 elapsed = time.time() - start_time
#                 with progress_lock:
#                     report_list.append({
#                         "zip": zip_code,
#                         "records": 0,
#                         "file": "ERROR",
#                         "time_sec": round(elapsed, 2),
#                         "status": f"failed ({e})",
#                         "attempts": attempt
#                     })
#                     print(f"[{zip_code}] ❌ Error after {attempt} attempts: {e}")
#                     progress_bar.update(1)
#         finally:
#             if driver:
#                 try:
#                     driver.quit()
#                 except:
#                     pass
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
#     if not os.path.isdir(ZIP_FOLDER):
#         print(f"ZIP folder not found: {ZIP_FOLDER}")
#         exit()
#
#     # Loop through all state JSON files
#     json_files = [f for f in os.listdir(ZIP_FOLDER) if f.endswith(".json")]
#
#     for json_file in json_files:
#         json_path = os.path.join(ZIP_FOLDER, json_file)
#
#         with open(json_path, "r", encoding="utf-8") as f:
#             zip_json = json.load(f)
#
#         if len(zip_json) != 1:
#             print(f"⚠️ Skipping {json_file} (invalid format)")
#             continue
#
#         state_abbr = list(zip_json.keys())[0]
#         zip_codes = zip_json[state_abbr]
#
#         print(f"\n🚀 Starting state: {state_abbr} ({len(zip_codes)} ZIPs)")
#
#         zip_queue = Queue()
#         for z in zip_codes:
#             zip_queue.put(z)
#
#         total_zipcodes = zip_queue.qsize()
#         completed_zipcodes = []
#         progress_lock = threading.Lock()
#         report_list = []
#
#         start_overall = time.time()
#
#         progress_bar = tqdm(total=total_zipcodes, desc=f"Scraping {state_abbr}", ncols=100)
#
#         threads = []
#         for _ in range(min(THREAD_COUNT, zip_queue.qsize())):
#             t = threading.Thread(target=scrape_worker, args=(zip_queue, progress_lock, state_abbr, report_list, progress_bar))
#             t.start()
#             threads.append(t)
#
#         for t in threads:
#             t.join()
#
#         progress_bar.close()
#
#         overall_elapsed = time.time() - start_overall
#
#         # Save report per state
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         report_file = f"{state_abbr}_scrape_report_{timestamp}.csv"
#         with open(report_file, "w", newline="", encoding="utf-8") as f:
#             writer = csv.DictWriter(f, fieldnames=["zip", "records", "file", "time_sec", "status", "attempts"])
#             writer.writeheader()
#             for row in report_list:
#                 writer.writerow(row)
#             writer.writerow({
#                 "zip": "TOTAL",
#                 "records": sum(r['records'] for r in report_list),
#                 "file": "-",
#                 "time_sec": round(overall_elapsed, 2),
#                 "status": "completed",
#                 "attempts": "-"
#             })
#
#         print(f"🎉 {state_abbr} complete! Report saved → {report_file}")

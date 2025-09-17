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
THREAD_COUNT = 8  # concurrent threads
MAX_RETRIES = 3   # retry attempts per ZIP
BATCH_SIZE = 8    # write 6 CSVs at once

ZIP_FOLDER = r"zipcode"   # folder containing all state JSONs

# ----------------------
# DRIVER INIT (headless)
# ----------------------
def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")   # ❌ comment this to see browser
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_window_size(1920, 1080)
    return driver

# ----------------------
# CHECK EXISTING CSV
# ----------------------
def get_processed_zips(state_abbr):
    """Return set of ZIP codes with existing, non-empty CSV files in the state's directory."""
    folder_path = f"USA/{state_abbr}"
    processed_zips = set()
    if not os.path.exists(folder_path):
        return processed_zips
    for file_name in os.listdir(folder_path):
        if file_name.startswith(f"{state_abbr}_") and file_name.endswith(".csv"):
            zip_code = file_name[len(state_abbr) + 1:-4]  # Extract ZIP
            try:
                if os.path.getsize(os.path.join(folder_path, file_name)) > 0:
                    processed_zips.add(zip_code)
            except:
                pass
    return processed_zips

# ----------------------
# BATCH HANDLING
# ----------------------
batch_results = {}   # state_abbr → list of (zip, dealers)
batch_lock = threading.Lock()

def flush_batch_to_csv(state_abbr):
    """Write all ZIPs in batch to individual CSV files."""
    global batch_results
    if not batch_results[state_abbr]:
        return

    folder_path = f"USA/{state_abbr}"
    os.makedirs(folder_path, exist_ok=True)

    for zip_code, dealers in batch_results[state_abbr]:
        output_file = os.path.join(folder_path, f"{state_abbr}_{zip_code}.csv")
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Business Name", "Phone(s)", "Address"])
            writer.writerows(dealers)
        print(f"📝 Wrote → {output_file} ({len(dealers)} records)")

    batch_results[state_abbr] = []  # reset batch

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

            elapsed = time.time() - start_time
            with progress_lock:
                report_list.append({
                    "zip": zip_code,
                    "records": len(all_dealers),
                    "file": f"USA/{state_abbr}/{state_abbr}_{zip_code}.csv",
                    "time_sec": round(elapsed, 2),
                    "status": "success",
                    "attempts": attempt
                })
                completed_zipcodes.append(zip_code)
                progress_bar.update(1)

            # Add to batch
            with batch_lock:
                batch_results[state_abbr].append((zip_code, all_dealers))
                if len(batch_results[state_abbr]) >= BATCH_SIZE:
                    flush_batch_to_csv(state_abbr)

            return  # ✅ success

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
            if zip_code.strip():
                scrape_zip(zip_code.strip(), state_abbr, progress_lock, report_list, progress_bar)
        finally:
            queue.task_done()

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

        processed_zips = get_processed_zips(state_abbr)
        zip_queue = Queue()
        for z in zip_codes:
            if z.strip() not in processed_zips:
                zip_queue.put(z.strip())
            else:
                print(f"[{z}] ⏭️ Skipped (already processed)")

        total_zipcodes = zip_queue.qsize()
        if total_zipcodes == 0:
            print(f"🎉 {state_abbr} complete! All ZIP codes already processed.")
            continue

        completed_zipcodes = []
        progress_lock = threading.Lock()
        report_list = []

        batch_results[state_abbr] = []  # init batch

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

        # Flush leftovers (<6 ZIPs)
        with batch_lock:
            flush_batch_to_csv(state_abbr)

        overall_elapsed = time.time() - start_overall

        # Save report per state
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
                "file": f"USA/{state_abbr}/*.csv",
                "time_sec": round(overall_elapsed, 2),
                "status": "completed",
                "attempts": "-"
            })

        print(f"🎉 {state_abbr} complete! Report saved → {report_file}")


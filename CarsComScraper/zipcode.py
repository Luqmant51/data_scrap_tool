import requests
from bs4 import BeautifulSoup
import json
import csv
import time
from urllib.parse import urljoin
import os
import re

# Map state names to abbreviations
STATE_ABBR = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA',
    'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA',
    'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA',
    'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new-hampshire': 'NH',
    'new-jersey': 'NJ', 'new-mexico': 'NM', 'new-york': 'NY', 'north-carolina': 'NC',
    'north-dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA',
    'rhode-island': 'RI', 'south-carolina': 'SC', 'south-dakota': 'SD', 'tennessee': 'TN',
    'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
    'west-virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY'
}

class USZipCodeScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.base_url = "https://worldpopulationreview.com"
        self.zip_data = {}

    def get_state_list(self):
        return list(STATE_ABBR.keys())

    def scrape_zip_codes_method1(self):
        """Scrape from worldpopulationreview.com"""
        for state in self.get_state_list():
            try:
                print(f"Scraping zip codes for {state.title()}...")
                url = f"{self.base_url}/us-cities/{state}"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                zip_codes = []

                # Extract zip codes from tables
                for table in soup.find_all('table'):
                    for row in table.find_all('tr'):
                        for cell in row.find_all(['td', 'th']):
                            text = cell.get_text().strip()
                            zip_matches = re.findall(r'\b\d{5}\b', text)
                            zip_codes.extend(zip_matches)

                self.zip_data[state] = sorted(list(set(zip_codes)))
                print(f"Found {len(zip_codes)} zip codes for {state.title()}")
                time.sleep(1)
            except Exception as e:
                print(f"Error scraping {state}: {e}")
                self.zip_data[state] = []

    def get_zip_codes_from_usps_format(self):
        """Generate all zip codes from USPS ranges"""
        zip_ranges = {
            'alabama': range(35000, 36999),
            'alaska': range(99500, 99999),
            'arizona': range(85000, 86599),
            'arkansas': range(71600, 72999),
            'california': list(range(90000, 96199)) + list(range(93200, 93599)),
            'colorado': range(80000, 81699),
            'connecticut': list(range(6001, 6389)) + list(range(6401, 6928)),
            'delaware': range(19700, 19999),
            'district_of_columbia': range(20001, 20600),
            'florida': range(32000, 35000),
            'georgia': range(30000, 32000),
            'hawaii': range(96800, 96999),
            'idaho': range(83700, 83999),
            'illinois': range(60000, 62999),
            'indiana': range(46000, 47999),
            'iowa': range(50000, 52999),
            'kansas': range(66000, 67999),
            'kentucky': range(40000, 42799),
            'louisiana': range(70000, 71499),
            'maine': range(3900, 4999),
            'maryland': range(20000, 21999),
            'massachusetts': range(10000, 27999),
            'michigan': range(48000, 49999),
            'minnesota': range(55000, 56799),
            'mississippi': range(39200, 39999),
            'missouri': range(63000, 65899),
            'montana': range(59000, 59999),
            'nebraska': range(68100, 69399),
            'nevada': range(89500, 89999),
            'new_hampshire': range(33000, 34999),
            'new_jersey': range(7000, 8999),
            'new_mexico': range(87500, 87999),
            'new_york': range(10000, 14999),
            'north_carolina': range(27000, 28999),
            'north_dakota': range(58100, 58999),
            'ohio': range(43000, 45999),
            'oklahoma': range(73000, 74999),
            'oregon': range(97000, 97999),
            'pennsylvania': range(15000, 19699),
            'rhode_island': range(28000, 29999),
            'south_carolina': range(29000, 29999),
            'south_dakota': range(57000, 57799),
            'tennessee': range(37000, 38599),
            'texas': list(range(73300, 73400)) + list(range(75000, 79999)),
            'utah': range(84000, 84799),
            'vermont': range(5000, 5999),
            'virginia': range(20100, 24699),
            'washington': range(98000, 99499),
            'west_virginia': range(24700, 26899),
            'wisconsin': range(53000, 54999),
            'wyoming': range(82000, 83100)
        }

        for state, zip_range in zip_ranges.items():
            self.zip_data[state] = [str(i).zfill(5) for i in zip_range]

    def save_to_json_per_state_abbr(self, folder="zipcode"):
        """Save ZIP codes per state using state abbreviation as JSON object"""
        os.makedirs(folder, exist_ok=True)
        for state, zip_codes in self.zip_data.items():
            abbr = STATE_ABBR.get(state.lower(), state[:2].upper())
            file_path = os.path.join(folder, f"{abbr}.json")
            data = {abbr: zip_codes}  # JSON object with state abbreviation as key
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"✅ Saved {len(zip_codes)} ZIP codes for {abbr} → {file_path}")

def main():
    scraper = USZipCodeScraper()
    print("Choose method:")
    print("1. Web scraping")
    print("2. Generate ALL zip codes from USPS ranges")

    choice = input("Enter choice (1 or 2): ").strip()
    if choice == '1':
        scraper.scrape_zip_codes_method1()
    else:
        scraper.get_zip_codes_from_usps_format()

    scraper.save_to_json_per_state_abbr(folder="zipcode")

if __name__ == "__main__":
    main()


# import requests
# from bs4 import BeautifulSoup
# import json
# import csv
# import time
# from urllib.parse import urljoin
#
#
# class USZipCodeScraper:
#     def __init__(self):
#         self.session = requests.Session()
#         self.session.headers.update({
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
#         })
#         self.base_url = "https://worldpopulationreview.com"
#         self.zip_data = {}
#
#     def get_state_list(self):
#         """Get list of all US states"""
#         states = [
#             'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado',
#             'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho',
#             'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana',
#             'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota',
#             'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
#             'new-hampshire', 'new-jersey', 'new-mexico', 'new-york',
#             'north-carolina', 'north-dakota', 'ohio', 'oklahoma', 'oregon',
#             'pennsylvania', 'rhode-island', 'south-carolina', 'south-dakota',
#             'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
#             'west-virginia', 'wisconsin', 'wyoming'
#         ]
#         return states
#
#     def scrape_zip_codes_method1(self):
#         """Method 1: Scrape from worldpopulationreview.com"""
#         states = self.get_state_list()
#
#         for state in states:
#             try:
#                 print(f"Scraping zip codes for {state.title()}...")
#                 url = f"{self.base_url}/us-cities/{state}"
#
#                 response = self.session.get(url, timeout=10)
#                 response.raise_for_status()
#
#                 soup = BeautifulSoup(response.content, 'html.parser')
#
#                 # Look for zip code patterns in the page
#                 zip_codes = []
#
#                 # Method: Find tables with city data that might contain zip codes
#                 tables = soup.find_all('table')
#                 for table in tables:
#                     rows = table.find_all('tr')
#                     for row in rows:
#                         cells = row.find_all(['td', 'th'])
#                         for cell in cells:
#                             text = cell.get_text().strip()
#                             # Look for 5-digit zip codes
#                             import re
#                             zip_matches = re.findall(r'\b\d{5}\b', text)
#                             zip_codes.extend(zip_matches)
#
#                 # Remove duplicates
#                 zip_codes = list(set(zip_codes))
#                 self.zip_data[state] = zip_codes
#
#                 print(f"Found {len(zip_codes)} zip codes for {state.title()}")
#                 time.sleep(1)  # Be respectful to the server
#
#             except Exception as e:
#                 print(f"Error scraping {state}: {str(e)}")
#                 self.zip_data[state] = []
#
#     def scrape_zip_codes_method2(self):
#         """Method 2: Alternative scraping from a different source"""
#         # You can implement alternative sources here
#         print("Alternative method not implemented in this example")
#
#     def use_api_method(self):
#         """Method 3: Use free APIs for zip code data"""
#         try:
#             # Example using a hypothetical free API
#             # Note: You'll need to find actual free APIs for zip codes
#             print("API method - you'll need to implement with actual API endpoints")
#
#             # Example structure:
#             # api_url = "https://api.example.com/zipcodes"
#             # response = self.session.get(api_url)
#             # data = response.json()
#             # Process the data...
#
#         except Exception as e:
#             print(f"API method error: {str(e)}")
#
#     def save_to_json(self, filename="us_zip_codes.json"):
#         """Save scraped data to JSON file"""
#         with open(filename, 'w') as f:
#             json.dump(self.zip_data, f, indent=2)
#         print(f"Data saved to {filename}")
#
#     def save_to_csv(self, filename="us_zip_codes.csv"):
#         """Save scraped data to CSV with 20 ZIP codes per row"""
#         with open(filename, 'w', newline='', encoding='utf-8') as f:
#             writer = csv.writer(f)
#             writer.writerow(['State', 'Zip Codes'])  # header
#
#             for state, zip_codes in self.zip_data.items():
#                 # Split zip_codes into chunks of 20
#                 for i in range(0, len(zip_codes), 20):
#                     chunk = zip_codes[i:i + 20]
#                     writer.writerow([state.title(), ", ".join(chunk)])
#
#         print(f"Data saved to {filename}")
#     def get_zip_codes_from_usps_format(self):
#         """Generate all zip codes from USPS ranges"""
#         zip_ranges = {
#             'alabama': range(35000, 36999),
#             'alaska': range(99500, 99999),
#             'arizona': range(85000, 86599),
#             'arkansas': range(71600, 72999),
#             'california': list(range(90000, 96199)) + list(range(93200, 93599)),
#             'colorado': range(80000, 81699),
#             'connecticut': list(range(6001, 6389)) + list(range(6401, 6928)),
#             'delaware': range(19700, 19999),
#             'district_of_columbia': range(20001, 20600),
#             'florida': range(32000, 35000),
#             'georgia': range(30000, 32000),
#             'hawaii': range(96800, 96999),
#             'idaho': range(83700, 83999),
#             'illinois': range(60000, 62999),
#             'indiana': range(46000, 47999),
#             'iowa': range(50000, 52999),
#             'kansas': range(66000, 67999),
#             'kentucky': range(40000, 42799),
#             'louisiana': range(70000, 71499),
#             'maine': range(3900, 4999),
#             'maryland': range(20000, 21999),
#             'massachusetts': range(10000, 27999),
#             'michigan': range(48000, 49999),
#             'minnesota': range(55000, 56799),
#             'mississippi': range(39200, 39999),
#             'missouri': range(63000, 65899),
#             'montana': range(59000, 59999),
#             'nebraska': range(68100, 69399),
#             'nevada': range(89500, 89999),
#             'new_hampshire': range(33000, 34999),
#             'new_jersey': range(7000, 8999),
#             'new_mexico': range(87500, 87999),
#             'new_york': range(10000, 14999),
#             'north_carolina': range(27000, 28999),
#             'north_dakota': range(58100, 58999),
#             'ohio': range(43000, 45999),
#             'oklahoma': range(73000, 74999),
#             'oregon': range(97000, 97999),
#             'pennsylvania': range(15000, 19699),
#             'rhode_island': range(28000, 29999),
#             'south_carolina': range(29000, 29999),
#             'south_dakota': range(57000, 57799),
#             'tennessee': range(37000, 38599),
#             'texas': list(range(73300, 73400)) + list(range(75000, 79999)),
#             'utah': range(84000, 84799),
#             'vermont': range(5000, 5999),
#             'virginia': range(20100, 24699),
#             'washington': range(98000, 99499),
#             'west_virginia': range(24700, 26899),
#             'wisconsin': range(53000, 54999),
#             'wyoming': range(82000, 83100)
#         }
#
#         for state, zip_range in zip_ranges.items():
#             possible_zips = [str(i).zfill(5) for i in zip_range]
#             self.zip_data[state] = possible_zips
#
#
# def main():
#     scraper = USZipCodeScraper()
#
#     print("Starting US Zip Code scraping...")
#     print("Choose method:")
#     print("1. Web scraping (may be limited by site structure)")
#     print("2. Generate ALL zip codes from USPS ranges (LARGE file)")
#     print("3. Generate sample zip codes (smaller file for testing)")
#
#     choice = input("Enter choice (1, 2, or 3): ").strip()
#
#     if choice == '1':
#         scraper.scrape_zip_codes_method1()
#     elif choice == '2':
#         scraper.get_zip_codes_from_usps_format()
#     elif choice == '3':
#         scraper.get_sample_zip_codes()
#     else:
#         print("Invalid choice. Using method 3 (sample)...")
#         scraper.get_sample_zip_codes()
#
#     # Save results
#     scraper.save_to_json()
#     scraper.save_to_csv()
#
#     # Display summary
#     total_zips = sum(len(zips) for zips in scraper.zip_data.values())
#     states_processed = len(scraper.zip_data)
#
#     print(f"\nProcessing complete!")
#     print(f"States processed: {states_processed}")
#     print(f"Total zip codes: {total_zips}")
#     print(f"Average per state: {total_zips / states_processed if states_processed > 0 else 0:.0f}")
#
#     # Show sample data structure
#     print(f"\nSample data structure:")
#     for state_abbr, zip_codes in list(scraper.zip_data.items())[:3]:
#         print(f"{state_abbr}: {zip_codes[:5]}... ({len(zip_codes)} total)")
#
#     print(f"\nFiles saved:")
#     print(f"- us_zip_codes.json (JSON format with state abbreviations as keys)")
#     print(f"- us_zip_codes.csv (CSV format)")
#
#
# if __name__ == "__main__":
#     # Required packages
#     required_packages = ['requests', 'beautifulsoup4']
#
#     print("Required packages:")
#     for package in required_packages:
#         print(f"pip install {package}")
#     print()
#
#     main()
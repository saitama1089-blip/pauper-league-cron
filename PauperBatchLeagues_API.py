import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urlparse
import requests
import json

# ==========================
# Supabase configuration
# ==========================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    raise ValueError("Missing required environment variables: SUPABASE_URL and/or SUPABASE_ANON_KEY")

SUPABASE_INSERT_ENDPOINT = f"{SUPABASE_URL}/rest/v1/pauper_league_results_insert"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# ==========================
# Selenium setup with GitHub Actions compatibility
# ==========================
def setup_driver():
    """Setup Chrome driver with options suitable for GitHub Actions"""
    options = Options()
    options.add_argument("--headless=new")  # Updated headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # Disable unnecessary features
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_experimental_option('prefs', {
        'profile.default_content_setting_values': {
            'images': 2,  # Don't load images for faster scraping
        }
    })
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        print(f"ERROR: Failed to setup Chrome driver: {e}")
        raise

# ==========================
# Helper: extract numeric deck ID from URL
# ==========================
def get_deck_id(deck_url: str):
    """
    Given a URL like:
      https://www.mtggoldfish.com/deck/6863434#online
    return 6863434 as an int.
    """
    try:
        parsed = urlparse(deck_url)
        path_last = parsed.path.rstrip("/").split("/")[-1]
        if path_last.isdigit():
            return int(path_last)
        else:
            print(f"WARNING: could not extract numeric id from URL: {deck_url}")
            return None
    except Exception as e:
        print(f"ERROR: Exception parsing deck URL {deck_url}: {e}")
        return None

# ==========================
# Main scraping logic
# ==========================
def main():
    driver = None
    
    try:
        driver = setup_driver()
        print("Chrome driver initialized successfully")
        
        # Date range to scrape
        today = datetime.today()
        start_date = today - timedelta(days=6)   # last 7 days including today
        end_date = today
        
        current_date = start_date
        total_rows_inserted = 0
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            url = f"https://www.mtggoldfish.com/tournament/pauper-league-{date_str}#online"
            print(f"\n{'='*60}")
            print(f"Scraping URL: {url}")
            print(f"{'='*60}")
            
            try:
                driver.get(url)
                time.sleep(2)  # Brief pause to ensure page loads
                
                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
                
                table = soup.find("table", class_="table-tournament")
                
                if table:
                    tbody = table.find("tbody")
                    if tbody:
                        rows = tbody.find_all("tr")
                        print(f"Found {len(rows)} rows in table")
                        
                        # Collect rows to send in a single POST to Supabase
                        payload = []
                        
                        for row in rows:
                            cols = row.find_all("td")
                            
                            # Expecting columns: place | deck | pilot | ...
                            if len(cols) >= 3:
                                place = cols[0].text.strip()
                                deck_name = cols[1].text.strip()
                                pilot_name = cols[2].text.strip()
                                
                                # Build full deck URL
                                a_tag = cols[1].find("a")
                                if not a_tag or "href" not in a_tag.attrs:
                                    print(f"Skipping row: no deck link found for {deck_name}")
                                    continue
                                
                                deck_url = "https://www.mtggoldfish.com" + a_tag["href"]
                                deck_id = get_deck_id(deck_url)
                                
                                if deck_id is None:
                                    continue
                                
                                row_data = {
                                    "id": deck_id,
                                    "event_date": date_str,
                                    "place": place,
                                    "deck_name": deck_name,
                                    "pilot": pilot_name,
                                    "deck_url": deck_url
                                }
                                
                                payload.append(row_data)
                            else:
                                print(f"Skipping row: Not enough columns ({len(cols)})")
                        
                        if payload:
                            try:
                                print(f"Attempting to insert {len(payload)} rows into Supabase...")
                                resp = requests.post(
                                    SUPABASE_INSERT_ENDPOINT,
                                    headers=SUPABASE_HEADERS,
                                    data=json.dumps(payload),
                                    timeout=30
                                )
                                
                                if resp.status_code in (200, 201, 204):
                                    print(f"✓ Supabase insert OK for {date_str} ({len(payload)} rows)")
                                    total_rows_inserted += len(payload)
                                else:
                                    print(f"✗ Supabase insert FAILED for {date_str}.")
                                    print(f"  Status: {resp.status_code}")
                                    print(f"  Body: {resp.text[:500]}")
                            except requests.exceptions.RequestException as e:
                                print(f"✗ Network error inserting into Supabase for {date_str}: {e}")
                            except Exception as e:
                                print(f"✗ Unexpected error inserting into Supabase for {date_str}: {e}")
                        else:
                            print(f"No valid rows to insert for {date_str}")
                    else:
                        print(f"Tbody not found for {date_str} (tournament may not exist)")
                else:
                    print(f"Table not found for {date_str} (tournament may not exist)")
                    
            except TimeoutException:
                print(f"✗ Timeout loading page for {date_str}")
            except WebDriverException as e:
                print(f"✗ WebDriver error for {date_str}: {e}")
            except Exception as e:
                print(f"✗ Unexpected error scraping {date_str}: {e}")
            
            # Rate limiting - be nice to the server
            time.sleep(1)
            current_date += timedelta(days=1)
        
        print(f"\n{'='*60}")
        print(f"Scraping complete! Total rows inserted: {total_rows_inserted}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
                print("Chrome driver closed successfully")
            except Exception as e:
                print(f"Error closing driver: {e}")

if __name__ == "__main__":
    main()

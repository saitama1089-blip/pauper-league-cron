import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urlparse
import requests
import json

# ==========================
# Supabase configuration
# ==========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
SUPABASE_INSERT_ENDPOINT = f"{SUPABASE_URL}/rest/v1/pauper_league_results_insert"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"  # we don't need the whole row back
}

# ==========================
# Selenium setup
# ==========================
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")  # newer headless mode
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# If running in CI (GitHub), we’ll pass CHROME_BINARY in env
chrome_binary = os.environ.get("CHROME_BINARY")
if chrome_binary:
    options.binary_location = chrome_binary

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)


# ==========================
# Helper: extract numeric deck ID from URL
# ==========================
def get_deck_id(deck_url: str):
    """
    Given a URL like:
      https://www.mtggoldfish.com/deck/6863434#online
    return 6863434 as an int.
    """
    parsed = urlparse(deck_url)
    path_last = parsed.path.rstrip("/").split("/")[-1]  # '6863434'
    if path_last.isdigit():
        return int(path_last)
    else:
        print(f"WARNING: could not extract numeric id from URL: {deck_url}")
        return None

# ==========================
# Date range to scrape
# ==========================
today = datetime.today()
start_date = today - timedelta(days=6)   # last 7 days including today
end_date = today

current_date = start_date

while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    url = f"https://www.mtggoldfish.com/tournament/pauper-league-{date_str}#online"
    print("Scraping URL:", url)

    driver.get(url)
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", class_="table-tournament")

    if table:
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")

            # Collect rows to send in a single POST to Supabase
            payload = []

            for row in rows:
                cols = row.find_all("td")

                # Expecting columns: place | deck | pilot | ...
                if len(cols) >= 4:
                    place = cols[0].text.strip()
                    deck_name = cols[1].text.strip()
                    pilot_name = cols[2].text.strip()

                    # Build full deck URL
                    a_tag = cols[1].find("a")
                    if not a_tag or "href" not in a_tag.attrs:
                        print("Skipping row: no deck link found")
                        continue

                    deck_url = "https://www.mtggoldfish.com" + a_tag["href"]
                    deck_id = get_deck_id(deck_url)

                    if deck_id is None:
                        # If we can't extract ID, skip this row to avoid bad data
                        continue

                    # This dict matches your Supabase view columns
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
                    # Send all rows for this date in a single POST
                    resp = requests.post(
                        SUPABASE_INSERT_ENDPOINT,
                        headers=SUPABASE_HEADERS,
                        data=json.dumps(payload),
                        timeout=30
                    )
                    if resp.status_code in (200, 201, 204):
                        print(f"Supabase insert OK for {date_str} "
                              f"({len(payload)} rows).")
                    else:
                        print(f"Supabase insert FAILED for {date_str}. "
                              f"Status: {resp.status_code}, Body: {resp.text}")
                except Exception as e:
                    print(f"Error inserting into Supabase for {date_str}: {e}")
            else:
                print(f"No rows to insert for {date_str}.")
        else:
            print(f"Tbody not found for {date_str}!")
    else:
        print(f"Table not found for {date_str}!")

    current_date += timedelta(days=1)

driver.quit()

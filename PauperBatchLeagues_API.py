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

# League insert view endpoint
SUPABASE_LEAGUE_INSERT_ENDPOINT = f"{SUPABASE_URL}/rest/v1/pauper_league_results_insert"

# Challenge insert view endpoint (this is the new one)
SUPABASE_CHALLENGE_INSERT_ENDPOINT = f"{SUPABASE_URL}/rest/v1/challenge_deck_results_insert"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# ==========================
# Challenge URL templates
# ==========================
TOURNAMENT_URL_TEMPLATES = [
    "https://www.mtggoldfish.com/tournament/pauper-challenge-32-{date}#online",
    "https://www.mtggoldfish.com/tournament/pauper-challenge-32-special-{date}#online",
    "https://www.mtggoldfish.com/tournament/pauper-showcase-challenge-{date}#online",
]

# Rolling window for challenges (defaults to 15 days)
CHALLENGE_LOOKBACK_DAYS = int(os.environ.get("CHALLENGE_LOOKBACK_DAYS", "15"))

# ==========================
# Selenium setup (GitHub Actions friendly)
# ==========================
def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    options.page_load_strategy = "eager"

    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_experimental_option("prefs", {
        "profile.default_content_setting_values": {"images": 2}
    })

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(45)
    driver.set_script_timeout(45)
    return driver

# ==========================
# Helper: extract numeric deck ID from URL
# ==========================
def get_deck_id(deck_url: str):
    try:
        parsed = urlparse(deck_url)
        path_last = parsed.path.rstrip("/").split("/")[-1]
        if path_last.isdigit():
            return int(path_last)
        print(f"WARNING: could not extract numeric id from URL: {deck_url}")
        return None
    except Exception as e:
        print(f"ERROR: Exception parsing deck URL {deck_url}: {e}")
        return None

# ==========================
# Helper: robust driver.get with retries
# ==========================
def load_page_with_retries(driver, url: str, max_retries: int = 2, sleep_after: float = 2.0):
    retry_count = 0
    while retry_count < max_retries:
        try:
            driver.get(url)
            time.sleep(sleep_after)
            return True
        except TimeoutException:
            retry_count += 1
            if retry_count < max_retries:
                print(f"⚠ Timeout on attempt {retry_count}, retrying...")
                try:
                    driver.execute_script("window.stop();")
                except Exception:
                    pass
                time.sleep(1)
            else:
                print(f"✗ Page failed to load after {max_retries} attempts: {url}")
                return False
    return False

# ==========================
# Challenge scraping helper
# ==========================
def scrape_challenge_for_date(driver, date_str: str):
    """
    Try all Challenge URL templates for a given date.
    Returns a payload list (records) or [].
    """
    for template in TOURNAMENT_URL_TEMPLATES:
        url = template.format(date=date_str)
        print(f"Trying Challenge URL: {url}")

        ok = load_page_with_retries(driver, url, max_retries=2, sleep_after=2.0)
        if not ok:
            continue

        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", class_="table-tournament")
        if not table:
            print(f"  -> No tournament table found at {url}")
            continue

        tbody = table.find("tbody")
        if not tbody:
            print(f"  -> No <tbody> found at {url}")
            continue

        rows = tbody.find_all("tr")
        if not rows:
            print(f"  -> Tournament table empty at {url}")
            continue

        records = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            place = cols[0].get_text(strip=True)

            deck_link_tag = cols[1].find("a")
            if not deck_link_tag or "href" not in deck_link_tag.attrs:
                continue

            deck_name = deck_link_tag.get_text(strip=True)
            deck_url = "https://www.mtggoldfish.com" + deck_link_tag["href"]
            deck_id = get_deck_id(deck_url)
            if deck_id is None:
                continue

            pilot_name = cols[2].get_text(strip=True)

            # Match your earlier challenge insert shape (deck_id + date)
            records.append({
                "deck_id": deck_id,
                "date": date_str,
                "place": place,
                "deck_name": deck_name,
                "pilot": pilot_name,
                "json_decklist": None
            })

        if records:
            print(f"  -> Found {len(records)} rows at {url}")
            return records

    print(f"❌ No valid Challenge page found for {date_str} (normal/special/showcase).")
    return []

# ==========================
# Main scraping logic
# ==========================
def main():
    driver = None

    try:
        driver = setup_driver()
        print("Chrome driver initialized successfully")

        # --------------------------
        # PART 1: Pauper Leagues (your existing logic)
        # --------------------------
        today = datetime.today()
        start_date = today - timedelta(days=6)  # last 7 days including today
        end_date = today

        current_date = start_date
        total_league_rows_inserted = 0

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            url = f"https://www.mtggoldfish.com/tournament/pauper-league-{date_str}#online"

            print(f"\n{'='*60}")
            print(f"LEAGUE | Scraping URL: {url}")
            print(f"{'='*60}")

            try:
                ok = load_page_with_retries(driver, url, max_retries=2, sleep_after=2.0)
                if not ok:
                    current_date += timedelta(days=1)
                    continue

                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.find("table", class_="table-tournament")

                if not table:
                    print(f"Table not found for {date_str} (tournament may not exist)")
                    current_date += timedelta(days=1)
                    time.sleep(1)
                    continue

                tbody = table.find("tbody")
                if not tbody:
                    print(f"Tbody not found for {date_str} (tournament may not exist)")
                    current_date += timedelta(days=1)
                    time.sleep(1)
                    continue

                rows = tbody.find_all("tr")
                print(f"Found {len(rows)} rows in league table")

                payload = []

                for row in rows:
                    cols = row.find_all("td")
                    if len(cols) < 3:
                        continue

                    place = cols[0].text.strip()
                    deck_name = cols[1].text.strip()
                    pilot_name = cols[2].text.strip()

                    a_tag = cols[1].find("a")
                    if not a_tag or "href" not in a_tag.attrs:
                        continue

                    deck_url = "https://www.mtggoldfish.com" + a_tag["href"]
                    deck_id = get_deck_id(deck_url)
                    if deck_id is None:
                        continue

                    payload.append({
                        "id": deck_id,
                        "event_date": date_str,
                        "place": place,
                        "deck_name": deck_name,
                        "pilot": pilot_name,
                        "deck_url": deck_url
                    })

                if payload:
                    print(f"Attempting to insert {len(payload)} league rows into Supabase...")
                    resp = requests.post(
                        SUPABASE_LEAGUE_INSERT_ENDPOINT,
                        headers=SUPABASE_HEADERS,
                        data=json.dumps(payload),
                        timeout=30
                    )

                    if resp.status_code in (200, 201, 204):
                        print(f"✓ League insert OK for {date_str} ({len(payload)} rows)")
                        total_league_rows_inserted += len(payload)
                    else:
                        print(f"✗ League insert FAILED for {date_str}.")
                        print(f"  Status: {resp.status_code}")
                        print(f"  Body: {resp.text[:500]}")
                else:
                    print(f"No valid league rows to insert for {date_str}")

            except TimeoutException:
                print(f"✗ Timeout loading league page for {date_str} - skipping")
            except WebDriverException as e:
                print(f"✗ WebDriver error for league {date_str}: {e}")
            except Exception as e:
                print(f"✗ Unexpected error scraping league {date_str}: {e}")

            time.sleep(1)
            current_date += timedelta(days=1)

        print(f"\n{'='*60}")
        print(f"Leagues complete! Total rows inserted: {total_league_rows_inserted}")
        print(f"{'='*60}")

        # --------------------------
        # PART 2: Pauper Challenges (last 15 days rolling)
        # --------------------------
        challenge_end = datetime.today().date()
        challenge_start = challenge_end - timedelta(days=CHALLENGE_LOOKBACK_DAYS - 1)

        print(f"\n{'='*60}")
        print(f"CHALLENGES | Rolling range: {challenge_start.isoformat()} -> {challenge_end.isoformat()} "
              f"({CHALLENGE_LOOKBACK_DAYS} days)")
        print(f"{'='*60}")

        total_challenge_rows_inserted = 0
        current_day = challenge_start

        while current_day <= challenge_end:
            date_str = current_day.isoformat()
            print(f"\n=== CHALLENGE | Processing {date_str} ===")

            records = scrape_challenge_for_date(driver, date_str)

            if not records:
                print(f"No challenge data to insert for {date_str}.")
                current_day += timedelta(days=1)
                time.sleep(1)
                continue

            try:
                print(f"Attempting to insert {len(records)} challenge rows into Supabase...")
                resp = requests.post(
                    SUPABASE_CHALLENGE_INSERT_ENDPOINT,
                    headers=SUPABASE_HEADERS,
                    data=json.dumps(records),
                    timeout=30
                )

                if resp.status_code in (200, 201, 204):
                    print(f"✓ Challenge insert OK for {date_str} ({len(records)} rows)")
                    total_challenge_rows_inserted += len(records)
                else:
                    print(f"✗ Challenge insert FAILED for {date_str}.")
                    print(f"  Status: {resp.status_code}")
                    print(f"  Body: {resp.text[:500]}")
            except requests.exceptions.RequestException as e:
                print(f"✗ Network error inserting challenge data for {date_str}: {e}")
            except Exception as e:
                print(f"✗ Unexpected error inserting challenge data for {date_str}: {e}")

            time.sleep(1)
            current_day += timedelta(days=1)

        print(f"\n{'='*60}")
        print(f"Challenges complete! Total rows inserted: {total_challenge_rows_inserted}")
        print(f"{'='*60}")

        print(f"\nALL DONE ✅ | League rows: {total_league_rows_inserted} | Challenge rows: {total_challenge_rows_inserted}")

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

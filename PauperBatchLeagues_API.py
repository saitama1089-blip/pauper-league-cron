import os
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

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
    "Prefer": "return=minimal",  # we don't need the whole row back
}

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


def fetch_league_html(date_str: str) -> str | None:
    """Fetch the MTGGoldfish Pauper League page for a given date."""
    url = f"https://www.mtggoldfish.com/tournament/pauper-league-{date_str}#online"
    print("Scraping URL:", url)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except Exception as e:
        print(f"Request error for {date_str}: {e}")
        return None

    if resp.status_code != 200:
        print(f"HTTP {resp.status_code} for {date_str}")
        return None

    return resp.text


def process_date(date_str: str):
    html = fetch_league_html(date_str)
    if not html:
        print(f"No HTML for {date_str}")
        return

    soup = BeautifulSoup(html, "html.parser")

    # Find table with class containing 'table-tournament'
    table = soup.find("table", class_="table-tournament")
    if not table:
        print(f"Table not found for {date_str}!")
        return

    tbody = table.find("tbody")
    if not tbody:
        print(f"Tbody not found for {date_str}!")
        return

    rows = tbody.find_all("tr")
    payload: list[dict] = []

    for row in rows:
        cols = row.find_all("td")

        # Expecting columns: place | deck | pilot | ...
        if len(cols) < 3:
            print(f"Skipping row: Not enough columns ({len(cols)})")
            continue

        place = cols[0].get_text(strip=True)
        deck_name = cols[1].get_text(strip=True)
        pilot_name = cols[2].get_text(strip=True)

        # Build full deck URL
        a_tag = cols[1].find("a")
        if not a_tag or "href" not in a_tag.attrs:
            print("Skipping row: no deck link found")
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
            "deck_url": deck_url,
        }
        payload.append(row_data)

    if not payload:
        print(f"No rows to insert for {date_str}.")
        return

    try:
        resp = requests.post(
            SUPABASE_INSERT_ENDPOINT,
            headers=SUPABASE_HEADERS,
            data=json.dumps(payload),
            timeout=30,
        )
        if resp.status_code in (200, 201, 204):
            print(f"Supabase insert OK for {date_str} ({len(payload)} rows).")
        else:
            print(
                f"Supabase insert FAILED for {date_str}. "
                f"Status: {resp.status_code}, Body: {resp.text}"
            )
    except Exception as e:
        print(f"Error inserting into Supabase for {date_str}: {e}")


def main():
    today = datetime.today()
    start_date = today - timedelta(days=6)  # last 7 days including today
    end_date = today

    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        process_date(date_str)
        current += timedelta(days=1)


if __name__ == "__main__":
    main()

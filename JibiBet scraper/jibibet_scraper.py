import os
import json
# --- MODIFIED: Switched to curl_cffi for better TLS fingerprinting bypass ---
from curl_cffi import requests
import threading
import time
from time import sleep
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# ----------------Default scraper configuration --------------------------------------------------
MODE = "prematch"
SPORT = "football"
LOOP = False
CYCLE_DELAY = 60
REVERSE = False


# ----------------------------------------------------------------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(description='Jibibet Sportsbook Scraper')
    parser.add_argument('--mode', choices=['live', 'prematch'], default=MODE,
                        help=f'Choose "live" or "prematch" (default: {MODE})')
    parser.add_argument('--sport', default=SPORT,
                        help=f'Sport to scrape (default: {SPORT})')
    parser.add_argument('--loop', action='store_true', default=LOOP,
                        help=f'Enable looping mode (default: {LOOP})')
    parser.add_argument('--reversed', action='store_true', default=REVERSE,
                        help=f'Scrape in reverse order (default: {REVERSE})')
    parser.add_argument('--delay', type=float, default=CYCLE_DELAY,
                        help=f'Delay in seconds between cycles when looping (default: {CYCLE_DELAY})')
    return parser.parse_args()


args = parse_arguments()

# -------------- Scraper Configuration --------------------------------------------------
MODE = args.mode
SPORT = args.sport
LOOP = args.loop
REVERSED = args.reversed
CYCLE_DELAY = args.delay
# ----------------------------------------------------------------------------------------

# -------------- Performance Configuration ----------------------------------------------
# These values are still important to avoid simple rate-limiting
MAX_WORKERS = 1
SUBMISSION_DELAY = 2
# ----------------------------------------------------------------------------------------

# Change directory to the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# --- API Configuration ---
BRAND_ID = "2420651747870650368"
BASE_URL = f"https://api-h-c7818b61-608.sptpub.com/api/v3/{MODE}/brand/{BRAND_ID}"

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://jibibet.com",
    "Referer": "https://jibibet.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_TIMEOUT = 20  # Increased timeout slightly for cffi
TUNISIA_TIMEZONE = datetime.timezone(datetime.timedelta(hours=1))


def load_json_setting(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"Successfully loaded settings from {file_path}")
            return data
    except FileNotFoundError:
        print(f"FATAL: Settings file not found at {file_path}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"FATAL: Error decoding JSON from {file_path}: {e}")
        exit(1)


SPORT_ID = load_json_setting(os.path.join("settings", SPORT, "sport_id.json"))["sport_id"]
PARSING_RULES = load_json_setting(os.path.join("settings", SPORT, "parsing.json"))

OUTPUT_DIR = f"scraped_{MODE}_matches/{SPORT}"


# --- MODIFIED: The custom session with retries is no longer needed with curl_cffi ---
# The retry logic from urllib3 is not compatible. We rely on impersonation to avoid errors.

def get_fixture_versions(session):
    url = f"{BASE_URL}/en/0"
    print("Fetching initial fixture versions...")
    # --- MODIFIED: Added impersonate parameter ---
    response = session.get(url, timeout=REQUEST_TIMEOUT, impersonate="chrome120")
    response.raise_for_status()
    data = response.json()
    versions = data.get("fixtures_versions", [])
    print(f"Found {len(versions)} fixture versions to process.")
    return versions


def fetch_all_fixtures_data(session, versions):
    categories_map = {}
    tournaments_map = {}
    all_events = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_version = {}
        for v in versions:
            # --- MODIFIED: Added impersonate parameter to the submitted task ---
            future = executor.submit(session.get, f"{BASE_URL}/en/{v}", timeout=REQUEST_TIMEOUT,
                                     impersonate="chrome120")
            future_to_version[future] = v
            #time.sleep(SUBMISSION_DELAY)

        for future in as_completed(future_to_version):
            version = future_to_version[future]
            try:
                response = future.result()
                response.raise_for_status()
                data = response.json()

                categories_map.update(data.get("categories", {}))
                tournaments_map.update(data.get("tournaments", {}))
                all_events.update(data.get("events", {}))

            except Exception as e:
                print(f"Could not fetch or process fixture version {version}: {e}")

    print(
        f"Consolidated data: {len(categories_map)} categories, {len(tournaments_map)} tournaments, {len(all_events)} events.")
    return categories_map, tournaments_map, all_events


def parse_odds(odds_data, parsing_rules):
    # This function remains unchanged as it only processes JSON data
    parsed_odds = {}
    markets = odds_data.get("markets", {})
    if not markets:
        return {}

    rules = parsing_rules.get("odds_market_types", {})

    for rule in rules.get("regular", []):
        market = markets.get(rule["market_id"])
        if market:
            outcomes = next(iter(market.values()), {})
            for outcome_id, odd_value in outcomes.items():
                if outcome_id in rule["export_map"] and odd_value.get("k"):
                    export_key = rule["export_map"][outcome_id]
                    parsed_odds[export_key] = odd_value.get("k")

    for rule in rules.get("total", []):
        market = markets.get(rule["market_id"])
        if market:
            for total_key, outcomes in market.items():
                try:
                    total_value_str = total_key.split("=")[1]
                    total_value = float(total_value_str)
                    formatted_total = f"{total_value:.1f}" if total_value == int(total_value) else str(total_value)

                    over_odd = outcomes.get(rule["over_marker"], {}).get("k")
                    under_odd = outcomes.get(rule["under_marker"], {}).get("k")

                    if over_odd:
                        parsed_odds[f"{rule['prefix']}over_{formatted_total}_odd"] = over_odd
                    if under_odd:
                        parsed_odds[f"{rule['prefix']}under_{formatted_total}_odd"] = under_odd
                except (IndexError, ValueError):
                    continue

    for rule in rules.get("handicap", []):
        market = markets.get(rule["market_id"])
        if market:
            for hcp_key, outcomes in market.items():
                try:
                    hcp_value_str = hcp_key.split("=")[1]
                    hcp_value = float(hcp_value_str)
                    formatted_hcp = f"{hcp_value:.1f}" if hcp_value == int(hcp_value) else str(hcp_value)

                    home_odd = outcomes.get(rule["home_marker"], {}).get("k")
                    away_odd = outcomes.get(rule["away_marker"], {}).get("k")

                    away_hcp_value = -hcp_value
                    if away_hcp_value == -0.0: away_hcp_value = 0.0
                    formatted_away_hcp = f"{away_hcp_value:.1f}" if away_hcp_value == int(away_hcp_value) else str(
                        away_hcp_value)

                    if home_odd:
                        parsed_odds[f"{rule['prefix']}home_handicap_{formatted_hcp}_odd"] = home_odd
                    if away_odd:
                        parsed_odds[f"{rule['prefix']}away_handicap_{formatted_away_hcp}_odd"] = away_odd
                except (IndexError, ValueError):
                    continue

    return parsed_odds


def fetch_and_process_match(session, match_id, match_data, parsing_rules):
    try:
        url = f"{BASE_URL}/event/en/{match_id}"
        # --- MODIFIED: Added impersonate parameter ---
        response = session.get(url, timeout=REQUEST_TIMEOUT, impersonate="chrome120")

        if response.status_code == 404:
            return None

        response.raise_for_status()

        response_json = response.json()
        event_details = response_json.get("events", {}).get(str(match_id))

        if not event_details:
            return None

        parsed_odds = parse_odds(event_details, parsing_rules)

        if parsed_odds:
            match_data.update(parsed_odds)
            return match_data

    except Exception as e:
        # We will see this error if retries fail
        print(f"  - ERROR fetching/parsing match {match_id}: {e}")
        pass

    return None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- MODIFIED: Create a simple curl_cffi session ---
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    versions = get_fixture_versions(session)
    if not versions:
        print("No fixture versions found. Exiting.")
        return

    categories, tournaments, events = fetch_all_fixtures_data(session, versions)

    matches_to_fetch_odds = []
    print(f"\nProcessing {len(events)} unique events for sport ID '{SPORT_ID}'...")

    for event_id, event_data in events.items():
        desc = event_data.get("desc", {})
        if desc.get("sport") != SPORT_ID:
            continue

        try:
            home_team = desc["competitors"][0]["name"]
            away_team = desc["competitors"][1]["name"]

            ts = desc.get("scheduled")
            dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
            dt_local = dt_utc.astimezone(TUNISIA_TIMEZONE)

            category_id = desc.get("category")
            tournament_id = desc.get("tournament")
            country_name = categories.get(category_id, {}).get("name", "Unknown Country")
            tournament_name = tournaments.get(tournament_id, {}).get("name", "Unknown Tournament")

            match_info = {
                "country": country_name,
                "tournament_id": int(tournament_id),
                "tournament_name": tournament_name,
                "match_id": int(event_id),
                "date": dt_local.strftime("%d/%m/%Y"),
                "time": dt_local.strftime("%H:%M"),
                "home_team": home_team,
                "away_team": away_team,
            }
            matches_to_fetch_odds.append(match_info)
        except (KeyError, IndexError):
            continue

    print(f"Found {len(matches_to_fetch_odds)} matches for the selected sport. Fetching odds...")

    processed_matches = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_match = {}
        for match in matches_to_fetch_odds:
            future = executor.submit(fetch_and_process_match, session, match['match_id'], match, PARSING_RULES)
            future_to_match[future] = match
            time.sleep(SUBMISSION_DELAY)

        total_matches = len(matches_to_fetch_odds)
        completed_count = 0
        success_count = 0
        fail_count = 0

        for future in as_completed(future_to_match):
            completed_count += 1
            match_info = future_to_match[future]
            try:
                result = future.result()
                if result:
                    processed_matches.append(result)
                    success_count += 1
                    print(f"Match with id {match_info['match_id']} was scraped successfully.")
            except Exception:
                fail_count += 1

            print(f"Progress: {completed_count}/{total_matches} | Success: {success_count} | Failed: {fail_count}",
                  end='\r')

    print(f"\n\nSuccessfully fetched and parsed odds for {len(processed_matches)} matches.")

    if not processed_matches:
        print("No matches with valid odds were found. No files will be written.")
        return

    data_by_country = {}
    for match in processed_matches:
        country = match.pop("country")
        if country not in data_by_country:
            data_by_country[country] = {}

        tourney_name = match.get("tournament_name")
        if tourney_name not in data_by_country[country]:
            data_by_country[country][tourney_name] = {
                "tournament_id": match.get("tournament_id"),
                "tournament_name": tourney_name,
                "matches": []
            }

        del match["tournament_id"]
        del match["tournament_name"]
        data_by_country[country][tourney_name]["matches"].append(match)

    updated_files_this_cycle = set()
    for country, tournaments_data in data_by_country.items():
        safe_filename = country.replace('&', 'and').replace('/', '_').replace(' ', '_')
        path = os.path.join(OUTPUT_DIR, f"{safe_filename}.json")

        now_iso = datetime.datetime.now(TUNISIA_TIMEZONE).isoformat()

        output_data = [{"last_updated": now_iso}] + list(tournaments_data.values())

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)

        updated_files_this_cycle.add(path)
        print(f"Saved {safe_filename}.json with {len(tournaments_data)} tournaments.")

    try:
        existing_files = {os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.endswith('.json')}
        files_to_delete = existing_files - updated_files_this_cycle
        for file_path in files_to_delete:
            os.remove(file_path)
            print(f"Deleted old file: {os.path.basename(file_path)}")
    except Exception as e:
        print(f"An error occurred during file cleanup: {e}")


if __name__ == "__main__":
    if LOOP:
        c = 0
        while True:
            c += 1
            print(f"--- Starting Scrape Cycle {c} ---")
            main()
            print(f"--- Cycle {c} complete. Waiting {CYCLE_DELAY} seconds... ---\n")
            sleep(CYCLE_DELAY)
    else:
        main()
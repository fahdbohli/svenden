import os
import json
import time
import datetime
import re
import sys
import argparse
from curl_cffi import requests as cffi_requests

# ----------------Default scraper configuration ---------------------------------
MODE = "prematch"  # Choose the mode "prematch" or "live", use --mode
SPORT = "football"  # Choose the sport, use --sport
LOOP = False  # Choose if the code will loop, use --loop to activate it
CYCLE_DELAY = 2  # Delay in seconds between cycles, use --delay to set
REVERSE = False  # Reverse the order of scraping, use --inversed to enable
SCRAPING_METHOD = "general"  # "specific" for one request per match, "general" for bulk odds
TARGET = ["all"]  # "all", a single match_id, or a list of match_ids
SCRAPING_DELAY = 0.1 # the delay between  requests when method is specific and the target is all
# --------------------------------------------------------------------------------

# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Get timezone for UTC+1
TUNISIA_TZ = datetime.timezone(datetime.timedelta(hours=1))

# --- API & Networking Configuration ---
BASE_URL = "https://api-h-c7818b61-608.sptpub.com/api/v3/prematch"
BRAND_ID = "2420651747870650368"
HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://jibibet.com',
    'Referer': 'https://jibibet.com/',
    'Sec-Ch-Ua': '"Not.A/Brand";v="99", "Chromium";v="136"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
}


def load_config(file_path):
    """Loads a JSON configuration file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌  Configuration file not found at {file_path}. Exiting.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌  Error decoding JSON from {file_path}: {e}. Exiting.")
        sys.exit(1)


def _normalize_sv(value_str):
    """Ensure the line value string has at least one decimal place (e.g., '2' -> '2.0')."""
    try:
        # Convert to float to handle scientific notation and normalize
        val = float(value_str)
        # Format back to string, ensuring at least one decimal place
        if val == int(val):
            return f"{int(val)}.0"
        return f"{val}"
    except (ValueError, TypeError):
        return value_str  # Return original if conversion fails


def fetch_json(endpoint):
    """Performs a GET request using curl_cffi and returns JSON."""
    url = f"{BASE_URL}/{endpoint}"
    try:
        # Using a session for potential connection pooling
        session = cffi_requests.Session(impersonate="chrome110")
        response = session.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except cffi_requests.errors.RequestsError as e:
        print(f"  ↳ Network error for {url}: {e}")
    except json.JSONDecodeError:
        print(f"  ↳ Failed to decode JSON from {url}")
    return None


def parse_odds(odds_data, parsing_config):
    """
    Parses different types of odds for a match based on the parsing.json config.
    """
    parsed_odds = {}
    api_markets = odds_data.get("markets", {})
    if not api_markets:
        return {}

    for market_group in parsing_config.get("market_groups", []):
        for market_key, market_config in market_group.get("markets", {}).items():
            market_type = market_config.get("type")
            found_market_id = None
            for mid in market_config.get("market_ids", []):
                if mid in api_markets:
                    found_market_id = mid
                    break
            if not found_market_id:
                continue
            market_data = api_markets[found_market_id]
            if market_type == "regular":
                odds_container = market_data.get("", {})
                for odd_name_export, odd_config in market_config.get("odds", {}).items():
                    for key in odd_config.get("keys", []):
                        if key in odds_container and "k" in odds_container[key]:
                            parsed_odds[odd_name_export] = odds_container[key]["k"]
                            break
            elif market_type == "total":
                prefix = market_config.get("line_prefix", "")
                for line, line_data in market_data.items():
                    if not line.startswith(prefix): continue
                    sv = _normalize_sv(line.replace(prefix, ""))
                    for odd_type, odd_config in market_config.get("odds", {}).items():
                        export_name = odd_config.get("name", "").replace("{sv}", sv)
                        for key in odd_config.get("keys", []):
                            if key in line_data and "k" in line_data[key]:
                                parsed_odds[export_name] = line_data[key]["k"]
                                break
            elif market_type == "handicap":
                prefix = market_config.get("line_prefix", "")
                for line, line_data in market_data.items():
                    if not line.startswith(prefix): continue
                    sv_home_str = _normalize_sv(line.replace(prefix, ""))
                    try:
                        sv_home_val = float(sv_home_str)
                        sv_away_val = 0.0 if sv_home_val == 0.0 else -sv_home_val
                        sv_away_str = _normalize_sv(sv_away_val)
                    except ValueError:
                        print(f"  ↳ Warning: Could not parse handicap value '{sv_home_str}'")
                        continue
                    home_config = market_config["odds"]["home"]
                    export_name_home = home_config.get("name", "").replace("{sv}", sv_home_str)
                    for key in home_config.get("keys", []):
                        if key in line_data and "k" in line_data[key]:
                            parsed_odds[export_name_home] = line_data[key]["k"]
                            break
                    away_config = market_config["odds"]["away"]
                    export_name_away = away_config.get("name", "").replace("{sv}", sv_away_str)
                    for key in away_config.get("keys", []):
                        if key in line_data and "k" in line_data[key]:
                            parsed_odds[export_name_away] = line_data[key]["k"]
                            break
    return parsed_odds


def scrape_prematch(loop, inversed, delay, sport, method, target):
    """Main function for scraping prematch data."""
    # --- Load Configurations ---
    parsing_config = load_config(f"settings/{sport}/parsing.json")
    sport_id_config = load_config(f"settings/{sport}/sport_id.json")
    target_sport_id = sport_id_config.get('sport_id')
    if not target_sport_id:
        print(f"❌ Sport ID not found in settings/{sport}/sport_id.json. Exiting.")
        sys.exit(1)

    # --- Process Target Argument ---
    processed_target = "all"
    if isinstance(target, list):  # argparse with nargs='+' always returns a list
        if len(target) == 1 and target[0].lower() == 'all':
            processed_target = "all"
        else:
            try:
                processed_target = [int(t) for t in target]
            except ValueError:
                print("❌ Invalid target. Match IDs must be integers. Exiting.")
                sys.exit(1)

    if method == "specific" and processed_target != 'all':
        print(f"Targeting specific match IDs: {processed_target}")

    while True:
        print("--- Starting Scrape Cycle ---")

        # 1. Initial request to get fixture and market versions
        print("1. Fetching API versions...")
        version_data = fetch_json(f"brand/{BRAND_ID}/en/0")
        if not version_data:
            print("  ↳ Could not get API versions. Retrying after delay...")
            if not loop: break
            time.sleep(delay)
            continue

        fixture_versions = version_data.get("fixtures_versions")
        markets_versions = version_data.get("markets_versions")

        if not fixture_versions:
            print("  ↳ Could not get fixture versions. Retrying after delay...")
            if not loop: break
            time.sleep(delay)
            continue
        print(f"  ↳ Found {len(fixture_versions)} fixture versions.")

        if method == "general":
            if not markets_versions:
                print("  ↳ 'general' method selected, but no markets_versions found. Retrying after delay...")
                if not loop: break
                time.sleep(delay)
                continue
            print(f"  ↳ Found {len(markets_versions)} market versions.")

        # 2. Fetch data from all fixture versions (common for both methods)
        all_data = {"sports": {}, "categories": {}, "tournaments": {}, "events": {}}
        print("2. Fetching fixture data...")
        for version in fixture_versions:
            print(f"  ↳ Fetching fixture version: {version}")
            fixture_data = fetch_json(f"brand/{BRAND_ID}/en/{version}")
            if fixture_data:
                all_data["sports"].update(fixture_data.get("sports", {}))
                all_data["categories"].update(fixture_data.get("categories", {}))
                all_data["tournaments"].update(fixture_data.get("tournaments", {}))
                all_data["events"].update(fixture_data.get("events", {}))
        print(f"  ↳ Total events found across all versions: {len(all_data['events'])}")

        # 3. Fetch all odds data if using the 'general' method
        all_odds_data = {}
        if method == "general":
            print("3a. Fetching all market odds (general method)...")
            for i, version in enumerate(markets_versions):
                print(f"  ↳ Fetching market version: {version} ({i + 1}/{len(markets_versions)})")
                market_data = fetch_json(f"brand/{BRAND_ID}/en/{version}")
                if market_data and "events" in market_data:
                    all_odds_data.update(market_data.get("events", {}))
            print(f"  ↳ Aggregated odds for {len(all_odds_data)} events.")

        # 4. Parse and group matches by country (common for both methods)
        print("3b. Parsing and grouping matches by country...")
        matches_by_country = {}
        for match_id, event in all_data["events"].items():
            desc = event.get("desc", {})
            if desc.get("sport") != target_sport_id: continue
            competitors = desc.get("competitors", [])
            if len(competitors) < 2: continue
            home_team = competitors[0].get("name", "N/A")
            away_team = competitors[1].get("name", "N/A")
            try:
                dt_utc = datetime.datetime.fromtimestamp(desc["scheduled"], tz=datetime.timezone.utc)
                dt_local = dt_utc.astimezone(TUNISIA_TZ)
                match_date = dt_local.strftime("%d/%m/%Y")
                match_time = dt_local.strftime("%H:%M")
            except (KeyError, TypeError):
                continue
            tournament_id = desc.get("tournament")
            tournament_info = all_data["tournaments"].get(str(tournament_id))
            if not tournament_info: continue
            tournament_name = tournament_info.get("name")
            category_id = desc.get("category")
            category_info = all_data["categories"].get(str(category_id))
            if not category_info: continue
            country_name = category_info.get("name")
            if country_name not in matches_by_country: matches_by_country[country_name] = {}
            if tournament_id not in matches_by_country[country_name]:
                matches_by_country[country_name][tournament_id] = {
                    "tournament_id": int(tournament_id), "tournament_name": tournament_name, "matches": []
                }
            match_base_info = {
                "match_id": int(match_id), "date": match_date, "time": match_time,
                "home_team": home_team, "away_team": away_team
            }
            matches_by_country[country_name][tournament_id]["matches"].append(match_base_info)
        print(f"  ↳ Parsed and grouped matches into {len(matches_by_country)} countries.")

        # 5. Fetch/Process odds and save data country-by-country
        print("\n4. Processing odds and saving data country-by-country...")
        total_matches_processed = 0
        countries_to_process = list(matches_by_country.keys())
        if inversed: countries_to_process.reverse()
        out_dir = f"scraped_prematch_matches/{sport}"
        os.makedirs(out_dir, exist_ok=True)

        for i, country_name in enumerate(countries_to_process):
            print(f"\n--- [{i + 1}/{len(countries_to_process)}] Processing Country: {country_name} ---")

            country_tournaments = list(matches_by_country[country_name].values())
            country_match_count = sum(len(t['matches']) for t in country_tournaments)

            if method == "specific":
                # --- METHOD: SPECIFIC --- Loop through matches and fetch odds one by one
                print(f"  ↳ Fetching odds for {country_match_count} matches individually...")
                for tournament in country_tournaments:
                    for match_info in tournament['matches']:

                        # Apply target filter if not "all"
                        if processed_target != "all" and match_info['match_id'] not in processed_target:
                            continue

                        total_matches_processed += 1
                        print(
                            f"  ↳ [{total_matches_processed}] Fetching: {match_info['home_team']} vs {match_info['away_team']}")
                        odds_data = fetch_json(f"brand/{BRAND_ID}/event/en/{match_info['match_id']}")

                        if odds_data:
                            match_id_str = str(match_info['match_id'])
                            event_details = odds_data.get("events", {}).get(match_id_str)
                            if event_details:
                                parsed_odds = parse_odds(event_details, parsing_config)
                                match_info.update(parsed_odds)
                            else:
                                print(
                                    f"  ↳ Warning: Event details not found for match ID {match_id_str} in the response.")
                        time.sleep(SCRAPING_DELAY)  # Be polite to the server
                print(f"  ↳ Finished fetching odds for {country_name}.")

            elif method == "general":
                # --- METHOD: GENERAL --- Loop through matches and look up odds from pre-fetched data
                print(f"  ↳ Processing odds for {country_match_count} matches from general data...")
                for tournament in country_tournaments:
                    for match_info in tournament['matches']:
                        total_matches_processed += 1
                        match_id_str = str(match_info['match_id'])

                        event_details = all_odds_data.get(match_id_str)

                        if event_details:
                            parsed_odds = parse_odds(event_details, parsing_config)
                            match_info.update(parsed_odds)
                print(f"  ↳ Finished processing odds for {country_name}.")

            # --- Save data for the country (common logic for both methods) ---
            safe_country_name = re.sub(r'[^\w\-.]+', '_', country_name)
            out_path = os.path.join(out_dir, f"{safe_country_name}.json")

            final_data = [{"last_updated": datetime.datetime.now(TUNISIA_TZ).isoformat()}]
            final_data.extend(country_tournaments)

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(final_data, f, indent=4, ensure_ascii=False)
            print(f"  ✓ Saved data for {country_name} to {out_path}")

        print(f"\n--- Scraping cycle completed. Processed {total_matches_processed} matches. ---")
        if not loop: break
        print(f"\n--- Waiting {delay} seconds before next loop... ---\n")
        time.sleep(delay)


# --- Entry Point ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape prematch sports data from Jibibet.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--mode", choices=["prematch"], type=str.lower, default=MODE,
        help="The scraping mode. Currently only 'prematch' is supported."
    )
    parser.add_argument(
        "--sport", type=str, default=SPORT,
        help=f"The sport to scrape. Default: {SPORT}"
    )
    parser.add_argument(
        "--loop", action="store_true", default=LOOP,
        help="Enable loop mode to repeat scraping after each full pass."
    )
    parser.add_argument(
        "--inversed", action="store_true", default=REVERSE,
        help="Iterate through countries in reverse order."
    )
    parser.add_argument(
        "--method", choices=["specific", "general"], type=str.lower, default=SCRAPING_METHOD,
        help="The odds scraping method. 'specific' for one request per match, 'general' for bulk odds."
    )
    parser.add_argument(
        "--target", nargs='+', default=TARGET,
        help='Only for "specific" method. "all" to scrape all matches, or provide one or more match IDs.'
    )
    parser.add_argument(
        "--delay", type=float, default=CYCLE_DELAY,
        help=f"Delay in seconds between full passes in loop mode. Default: {CYCLE_DELAY}"
    )

    args = parser.parse_args()

    print(f"--- Starting Scraper ---")
    print(f"Mode: {args.mode.upper()}")
    print(f"Sport: {args.sport.upper()}")
    print(f"Method: {args.method.upper()}")
    if args.method == 'specific':
        print(f"Target: {' '.join(args.target)}")
    print(f"Loop: {'Enabled' if args.loop else 'Disabled'}")
    if args.loop:
        print(f"Delay: {args.delay}s")
    print(f"Inversed: {'Enabled' if args.inversed else 'Disabled'}")
    print("------------------------\n")

    if args.mode == "prematch":
        scrape_prematch(
            loop=args.loop,
            inversed=args.inversed,
            delay=args.delay,
            sport=args.sport,
            method=args.method,
            target=args.target
        )
    # Add live mode logic here in the future
    # elif args.mode == "live":
    #     scrape_live(...)
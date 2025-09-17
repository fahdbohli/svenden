import os
import json
import time
import datetime
import re
import sys
import argparse
import itertools
from curl_cffi import requests as cffi_requests

# ----------------Default scraper configuration ---------------------------------
MODE = "prematch"  # Choose the mode "prematch" or "live", use --mode
SPORT = "football"  # Choose the sport, use --sport
LOOP = False  # Choose if the code will loop, use --loop to activate it
CYCLE_DELAY = 0.2  # Delay in seconds between cycles, use --delay to set
REVERSE = False  # Reverse the order of scraping, use --inversed to enable
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


# --- Proxy Management Class ---
class ProxyManager:
    """Loads and cycles through proxies from a file."""

    def __init__(self, proxy_file="proxies/proxies.txt"):
        self.proxies = self._load_proxies(proxy_file)
        if not self.proxies:
            print("⚠️  Warning: No proxies loaded. Running without proxies.")
            self.proxy_pool = itertools.cycle([None])
        else:
            print(f"✅  Successfully loaded {len(self.proxies)} proxies.")
            self.proxy_pool = itertools.cycle(self.proxies)
        self.current_proxy = next(self.proxy_pool)

    def _load_proxies(self, file_path):
        """Loads proxies from a text file, one per line."""
        if not os.path.exists(file_path):
            return []
        try:
            with open(file_path, 'r') as f:
                proxies = [line.strip() for line in f if line.strip()]
                return proxies
        except Exception as e:
            print(f"❌  Could not read proxy file {file_path}: {e}")
            return []

    def get_next_proxy(self):
        """Rotates to the next proxy in the list."""
        self.current_proxy = next(self.proxy_pool)
        print(f"  🔄 Switched proxy to: {self.current_proxy}")
        return self.current_proxy

    def get_current_proxy_dict(self):
        """
        Returns the current proxy in the format requests expects.
        """
        if not self.current_proxy:
            return None
        fixed_proxy_url = self.current_proxy.replace("https://", "http://")
        return {'http': fixed_proxy_url, 'https': fixed_proxy_url}


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
        val = float(value_str)
        if val == int(val):
            return f"{int(val)}.0"
        return f"{val}"
    except (ValueError, TypeError):
        return value_str


def fetch_json(endpoint, proxy_manager):
    url = f"{BASE_URL}/{endpoint}"
    proxy_dict = proxy_manager.get_current_proxy_dict()
    session = cffi_requests.Session(impersonate="chrome110")
    session.verify = False

    try:
        response = session.get(
            url,
            headers=HEADERS,
            proxies=proxy_dict,
            timeout=10
        )

        # 1) If it’s a 503 you already handle that:
        if response.status_code == 503:
            print(f"  ↳ 503 Server Error/Blocked for {url} with proxy {proxy_manager.current_proxy}")
            return 503

        # 2) Debug here, *before* you try to decode JSON:
        content_type = response.headers.get("Content-Type", "")
        print(f"DEBUG: status={response.status_code}, Content-Type={content_type}")
        # show the first 300 characters of the body so you can see if it’s HTML or empty
        print("DEBUG: body snippet:", response.text[:300].replace("\n", " ") + "...")

        # 3) Only now attempt to parse JSON
        data = response.json()
        return data

    except cffi_requests.errors.RequestsError as e:
        print(f"  ↳ Network error for {url} with proxy {proxy_manager.current_proxy}: {e}")
        return "NETWORK_ERROR"

    except ValueError as e:  # JSONDecodeError is a subclass of ValueError
        print(f"  ↳ Failed to decode JSON from {url}: {e}")
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


def scrape_prematch(loop, inversed, delay, sport):
    """Main function for scraping prematch data with robust proxy rotation."""
    parsing_config = load_config(f"settings/{sport}/parsing.json")
    sport_id_config = load_config(f"settings/{sport}/sport_id.json")
    target_sport_id = sport_id_config.get('sport_id')
    if not target_sport_id: print(f"❌ Sport ID not found... Exiting."); sys.exit(1)

    proxy_manager = ProxyManager()
    print(f"Targeting Sport ID: {target_sport_id}")

    while True:
        print("--- Starting Scrape Cycle ---")

        print("1. Fetching API versions...")
        version_data = None
        while version_data is None:
            result = fetch_json(f"brand/{BRAND_ID}/en/0", proxy_manager)
            if result == 503 or result == "NETWORK_ERROR":
                proxy_manager.get_next_proxy()
                time.sleep(1)
            elif result is None:
                print("  ↳ Could not get fixture versions. Retrying after delay...")
                if not loop: break
                time.sleep(delay)
                proxy_manager.get_next_proxy()
            else:
                version_data = result

        if not version_data or "fixtures_versions" not in version_data:
            print("  ↳ Failed to get fixture versions. Ending cycle.")
            if not loop: break
            time.sleep(delay)
            continue

        fixture_versions = version_data["fixtures_versions"]
        print(f"  ↳ Found {len(fixture_versions)} fixture versions.")

        all_data = {"sports": {}, "categories": {}, "tournaments": {}, "events": {}}
        print("2. Fetching fixture data...")
        for version in fixture_versions:
            print(f"  ↳ Fetching version: {version}")
            fixture_data = None
            while fixture_data is None:
                result = fetch_json(f"brand/{BRAND_ID}/en/{version}", proxy_manager)
                if result == 503 or result == "NETWORK_ERROR":
                    proxy_manager.get_next_proxy()
                    time.sleep(1)
                else:
                    fixture_data = result
            if fixture_data:
                all_data["sports"].update(fixture_data.get("sports", {}))
                all_data["categories"].update(fixture_data.get("categories", {}))
                all_data["tournaments"].update(fixture_data.get("tournaments", {}))
                all_data["events"].update(fixture_data.get("events", {}))
        print(f"  ↳ Total events found across all versions: {len(all_data['events'])}")

        print("3. Parsing match information...")
        matches_by_country = {}
        for match_id, event in all_data["events"].items():
            desc = event.get("desc", {});
            competitors = desc.get("competitors", [])
            if desc.get("sport") != target_sport_id or len(competitors) < 2: continue
            home_team, away_team = competitors[0].get("name", "N/A"), competitors[1].get("name", "N/A")
            try:
                dt_utc = datetime.datetime.fromtimestamp(desc["scheduled"],
                                                         tz=datetime.timezone.utc);
                dt_local = dt_utc.astimezone(
                    TUNISIA_TZ);
                match_date, match_time = dt_local.strftime("%d/%m/%Y"), dt_local.strftime("%H:%M")
            except (KeyError, TypeError):
                continue
            tournament_id = desc.get("tournament");
            tournament_info = all_data["tournaments"].get(str(tournament_id));
            category_id = desc.get("category");
            category_info = all_data["categories"].get(str(category_id))
            if not tournament_info or not category_info: continue
            tournament_name, country_name = tournament_info.get("name"), category_info.get("name")
            if country_name not in matches_by_country: matches_by_country[country_name] = {}
            if tournament_id not in matches_by_country[country_name]: matches_by_country[country_name][
                tournament_id] = {"tournament_id": int(tournament_id), "tournament_name": tournament_name,
                                  "matches": []}
            matches_by_country[country_name][tournament_id]["matches"].append(
                {"match_id": int(match_id), "date": match_date, "time": match_time, "home_team": home_team,
                 "away_team": away_team})
        print(f"  ↳ Parsed matches grouped into {len(matches_by_country)} countries.")

        # --- MODIFIED SECTION: PROCESS AND EXPORT ONE COUNTRY AT A TIME ---
        print("\n4. Processing and Exporting Countries One by One...")
        out_dir = f"scraped_prematch_matches/{sport}";
        os.makedirs(out_dir, exist_ok=True)
        total_matches_processed = 0

        countries_to_process = list(matches_by_country.keys())
        if inversed: countries_to_process.reverse()

        for country_name in countries_to_process:
            print(f"\n--- Starting to process country: {country_name} ---")
            country_tournaments_data = matches_by_country[country_name]

            for tournament_id in country_tournaments_data:
                matches_to_process = country_tournaments_data[tournament_id]["matches"]
                match_index = 0
                while match_index < len(matches_to_process):
                    match_info = matches_to_process[match_index]

                    if 'retrying' not in match_info:
                        total_matches_processed += 1
                        print(
                            f"  ↳ [{total_matches_processed}] Fetching: {match_info['home_team']} vs {match_info['away_team']}")

                    endpoint = f"brand/{BRAND_ID}/event/en/{match_info['match_id']}"
                    full_response_data = fetch_json(endpoint, proxy_manager)

                    if full_response_data == 503 or full_response_data == "NETWORK_ERROR":
                        proxy_manager.get_next_proxy()
                        match_info['retrying'] = True
                        time.sleep(1)
                        continue

                    match_info.pop('retrying', None)

                    if full_response_data:
                        event_details = full_response_data.get("events", {}).get(str(match_info['match_id']))
                        if event_details:
                            parsed_odds = parse_odds(event_details, parsing_config)
                            match_info.update(parsed_odds)

                    time.sleep(0.1)
                    match_index += 1

            # After all matches for the country are processed, save the file
            print(f"--- Finished processing {country_name}. Saving file... ---")
            safe_country_name = re.sub(r'[^\w\-.]+', '_', country_name)
            out_path = os.path.join(out_dir, f"{safe_country_name}.json")

            final_data_for_country = [{"last_updated": datetime.datetime.now(TUNISIA_TZ).isoformat()}]
            final_data_for_country.extend(list(country_tournaments_data.values()))

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(final_data_for_country, f, indent=4, ensure_ascii=False)

            print(f"  ↳ ✅ Saved data for {country_name} to {out_path}")

        print(f"\n--- Scraping cycle completed. Processed a total of {total_matches_processed} matches. ---")
        if not loop: break
        print(f"\n--- Waiting {delay} seconds before next loop... ---\n")
        time.sleep(delay)


# --- Entry Point (UNCHANGED) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape prematch sports data from Jibibet.",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--mode", choices=["prematch"], type=str.lower, default=MODE,
                        help="The scraping mode. Currently only 'prematch' is supported.")
    parser.add_argument("--sport", type=str, default=SPORT, help=f"The sport to scrape. Default: {SPORT}")
    parser.add_argument("--loop", action="store_true", default=LOOP,
                        help="Enable loop mode to repeat scraping after each full pass.")
    parser.add_argument("--inversed", action="store_true", default=REVERSE,
                        help="Iterate through countries in reverse order.")
    parser.add_argument("--delay", type=float, default=CYCLE_DELAY,
                        help=f"Delay in seconds between full passes in loop mode. Default: {CYCLE_DELAY}")

    args = parser.parse_args()

    print(f"--- Starting Scraper ---")
    print(f"Mode: {args.mode.upper()}")
    print(f"Sport: {args.sport.upper()}")
    print(f"Loop: {'Enabled' if args.loop else 'Disabled'}")
    if args.loop: print(f"Delay: {args.delay}s")
    print(f"Inversed: {'Enabled' if args.inversed else 'Disabled'}")
    print("------------------------\n")

    if args.mode == "prematch":
        scrape_prematch(loop=args.loop, inversed=args.inversed, delay=args.delay, sport=args.sport)
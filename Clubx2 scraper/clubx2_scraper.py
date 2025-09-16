import os
import json
import time
import datetime
import re
import sys
import random
import requests
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# ----------------Default scraper configuration --------------------------------------------------
MODE = "prematch"               # Choose the mode "live" or "prematch", use --mode
SPORT = "football"              # Choose the sport "football" or ..., use --sport
LOOP = False                    # Choose if the code will loop or one time scrape
CYCLE_DELAY = 1                 # Choose the delay in seconds between each cycle if loop is activated
REVERSE = False                 # Choose the reverse state
MAX_WORKERS = 20                # Number of concurrent threads for fetching data
TOURNAMENTS_PER_REQUEST = 15    # Number of tournaments to batch into a single prematch API request
MERGE_FILES = False             # Set to True to merge all output files into one, use --merge
MERGED_FILENAME = "merged.json" # Filename for the merged output, use --merged-filename
# ----------------------------------------------------------------------------------------

# ----------------- Presets for default configuration ------------------------
PRESET = "football"
# -----------------------------------------------------------------------------

# Preset configuration application
if PRESET == "tennis":
    SPORT = "tennis"
    MERGE_FILES = True
if PRESET == "football":
    SPORT = "football"
    MERGE_FILES = False


# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Get Tunisia timezone : UTC+1
TUNISIA_TZ = datetime.timezone(datetime.timedelta(hours=1))

def load_sport_id(sport):
    """Loads the sport_id from settings/<sport>/sport_id.json"""
    path = os.path.join("settings", sport, "sport_id.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"Loaded sport_id {data['sport_id']} from {path}")
            return data["sport_id"]
    except Exception as e:
        print(f"WARNING: Could not load sport_id from {path}: {e}")
        return None

# load it right here, before any API params are constructed
SPORT_ID = load_sport_id(SPORT)

# ---------------------------------------------------
# Common Configuration
# ---------------------------------------------------
BASE_URL = "https://sb2frontend-1-altenar2.biahosted.com"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def load_parsing_config(sport):
    """Load parsing configuration from JSON file."""
    config_path = f"settings/{sport}/parsing.json"
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️  Parsing config not found at {config_path}.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing {config_path}: {e}.")
        sys.exit(1)


# REMOVED: get_market_type_ids is no longer needed as IDs are fetched per group.

def create_session_with_retries():
    session = requests.Session()
    session.headers.update(HEADERS)
    retry_strategy = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_json(session, endpoint, params):
    url = BASE_URL + endpoint
    resp = session.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def parse_market_odds(market, odd_map, market_name, all_markets_config, base_data):
    """
    MODIFIED: Dynamically parse odds for a specific market based on the 'structure'
    defined in the parsing configuration. Now searches across all market groups.
    """
    market_config = None

    # Find matching market configuration by searching through all groups
    for group in all_markets_config.get("market_groups", []):
        for key, data in group["markets"].items():
            # Use .lower() for robust, case-insensitive matching
            if market_name in [name.lower() for name in data["market_names"]]:
                market_config = data
                break
        if market_config:
            break

    if not market_config:
        return

    # Read the structure type from the config
    structure_type = market_config.get("structure")

    # Dispatch to the correct parsing function based on the structure
    if structure_type == "regular":
        parse_regular_market(market, odd_map, market_config, base_data)
    elif structure_type == "total":
        parse_total_market(market, odd_map, market_config, base_data)
    elif structure_type == "handicap":
        parse_handicap_market(market, odd_map, market_config, base_data)


def parse_regular_market(market, odd_map, market_config, base_data):
    """Parse regular market odds (1x2, double chance, both teams score)."""
    for oid in market.get("oddIds", []):
        odd = odd_map.get(oid)
        if odd and odd["oddStatus"] == 0:
            odd_name = odd["name"].upper()
            price = odd["price"]

            # Find matching odd configuration
            for odd_key, odd_data in market_config["odds"].items():
                if odd_name in [name.upper() for name in odd_data["names"]]:
                    base_data[odd_data["export_name"]] = price
                    break


def parse_total_market(market, odd_map, market_config, base_data):
    """Parse total market odds."""
    lines = market.get("lines", [])
    if lines:
        for line in lines:
            sv = _normalize_line(line.get("sv", "").strip())
            if not sv:
                continue
            for oid in line.get("oddIds", []):
                odd = odd_map.get(oid)
                if odd and odd["oddStatus"] == 0:
                    odd_name = odd.get("name", "").lower()
                    price = odd.get("price")

                    for odd_key, odd_data in market_config["odds"].items():
                        if any(name in odd_name for name in odd_data["names"]):
                            export_name = odd_data["export_name"].replace("{sv}", sv)
                            base_data[export_name] = price
                            break

    # Handle top-level odds
    top_level_odd_ids = market.get("oddIds", [])
    if top_level_odd_ids:
        for oid in top_level_odd_ids:
            odd = odd_map.get(oid)
            if not odd or odd.get("oddStatus") != 0:
                continue
            price = odd.get("price")
            name_full = odd.get("name", "").lower().strip()
            parts = name_full.split()
            if len(parts) == 2:
                side, raw_val = parts
                sv = _normalize_line(raw_val)
                if not sv:
                    continue

                for odd_key, odd_data in market_config["odds"].items():
                    if any(name in side for name in odd_data["names"]):
                        export_name = odd_data["export_name"].replace("{sv}", sv)
                        base_data[export_name] = price
                        break


def parse_handicap_market(market, odd_map, market_config, base_data):
    """Parse handicap market odds."""
    lines = market.get("lines", [])
    if lines:
        for line in lines:
            sv = _normalize_line(line.get("sv", "").strip())
            if not sv:
                continue
            try:
                val = float(sv)
                away_val = 0.0 if val == 0.0 else -val
                away_sv = _normalize_line(str(away_val))
            except ValueError:
                away_sv = sv.lstrip("-") if sv.startswith("-") else f"-{sv}"

            for oid in line.get("oddIds", []):
                odd = odd_map.get(oid)
                if odd and odd["oddStatus"] == 0:
                    name = odd.get("name", "").strip()
                    price = odd.get("price")
                    m_side = re.match(r"^([12])", name)
                    if m_side:
                        side_digit = m_side.group(1)
                        if side_digit == "1":
                            export_name = market_config["odds"]["home"]["export_name"].replace("{sv}", sv)
                            base_data[export_name] = price
                        else:
                            export_name = market_config["odds"]["away"]["export_name"].replace("{sv}", away_sv)
                            base_data[export_name] = price

    # Handle top-level handicap odds
    top_level_odd_ids = market.get("oddIds", [])
    if top_level_odd_ids:
        for oid in top_level_odd_ids:
            odd = odd_map.get(oid)
            if not odd or odd.get("oddStatus") != 0:
                continue
            name_full = odd.get("name", "").strip()
            price = odd.get("price")
            m_h = re.match(r"([12])\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s*\)", name_full)
            if not m_h:
                continue
            side_digit, raw_val = m_h.groups()
            clean_val = _normalize_line(raw_val)
            if not clean_val:
                continue

            if side_digit == "1":
                export_name = market_config["odds"]["home"]["export_name"].replace("{sv}", clean_val)
                base_data[export_name] = price
            else:
                export_name = market_config["odds"]["away"]["export_name"].replace("{sv}", clean_val)
                base_data[export_name] = price


# ---------------------------------------------------
# PREMATCH Endpoints & Helpers
# ---------------------------------------------------
SPORT_MENU_ENDPOINT = "/api/widget/GetSportMenu"
OVERVIEW_ENDPOINT = "/api/Widget/GetOverviewWithGroups"
EVENT_DETAILS_ENDPOINT = "/api/widget/GetEventDetails"

MENU_PARAMS = {
    "culture": "en-GB", "timezoneOffset": "-60", "integration": "webetx2",
    "deviceType": "1", "numFormat": "en-GB", "countryCode": "TN", "period": "0"
}
OVERVIEW_COMMON_PARAMS = {
    "culture": "en-GB", "timezoneOffset": "-60", "integration": "webetx2",
    "deviceType": "1", "numFormat": "en-GB", "countryCode": "TN",
    "eventCount": "0", "sportId": SPORT_ID
}


def fetch_event_details(session, event_id):
    """
    Use curl_cffi (Chrome-120 impersonation) to bypass TLS/anti-bot blocking
    for GetEventDetails.
    """
    try:
        from curl_cffi import requests as cc_requests
    except ImportError:
        raise RuntimeError("❌ curl_cffi not installed. Run: pip install curl-cffi")

    url = BASE_URL + EVENT_DETAILS_ENDPOINT
    params = {"culture": "en-GB", "timezoneOffset": "-60", "integration": "webetx2",
              "deviceType": "1", "numFormat": "en-GB", "countryCode": "TN", "eventId": str(event_id)
              }
    headers = {
        "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9,en-GB;q=0.8,en;q=0.7", "Cache-Control": "no-cache",
        "Origin": "https://www.clubx2.com", "Referer": "https://www.clubx2.com/",
        "Sec-Ch-Ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
        "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": "\"Windows\"", "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = cc_requests.get(url, params=params, headers=headers, impersonate="chrome120", timeout=30)
    if response.status_code != 200:
        raise requests.HTTPError(f"{response.status_code} Client Error: {response.text[:200]}", response=response)
    return response.json()


def parse_sport_menu(menu_json):
    sports     = menu_json.get("sports", [])
    categories = menu_json.get("categories", [])
    # Find the sport whose 'id' matches SPORT_ID
    sport_obj = next((s for s in sports if s.get("id") == SPORT_ID), None)
    if not sport_obj:
        print(f"⚠️  Sport with id={SPORT_ID} not found in menu.")
        return []
    cat_ids = set(sport_obj.get("catIds", []))
    result = []
    for cat in categories:
        if cat.get("id") in cat_ids:
            result.append({"country_id": cat["id"], "country_name": cat.get("name", "").strip(),
                           "champ_ids": cat.get("champIds", [])})
    return result


MATCHES_COUNTER = 0


# NEW HELPER FUNCTION
def _merge_and_parse_partial_overview(overview_json, all_matches_map, parsing_config):
    """
    Parses a partial overview response (from a single market group request)
    and merges the data into the main matches map.
    MODIFIED: Now adds champId to the base match data for later grouping.
    """
    global MATCHES_COUNTER
    odds_list = overview_json.get("odds", [])
    odd_map = {o["id"]: o for o in odds_list}
    markets = overview_json.get("markets", [])
    market_map = {m["id"]: m for m in markets}
    for m in markets:
        for line in m.get("lines", []):
            market_map[line["id"]] = line

    for ev in overview_json.get("events", []):
        match_id = ev.get("id")

        # If match is new, create its base data.
        if match_id not in all_matches_map:
            MATCHES_COUNTER += 1
            raw_name = ev.get("name", "")
            parts = re.split(r"\s+vs\.?\s+", raw_name, flags=re.IGNORECASE)
            home_team, away_team = (parts[0].strip(), parts[1].strip()) if len(parts) == 2 else ("", "")
            dt_utc = datetime.datetime.strptime(ev.get("startDate"), "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=datetime.timezone.utc)
            dt_local = (dt_utc + datetime.timedelta(hours=1)).astimezone(datetime.timezone(datetime.timedelta(hours=0)))
            base = {"match_id": match_id,
                    "champ_id": ev.get("champId"),  # Temporarily add champId for grouping
                    "date": dt_local.strftime("%d/%m/%Y"), "time": dt_local.strftime("%H:%M"),
                    "home_team": home_team, "away_team": away_team}
            all_matches_map[match_id] = base

        # Get the match object (whether new or existing) to add odds to it.
        match_data = all_matches_map[match_id]

        for m_id in ev.get("marketIds", []):
            market = market_map.get(m_id)
            if not market:
                continue
            m_name = market.get("name", "").strip().lower()
            # The last argument is the full parsing config
            parse_market_odds(market, odd_map, m_name, parsing_config, match_data)


# NEW BATCH-PROCESSING WORKER FUNCTION
def fetch_and_parse_championship_batch(session, champ_id_chunk, all_champs_map, parsing_config):
    """
    NEW: Worker function to fetch and parse a BATCH of championships.
    It makes one request per market group for all specified champ IDs.
    """
    all_matches_for_batch = {}  # A single map for all matches in the batch

    try:
        # Loop through each market group, fetching data for the entire batch of champs
        for market_group in parsing_config.get("market_groups", []):
            group_id = market_group.get("group_id")
            markets_in_group = market_group.get("markets", {})
            if not group_id or not markets_in_group:
                continue

            market_type_ids = [str(m_data["id"]) for m_data in markets_in_group.values() if "id" in m_data]
            if not market_type_ids:
                continue

            params = OVERVIEW_COMMON_PARAMS.copy()
            params["champIds"] = ",".join(map(str, champ_id_chunk))
            params["marketGroupId"] = str(group_id)
            params["marketTypeIds"] = ",".join(market_type_ids)

            overview_json = fetch_json(session, OVERVIEW_ENDPOINT, params)
            _merge_and_parse_partial_overview(overview_json, all_matches_for_batch, parsing_config)

        if not all_matches_for_batch:
            return []

        # After all market groups are fetched, group the parsed matches by their tournament ID
        results_by_champ = {}
        for match_data in all_matches_for_batch.values():
            champ_id = match_data.pop("champ_id", None)  # Get and remove the temporary champId
            if not champ_id:
                continue

            if champ_id not in results_by_champ:
                tournament_name = all_champs_map.get(champ_id, {}).get("name", str(champ_id))
                results_by_champ[champ_id] = {
                    "tournament_id": champ_id,
                    "tournament_name": tournament_name,
                    "matches": []
                }
            results_by_champ[champ_id]["matches"].append(match_data)

        return list(results_by_champ.values())

    except Exception as e:
        print(f"  ↳ Could not process champ batch starting with {champ_id_chunk[0]}: {e}")
    return []


sport_menu_cache = None
cache_expiry_time = None


def scrape_prematch(loop, inversed, delay, sport, merge_files, merged_filename):  # NEW: Added merge arguments
    """
    Main prematch loop using ThreadPoolExecutor for concurrency.
    MODIFIED: Now batches tournament IDs and deletes files for empty countries.
    NEW: Added logic to merge all scraped data into a single file if enabled.
    """
    global sport_menu_cache, cache_expiry_time, MATCHES_COUNTER

    parsing_config = load_parsing_config(sport)

    while True:
        MATCHES_COUNTER = 0  # Reset counter for each pass
        session = create_session_with_retries()

        # NEW: List to hold data from all countries for potential merging
        all_country_data_for_merge = []

        if not sport_menu_cache or time.time() > cache_expiry_time:
            print("Fetching new sport menu...")
            sport_menu_cache = fetch_json(session, SPORT_MENU_ENDPOINT, MENU_PARAMS)
            cache_expiry_time = time.time() + 1
            print("Sport menu cached.")
        else:
            print("Using cached sport menu.")

        menu_json = sport_menu_cache
        countries = parse_sport_menu(menu_json)
        all_champs_map = {c.get('id'): c for c in menu_json.get("champs", [])}

        if inversed: countries = list(reversed(countries))

        out_dir = f"scraped_prematch_matches/{sport}"
        os.makedirs(out_dir, exist_ok=True)

        new_safe_countries = {re.sub(r"[^\w\-]+", "_", c["country_name"]) for c in countries}
        for filename in os.listdir(out_dir):
            if filename.lower().endswith(".json") and filename[:-5] not in new_safe_countries:
                try:
                    # NEW: Ensure we don't delete the merged file if it's in the same directory
                    if merge_files and filename == merged_filename:
                        continue
                    os.remove(os.path.join(out_dir, filename))
                    print(f"→ Removed old file: {filename}")
                except OSError:
                    pass

        for country in countries:
            c_name, safe_country = country["country_name"], re.sub(r"[^\w\-]+", "_", country["country_name"])
            out_path = os.path.join(out_dir, f"{safe_country}.json")

            final_champ_ids = []
            for champ_id_or_group_id in country.get("champ_ids", []):
                champ_obj = all_champs_map.get(champ_id_or_group_id)
                if champ_obj and "champIds" in champ_obj and champ_obj.get("champIds"):
                    final_champ_ids.extend(champ_obj["champIds"])
                else:
                    final_champ_ids.append(champ_id_or_group_id)

            country_data = []

            batch_size = TOURNAMENTS_PER_REQUEST
            champ_id_chunks = [final_champ_ids[i:i + batch_size] for i in
                               range(0, len(final_champ_ids), batch_size)]

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_chunk = {
                    executor.submit(fetch_and_parse_championship_batch, session, chunk, all_champs_map,
                                    parsing_config): chunk
                    for chunk in champ_id_chunks}

                for future in future_to_chunk:
                    result_list = future.result()
                    if result_list:
                        country_data.extend(result_list)

            if country_data:
                current_time = datetime.datetime.now(TUNISIA_TZ)
                timestamp_iso = current_time.isoformat()
                final_data = [{"last_updated": timestamp_iso}] + country_data

                # NEW: Store data for merging if the option is enabled
                if merge_files:
                    all_country_data_for_merge.append({
                        "timestamp": timestamp_iso,
                        "tournaments": country_data
                    })

                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=4)
                print(f"→ Saved PREMATCH for {c_name} → {out_path} ({len(country_data)} tournaments)")
            else:
                print(f"→ No PREMATCH matches for {c_name}, cleaning up.")
                if os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                        print(f"  ↳ Removed old file: {out_path}")
                    except OSError as e:
                        print(f"  ↳ Failed to remove old file {out_path}: {e}")

        # --- NEW: MERGING LOGIC ---
        if merge_files and all_country_data_for_merge:
            print("\n--- Merging all scraped files ---")

            # Find the oldest timestamp among all files
            oldest_timestamp = min(item["timestamp"] for item in all_country_data_for_merge)

            # Combine all tournaments into a single list
            all_tournaments = []
            for item in all_country_data_for_merge:
                all_tournaments.extend(item["tournaments"])

            # Create the final merged data structure
            merged_data = [{"last_updated": oldest_timestamp}] + all_tournaments

            # Save the merged file
            merged_file_path = os.path.join(out_dir, merged_filename)
            with open(merged_file_path, "w", encoding="utf-8") as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=4)
            print(f"✓ All data merged into -> {merged_file_path}")

            # CLEAN UP INDIVIDUAL COUNTRY FILES ---
            print("--- Cleaning up individual files ---")
            for filename in os.listdir(out_dir):
                if filename.lower().endswith(".json") and filename != merged_filename:
                    try:
                        os.remove(os.path.join(out_dir, filename))
                        print(f"  ↳ Removed: {filename}")
                    except OSError as e:
                        print(f"  ↳ Failed to remove {filename}: {e}")
            print("✓ Cleanup complete.")

        elif merge_files:
            print("\n--- No data scraped, skipping merge ---")
        # --- END OF NEW MERGING LOGIC ---

        if not loop:
            print(f"Scraping successfully completed, found {MATCHES_COUNTER} matches.")
            break

        print(f"\n--- Prematch pass complete. Waiting {delay} seconds before next loop... ---\n")
        time.sleep(delay)


# ---------------------------------------------------
# LIVE
# ---------------------------------------------------
LIVE_OVERVIEW_ENDPOINT = "/api/widget/GetLiveOverview"
LIVE_PARAMS_TEMPLATE = {
    "culture": "en-GB",
    "timezoneOffset": "-60",
    "integration": "webetx2",
    "deviceType": "1",
    "numFormat": "en-GB",
    "countryCode": "TN",
    "sportId": SPORT_ID,
    "marketGroupId": "1"
}


def _get_live_market_type_ids(parsing_config):
    """Helper to get all market IDs for live mode from the new config structure."""
    market_type_ids = []
    for group in parsing_config.get("market_groups", []):
        for market_data in group.get("markets", {}).values():
            if "id" in market_data:
                market_type_ids.append(str(market_data["id"]))
    return market_type_ids


def parse_live_event_basic(ev, odd_map, market_map, parsing_config):
    match_id = ev.get("id")
    raw_name = ev.get("name", "")
    parts = re.split(r"\s+vs\.?\s+", raw_name, flags=re.IGNORECASE)
    home_team, away_team = (parts[0].strip(), parts[1].strip()) if len(parts) == 2 else ("", "")
    dt_utc = datetime.datetime.strptime(ev.get("startDate"), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    dt_local = (dt_utc + datetime.timedelta(hours=1)).astimezone(datetime.timezone(datetime.timedelta(hours=0)))
    base = {"match_id": match_id, "date": dt_local.strftime("%d/%m/%Y"), "time": dt_local.strftime("%H:%M"),
            "home_team": home_team, "away_team": away_team}

    for m_id in ev.get("marketIds", []):
        market = market_map.get(m_id)
        if not market:
            continue
        m_name = market.get("name", "").strip().lower()
        parse_market_odds(market, odd_map, m_name, parsing_config, base)

    return base


def _normalize_line(raw_sv: str) -> str:
    if not raw_sv: return ""
    s = raw_sv.strip().lstrip("+")
    if "." not in s: return f"{s}.0"
    integer_part, decimal_part = s.split(".", 1)
    if set(decimal_part) == {"0"}: return f"{integer_part}.0"
    if len(decimal_part) == 1: return f"{integer_part}.{decimal_part}"
    return f"{integer_part}.{decimal_part[0]}" if decimal_part[1] == "0" else f"{integer_part}.{decimal_part[:2]}"


def parse_event_details(event_json, parsing_config):
    """
    MODIFIED: This function now dynamically parses all configured markets from the GetEventDetails response,
    regardless of their group, by using the market "structure" from parsing.json.
    """
    odd_map = {o["id"]: o for o in event_json.get("odds", [])}
    extra_data = {}

    # Create a flat map of all markets from all groups for easier lookup
    all_markets_config = {}
    for group in parsing_config.get("market_groups", []):
        all_markets_config.update(group.get("markets", {}))

    for m in event_json.get("markets", []):
        m_name = m.get("name", "").strip().lower()
        odd_groups = m.get("desktopOddIds", []) or m.get("mobileOddIds", [])

        # Find matching market configuration
        market_config = None
        for market_key, market_data in all_markets_config.items():
            if m_name in [name.lower() for name in market_data.get("market_names", [])]:
                market_config = market_data
                break

        if not market_config:
            continue

        structure_type = market_config.get("structure")

        # --- DYNAMIC PARSING BASED ON STRUCTURE ---
        if structure_type == "regular":
            for odd_group in odd_groups:
                for oid in odd_group:
                    odd = odd_map.get(oid)
                    if odd and odd.get("oddStatus") == 0:
                        odd_name_upper = odd["name"].upper()
                        price = odd["price"]
                        for odd_key, odd_data in market_config["odds"].items():
                            if odd_name_upper in [name.upper() for name in odd_data["names"]]:
                                extra_data[odd_data["export_name"]] = price
                                break

        elif structure_type == "total":
            for sub_group in odd_groups:
                for oid in sub_group:
                    odd = odd_map.get(oid)
                    if not odd or odd.get("oddStatus") != 0:
                        continue
                    sv = _normalize_line(odd.get("sv", "").strip())
                    if not sv:
                        continue
                    nm_lower = odd.get("name", "").lower()
                    price = odd.get("price")
                    for odd_key, odd_data in market_config["odds"].items():
                        if any(name in nm_lower for name in odd_data["names"]):
                            export_name = odd_data["export_name"].replace("{sv}", sv)
                            extra_data[export_name] = price
                            break

        elif structure_type == "handicap":
            for sub_group in odd_groups:
                for oid in sub_group:
                    odd = odd_map.get(oid)
                    if not odd or odd.get("oddStatus") != 0:
                        continue
                    nm_full = odd.get("name", "").strip()
                    price = odd.get("price")
                    m_h = re.match(r"([12])\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s*\)", nm_full)
                    if not m_h:
                        continue
                    side_digit, raw_val = m_h.groups()
                    clean_val = _normalize_line(raw_val)
                    if side_digit == "1":
                        export_name = market_config["odds"]["home"]["export_name"].replace("{sv}", clean_val)
                        extra_data[export_name] = price
                    else:  # side_digit == "2"
                        export_name = market_config["odds"]["away"]["export_name"].replace("{sv}", clean_val)
                        extra_data[export_name] = price

    return extra_data


def fetch_and_parse_live_event(session, ev, odd_map, market_map, parsing_config):
    """Worker function to process a single live event, including fetching its details."""
    try:
        # Step 1: Parse basic odds from the initial GetLiveOverview response
        base_data = parse_live_event_basic(ev, odd_map, market_map, parsing_config)

        # Step 2: Fetch detailed odds from the GetEventDetails endpoint
        event_id = ev.get("id")
        event_json = fetch_event_details(session, event_id)

        # Step 3: Parse the detailed odds and merge them into the base data
        extra_data = parse_event_details(event_json, parsing_config)
        base_data.update(extra_data)

        return base_data
    except Exception as e:
        print(f"  ↳ Failed to fetch details for live event {ev.get('id')}: {e}")
        return None


def scrape_live(loop, inversed, delay, sport, merge_files, merged_filename):  # NEW: Added merge arguments
    """
    Main live loop using ThreadPoolExecutor for fetching event details concurrently.
    MODIFIED: Deletes files for empty countries.
    NEW: Added logic to merge all scraped data into a single file if enabled.
    """
    parsing_config = load_parsing_config(sport)

    while True:
        session = create_session_with_retries()
        session.get("https://www.clubx2.com/", headers={"User-Agent": HEADERS["User-Agent"]}, timeout=10)

        # NEW: List to hold data from all countries for potential merging
        all_country_data_for_merge = []

        live_params = LIVE_PARAMS_TEMPLATE.copy()
        market_type_ids = _get_live_market_type_ids(parsing_config)
        if market_type_ids:
            live_params["marketTypeIds"] = ",".join(market_type_ids)

        live_json = fetch_json(session, LIVE_OVERVIEW_ENDPOINT, live_params)

        odd_map = {o["id"]: o for o in live_json.get("odds", [])}
        market_map = {m["id"]: m for m in live_json.get("markets", [])}
        for m in live_json.get("markets", []):
            for line in m.get("lines", []): market_map[line["id"]] = line

        all_events = live_json.get("events", [])
        champ_name_map = {c["id"]: c.get("name", str(c["id"])) for c in live_json.get("champs", [])}
        categories = live_json.get("categories", [])
        if inversed: categories = list(reversed(categories))

        out_dir = f"scraped_live_matches/{sport}"
        os.makedirs(out_dir, exist_ok=True)
        new_safe_countries = {re.sub(r"[^\w\-]+", "_", cat.get("name", "").strip()) for cat in categories}

        for filename in os.listdir(out_dir):
            if filename.lower().endswith(".json") and filename[:-5] not in new_safe_countries:
                try:
                    # NEW: Ensure we don't delete the merged file if it's in the same directory
                    if merge_files and filename == merged_filename:
                        continue
                    os.remove(os.path.join(out_dir, filename))
                    print(f"→ Removed old LIVE file: {filename}")
                except OSError:
                    pass

        for cat in categories:
            country_name, safe_country = cat.get("name", "").strip(), re.sub(r"[^\w\-]+", "_",
                                                                             cat.get("name", "").strip())
            out_path = os.path.join(out_dir, f"{safe_country}.json")
            country_data = []

            for champ_id in cat.get("champIds", []):
                t_name = champ_name_map.get(champ_id, str(champ_id))
                matches_in_champ = [ev for ev in all_events if ev.get("champId") == champ_id]
                if not matches_in_champ:
                    continue

                matches_list = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_event = {
                        executor.submit(fetch_and_parse_live_event, session, ev, odd_map, market_map,
                                        parsing_config): ev
                        for ev in matches_in_champ}

                    for future in future_to_event:
                        result = future.result()
                        if result:
                            matches_list.append(result)

                if matches_list:
                    country_data.append(
                        {"tournament_id": int(champ_id), "tournament_name": t_name, "matches": matches_list})

            if country_data:
                current_time = datetime.datetime.now(TUNISIA_TZ)
                timestamp_iso = current_time.isoformat()
                final_data = [{"last_updated": timestamp_iso}] + country_data

                # NEW: Store data for merging if the option is enabled
                if merge_files:
                    all_country_data_for_merge.append({
                        "timestamp": timestamp_iso,
                        "tournaments": country_data
                    })

                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=4)
                print(f"→ Saved LIVE for {country_name} → {out_path}")
            else:
                print(f"→ No LIVE matches for {country_name}, cleaning up.")
                if os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                        print(f"  ↳ Removed old file: {out_path}")
                    except OSError as e:
                        print(f"  ↳ Failed to remove old file {out_path}: {e}")

        # --- NEW: MERGING LOGIC ---
        if merge_files and all_country_data_for_merge:
            print("\n--- Merging all scraped files ---")

            # Find the oldest timestamp among all files
            oldest_timestamp = min(item["timestamp"] for item in all_country_data_for_merge)

            # Combine all tournaments into a single list
            all_tournaments = []
            for item in all_country_data_for_merge:
                all_tournaments.extend(item["tournaments"])

            # Create the final merged data structure
            merged_data = [{"last_updated": oldest_timestamp}] + all_tournaments

            # Save the merged file
            merged_file_path = os.path.join(out_dir, merged_filename)
            with open(merged_file_path, "w", encoding="utf-8") as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=4)
            print(f"✓ All data merged into -> {merged_file_path}")

            # CLEAN UP INDIVIDUAL COUNTRY FILES ---
            print("--- Cleaning up individual files ---")
            for filename in os.listdir(out_dir):
                if filename.lower().endswith(".json") and filename != merged_filename:
                    try:
                        os.remove(os.path.join(out_dir, filename))
                        print(f"  ↳ Removed: {filename}")
                    except OSError as e:
                        print(f"  ↳ Failed to remove {filename}: {e}")
            print("✓ Cleanup complete.")

        elif merge_files:
            print("\n--- No data scraped, skipping merge ---")
        # --- END OF NEW MERGING LOGIC ---

        if not loop: break
        print(f"\n--- Live pass complete. Waiting {delay} seconds before next loop... ---\n")
        time.sleep(delay)


# ---------------------------------------------------
# Entry Point
# ---------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape match data from ClubX2.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--mode",
        choices=["prematch", "live"],
        type=str.lower,
        default=MODE,
        help="The scraping mode:\n"
             "'prematch' - Scrapes upcoming matches.\n"
             "'live'     - Scrapes currently live matches."
    )

    parser.add_argument(
        "--sport",
        type=str,
        default=SPORT,
        help=f"The sport to scrape . Default: {SPORT}",
    )

    parser.add_argument(
        "--loop",
        action="store_true",
        default=LOOP,
        help="Enable loop mode to repeat scraping after each full pass."
    )

    parser.add_argument(
        "--inversed",
        action="store_true",
        default=REVERSE,
        help="Iterate through countries/categories in reverse order."
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=CYCLE_DELAY,
        help=f"Delay in seconds between full passes when loop mode is enabled. Default: {CYCLE_DELAY}"
    )

    # --- NEW ARGUMENTS ---
    parser.add_argument(
        "--merge",
        action="store_true",
        default=MERGE_FILES,
        help="Merge all scraped country files into a single file."
    )

    parser.add_argument(
        "--merged-filename",
        type=str,
        default=MERGED_FILENAME,
        help=f"The name for the merged output file. Default: {MERGED_FILENAME}"
    )
    # --- END OF NEW ARGUMENTS ---

    args = parser.parse_args()

    print(f"--- Starting Scraper ---")
    print(f"Mode: {args.mode.upper()}")
    print(f"Sport: {args.sport.upper()}")
    print(f"Loop: {'Enabled' if args.loop else 'Disabled'}")
    if args.loop:
        print(f"Delay: {args.delay}s")
    print(f"Inversed: {'Enabled' if args.inversed else 'Disabled'}")
    # NEW: Print merge status
    print(f"Merge Files: {'Enabled' if args.merge else 'Disabled'}")
    if args.merge:
        print(f"Merged Filename: {args.merged_filename}")
    print("------------------------\n")

    if args.mode == "prematch":
        # NEW: Pass merge arguments to the function
        scrape_prematch(
            loop=args.loop,
            inversed=args.inversed,
            delay=args.delay,
            sport=args.sport,
            merge_files=args.merge,
            merged_filename=args.merged_filename
        )
    elif args.mode == "live":
        # NEW: Pass merge arguments to the function
        scrape_live(
            loop=args.loop,
            inversed=args.inversed,
            delay=args.delay,
            sport=args.sport,
            merge_files=args.merge,
            merged_filename=args.merged_filename
        )
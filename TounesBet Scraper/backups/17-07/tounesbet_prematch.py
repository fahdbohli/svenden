import os
import json
from curl_cffi import requests
import threading
import time
from datetime import datetime, timezone, timedelta
import random
from bs4 import BeautifulSoup
import urllib3
import sys
import shutil
import re
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import argparse
from pathlib import Path # Added for cleaner path handling


# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Disable warnings about unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global lock for print operations to prevent overlapping output
print_lock = threading.Lock()

# --------------------- Default Scraper Configuration -------------------------------------
SPORT = "football"              # Default sport to scrape. Use --sport to change it.
BONUS = False                    # Default state for the 1.1x bonus. Use --bonus to enable it.
LOOP = False                    # Loop state, use --loop to enable it
DELAY = 5                       # Delay in seconds between each cycle if loop is activated, use --delay to modify it
TOURNAMENTS_PER_REQUEST = 15    # How many tournaments to fetch in a single request (used when SCRAPE_PER_COUNTRY is False)
SCRAPE_PER_COUNTRY = True      # Default strategy. If True, scrapes a whole country in one request. Use --scrapecountry to enable.
NUM_SESSIONS = 30               # Sessions with unique ddos codes number
MAX_WORKERS = 25                 # Workers number
# ---------------------------------------------------------------------------------

# Get Tunisia Timezone : UTC+1
TUNISIA_TZ = timezone(timedelta(hours=1))

def load_sport_id(sport_name):
    """Loads the sport_id from settings/<sport_name>/sport_id.json."""
    path = Path(f"settings/{sport_name}/sport_id.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        sid = int(data["sport_id"])  # or leave as str(data["sport_id"]) if you really need a string
        safe_print(f"Loaded sport_id {sid} from {path}")
        return sid
    except Exception as e:
        safe_print(f"ERROR loading sport_id from {path}: {e}")
        sys.exit(1)

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# --------------------- NEW: Parsing Configuration Manager --------------------------------
class ParsingConfig:
    """Loads and provides access to the parsing configuration from a JSON file."""

    def __init__(self, sport_name):
        self.sport_name = sport_name
        self.config_path = Path(f"settings/{self.sport_name}/parsing.json")
        self.config = self._load_config()

    def _load_config(self):
        """Loads the JSON config file."""
        safe_print(f"Loading parsing configuration from: {self.config_path}")
        if not self.config_path.exists():
            safe_print(f"ERROR: Parsing config file not found at '{self.config_path}'.")
            safe_print("Please ensure the file exists and is correctly formatted.")
            # For robustness, you could create a default file here, but for now, we exit.
            raise FileNotFoundError(f"Parsing configuration not found for sport '{self.sport_name}'")
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            safe_print("Parsing configuration loaded successfully.")
            return config
        except json.JSONDecodeError as e:
            safe_print(f"ERROR: Invalid JSON in {self.config_path}: {e}")
            raise
        except Exception as e:
            safe_print(f"ERROR: Could not read config file {self.config_path}: {e}")
            raise

    def get_main_markets(self):
        """Returns the list of main market parsing rules."""
        return self.config.get("main_markets", [])

    def get_special_markets(self):
        """Returns the list of special market parsing rules."""
        return self.config.get("special_markets", [])

# ------------------------------------------------------------------------------------------

class SessionManager:
    """Manages a pool of sessions with different DDoS protection codes."""

    def __init__(self, num_sessions=10, retry_attempts=3):
        self.sessions = []
        self.headers_post = []
        self.session_lock = threading.Lock()
        self.current_index = 0
        self.retry_attempts = retry_attempts

        safe_print(f"Initializing {num_sessions} sessions with unique DDoS protection codes...")
        for i in range(num_sessions):
            session, headers = self._create_new_session()
            if session and headers:
                self.sessions.append(session)
                self.headers_post.append(headers)
                safe_print(f"Session {i + 1}/{num_sessions} initialized")
            time.sleep(0.2)  # Longer delay between session creation to avoid detection

        safe_print(f"Successfully created {len(self.sessions)} sessions")

    def _create_new_session(self):
        """Create a new session with a unique DDoS protection code by impersonating a browser."""
        # This now creates a session object from the curl_cffi library
        session = requests.Session()

        # The Retry and HTTPAdapter logic is removed as curl_cffi handles connections differently.

        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        ]
        user_agent = random.choice(user_agents)

        headers_get = {
            "Host": "tounesbet.com",
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        try:
            # First GET request to retrieve the REAL page by impersonating a browser
            url_get = "https://tounesbet.com/"

            # The crucial 'impersonate' parameter is added here
            response_get = session.get(
                url_get,
                headers=headers_get,
                verify=False,
                timeout=45,
                impersonate="chrome120"  # This is the key to the solution
            )

            # Extract the DDoS_Protection value using a regular expression
            match = re.search(r'DDoS_Protection=([0-9a-f]+)', response_get.text)
            if match:
                ddos_value = match.group(1)
                session.cookies.set("DDoS_Protection", ddos_value, domain="tounesbet.com", path="/")

                headers_post = {
                    "Host": "tounesbet.com",
                    "Cookie": f"_culture=en-us; TimeZone=-60; DDoS_Protection={ddos_value}",
                    "Content-Length": "0",
                    "X-Requested-With": "XMLHttpRequest",
                    "User-Agent": user_agent,
                    "Accept": "*/*",
                    "Origin": "https://tounesbet.com",
                    "Referer": "https://tounesbet.com/?d=1",
                }
                return session, headers_post
            else:
                safe_print("DDoS_Protection cookie not found in the GET response.")
                return None, None

        except Exception as e:
            safe_print(f"Error creating session: {str(e)}")
            return None, None

    def _generate_random_cookie(self):
        """Generate a random cookie value to mimic browser behavior."""
        import base64
        random_bytes = bytearray(random.getrandbits(8) for _ in range(32))
        return base64.b64encode(random_bytes).decode('utf-8')

    def get_session(self):
        """Get a session from the pool in a round-robin fashion."""
        with self.session_lock:
            if not self.sessions:
                # If all sessions failed, create a new one on-the-fly
                session, headers = self._create_new_session()
                if not session:
                    raise Exception("Failed to create a new session")
                return session, headers

            session = self.sessions[self.current_index]
            headers = self.headers_post[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.sessions)
            return session, headers

    def add_session(self):
        """Add a new session to the pool."""
        with self.session_lock:
            session, headers = self._create_new_session()
            if session and headers:
                self.sessions.append(session)
                self.headers_post.append(headers)
                return True
            return False


def get_country_list(session, headers_post, sport_id, max_retries=3):
    """
    Step 1.
    Send a POST request to the SportCategory endpoint and extract each country with its sport category id.
    Returns a list of dicts: {"country_name", "sportcategory_id"}.
    """
    url = f"https://tounesbet.com/SportCategory?SportId={sport_id}&BetRangeFilter=0&DateDay=all_days"
    safe_print(f"Requesting countries at: {url}")

    # Implement retry logic
    for attempt in range(max_retries):
        try:
            response = session.post(url, headers=headers_post, verify=False, timeout=45)
            soup = BeautifulSoup(response.text, "html.parser")
            countries = []
            for div in soup.find_all("div", class_="divSportCategory"):
                a_tag = div.find("a", class_="sportcategory_item")
                if not a_tag:
                    continue
                span = a_tag.find("span", class_="menu-sport-name")
                if not span:
                    continue
                country_name = span.get_text(strip=True)
                sportcategory_id = a_tag.get("data-sportcategoryid")
                safe_print(f"Found country: {country_name} with sportcategory id: {sportcategory_id}")
                countries.append({"country_name": country_name, "sportcategory_id": sportcategory_id})
            return countries

        except Exception as e:
            safe_print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = (attempt + 1) * 5  # Progressive backoff
                safe_print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                safe_print("Maximum retries reached. Could not fetch countries.")
                return []


def get_tournaments(session, headers_post, sport_id, sportcategory_id, max_retries=3):
    """
    Step 2.
    Using a given country's sport category id, post to the Tournaments endpoint and extract tournaments.
    Returns a list of dicts: {"tournament_name", "tournament_id"}.
    """
    url = (
        f"https://tounesbet.com/Tournaments?"
        f"SportId={sport_id}&SportCategoryId={sportcategory_id}&BetRangeFilter=0&DateDay=all_days"
    )

    for attempt in range(max_retries):
        try:
            safe_print(f"Requesting tournaments at: {url}")
            response = session.post(url, headers=headers_post, verify=False, timeout=45)
            soup = BeautifulSoup(response.text, "html.parser")
            tournaments = []
            for div in soup.find_all("div", class_="divTournament"):
                a_tag = div.find("a", class_="tournament_item")
                if not a_tag:
                    continue
                span = a_tag.find("span", class_="menu-sport-name")
                if not span:
                    continue
                tournament_name = span.get_text(strip=True)
                tournament_id = a_tag.get("data-tournamentid")
                safe_print(f"  Found tournament: {tournament_name} with id: {tournament_id}")
                tournaments.append({"tournament_name": tournament_name, "tournament_id": tournament_id})
            return tournaments

        except Exception as e:
            safe_print(f"Attempt {attempt + 1}/{max_retries} to get tournaments failed: {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = (attempt + 1) * 5
                safe_print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                safe_print("Maximum retries reached. Could not fetch tournaments.")
                return []


def process_odd(value):
    """
    - Cleans the input string (removes non-digits except . and ,; converts ',' to '.')
    - Parses it as Decimal
    - If > 1.5, multiplies by 1.1
    - Truncates (not rounds) to 3 decimal places
    - Returns a float, or None if parsing fails
    """
    try:
        # 1) clean input
        cleaned = re.sub(r"[^\d.,]", "", value).replace(",", ".")
        odd = Decimal(cleaned)
        # 2) conditionally apply margin
        if BONUS and odd > Decimal("1.5"):
            odd *= Decimal("1.1")
        # 3) truncate to 3 decimal places
        odd = odd.quantize(Decimal("0.001"), rounding=ROUND_DOWN)
        # 4) return as float so json.dump emits a number
        return float(odd)
    except (InvalidOperation, TypeError):
        return None


# --- REFACTORED: extract_matches is now data-driven ---
def extract_matches(html, parsing_config):
    """
    Extracts main market odds based on the provided parsing configuration.
    Returns a list of match dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    matches = []
    tbody = soup.find("tbody", id="matchesTableBody")
    if not tbody:
        return matches

    main_market_configs = parsing_config.get_main_markets()

    current_date = ""
    for tr in tbody.find_all("tr", recursive=False):
        classes = tr.get("class", [])
        if "prematch-header-row" in classes:
            span = tr.find("span")
            current_date = span.get_text(strip=True) if span else current_date
        elif "trMatch" in classes:
            # Basic match info
            match_id = tr.get("data-matchid", "").strip()
            tournament_id = tr.get("data-tournamentid", "").strip()
            time_td = tr.find("td", class_="tdMatch")
            time_div = time_td.find("div") if time_td else None
            match_time = time_div.get_text(strip=True) if time_div else ""
            comp1 = tr.find("div", class_="competitor1-name")
            comp2 = tr.find("div", class_="competitor2-name")
            home_team = comp1.get_text(strip=True).replace("ü", "ü") if comp1 else ""
            away_team = comp2.get_text(strip=True).replace("ü", "ü") if comp2 else ""

            match = {
                "match_id": match_id,
                "tournament_id": tournament_id,
                "date": current_date,
                "time": match_time,
                "home_team": home_team,
                "away_team": away_team,
            }

            # --- Data-driven odds extraction ---
            for market_config in main_market_configs:
                td_marker_class = market_config.get("marker_class")
                odds_to_extract = market_config.get("odds", [])

                td = tr.find("td", class_=f"betColumn {td_marker_class}")
                if not td:
                    continue

                # Filter out the span that shows the handicap (e.g., "2.5")
                spans = [s for s in td.find_all("span") if not s.get("data-spreadcount")]

                for odd_config in odds_to_extract:
                    output_name = odd_config.get("output_name")
                    index = odd_config.get("index")
                    if output_name is not None and index is not None and index < len(spans):
                        odd_value_str = spans[index].get_text(strip=True)
                        match[output_name] = process_odd(odd_value_str)

            matches.append(match)
    return matches


# --- REFACTORED: Generic function for special markets (replaces extract_total_lines and extract_handicap_lines) ---
def extract_special_market_lines(html, market_config):
    """
    Extracts special market lines (Totals, Handicaps) based on the provided configuration.
    Returns a dictionary mapping match_id to a dict of its odds.
    """
    soup = BeautifulSoup(html, "html.parser")
    match_odds_data = {}
    tbody = soup.find("tbody", id="matchesTableBody")
    if not tbody:
        return match_odds_data

    market_type = market_config.get("type")
    outcome_mapping_raw = market_config.get("outcome_mapping", [])
    # Create a quick lookup dictionary for outcome IDs
    outcome_map = {item['id']: item['name_template'] for item in outcome_mapping_raw}

    for tr in tbody.find_all("tr", class_="trMatch"):
        match_id = tr.get("data-matchid")
        if not match_id:
            continue

        match_odds_data[match_id] = {}
        bet_column = tr.find("td", class_="betColumn")
        if not bet_column:
            continue

        line_spans = bet_column.find_all("span", {"data-spreadcount": True, "class": "special-bet-prematch"})

        for line_span in line_spans:
            spread_no = line_span.get('data-spreadno')
            line_text_raw = line_span.get_text(strip=True)

            # Find the odd spans associated with this line
            odd_spans = bet_column.find_all("span", {"data-spreadno": spread_no, "class": "match_odd"})
            if len(odd_spans) != 2:
                continue

            # --- Logic to parse the line value (differs by market type) ---
            parsed_lines = {}
            if market_type == "totals":
                line_val = re.sub(r"[^\d.]", "", line_text_raw)
                parsed_lines = {outcome_id: line_val for outcome_id in outcome_map.keys()}
            elif market_type == "handicap":
                handicap_match = re.match(r"([-\d.]+)", line_text_raw)
                if handicap_match:
                    home_line = handicap_match.group(1)
                    try:
                        away_line = f"{-Decimal(home_line):g}" # Calculate away line
                        parsed_lines = {"1": home_line, "2": away_line}
                    except InvalidOperation:
                        continue
            else:
                continue # Skip unknown market types

            # --- Generic logic to extract odds using the outcome map ---
            for odd_span in odd_spans:
                outcome_id = odd_span.get('data-outcomeid', '').lower()
                name_template = outcome_map.get(outcome_id)
                line_value = parsed_lines.get(outcome_id)

                if name_template and line_value is not None:
                    odd_val = process_odd(odd_span.get_text(strip=True))
                    if odd_val is not None:
                        output_key = name_template.format(line=line_value)
                        match_odds_data[match_id][output_key] = odd_val

    return match_odds_data


def chunk_list(data, size):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(data), size):
        yield data[i:i + size]


# --- UPDATED: process_country now accepts and uses parsing_config ---
def process_country(country_info):
    """Process a single country: fetch all odds types and save combined data."""
    country, session_manager, sport_id, base_output_dir, tournaments_per_request, parsing_config, scrape_per_country = country_info
    country_name = country["country_name"]
    sportcategory_id = country["sportcategory_id"]

    safe_print(f"\nProcessing country: {country_name} (SportCategoryId: {sportcategory_id})")

    try:
        session, headers = session_manager.get_session()
        time.sleep(random.uniform(1, 3))

        # We always get the initial tournament list. In per-country mode, it's a "best-effort" name map.
        # In per-tournament mode, it's the list we iterate through.
        initial_tournaments = get_tournaments(session, headers, sport_id, sportcategory_id)
        safe_print(f"Found {len(initial_tournaments)} initial tournaments for {country_name}")

        if not initial_tournaments and not scrape_per_country:
             # If no tournaments are found AND we are in the old mode, we can exit early.
             # In the new mode, the main country page might still have matches, so we continue.
            safe_country_name = re.sub(r'[\\/*?:"<>|]', "", country_name).replace(" ", "_")
            output_file = os.path.join(base_output_dir, f"{safe_country_name}.json")
            current_time = datetime.now(TUNISIA_TZ).isoformat()
            with open(output_file, "w", encoding="utf-8") as outfile:
                json.dump([{"last_updated": current_time}], outfile, indent=4, ensure_ascii=False)
            safe_print(f"No tournaments found for {country_name}. Empty data saved to {output_file}")
            return

        all_matches_data = {}

        if scrape_per_country:
            # --- NEW LOGIC: Scrape whole country at once ---
            safe_print(f"Scraping all tournaments for {country_name} in a single request (--scrapecountry).")
            session, headers = session_manager.get_session()
            tournaments_to_fetch_per_page = 500
            base_url = (f"https://tounesbet.com/Sport/{sport_id}/Category/{sportcategory_id}?"
                        f"DateDay=all_days&BetRangeFilter=0&Page_number=1&Tournament_per_page={tournaments_to_fetch_per_page}")
            try:
                response_main = session.post(base_url, headers=headers, verify=False, timeout=120)
                matches_from_main = extract_matches(response_main.text, parsing_config)
                for match in matches_from_main:
                    all_matches_data[match['match_id']] = match
                safe_print(f"    -> Found {len(matches_from_main)} matches with main odds.")
            except Exception as e:
                safe_print(f"    ERROR fetching main odds for country {country_name}: {str(e)}")
            time.sleep(random.uniform(0.5, 1.5))
            for market_config in parsing_config.get_special_markets():
                market_desc, odd_type_id = market_config.get('description', 'special market'), market_config.get('odd_type_id')
                safe_print(f"  Fetching {market_desc} (oddType={odd_type_id}) for the entire country...")
                url_special = f"{base_url}&onlyOddType={odd_type_id}"
                try:
                    response_special = session.post(url_special, headers=headers, verify=False, timeout=120)
                    special_lines_data = extract_special_market_lines(response_special.text, market_config)
                    if special_lines_data:
                        for match_id, odds in special_lines_data.items():
                            if match_id in all_matches_data:
                                if market_config.get('type') == 'totals':
                                    all_matches_data[match_id].pop("under_2.5_odd", None)
                                    all_matches_data[match_id].pop("over_2.5_odd", None)
                                all_matches_data[match_id].update(odds)
                        safe_print(f"    -> Found and merged {market_desc} for {len(special_lines_data)} matches.")
                except Exception as e:
                    safe_print(f"    ERROR fetching {market_desc} for country {country_name}: {str(e)}")
                time.sleep(random.uniform(0.5, 1.5))
        else:
            # --- ORIGINAL LOGIC: Scrape by tournament chunks ---
            all_tournament_ids = [t['tournament_id'] for t in initial_tournaments]
            tournament_id_chunks = list(chunk_list(all_tournament_ids, TOURNAMENTS_PER_REQUEST))
            safe_print(f"Fetching matches for {len(all_tournament_ids)} tournaments in {len(tournament_id_chunks)} chunks.")
            for i, chunk in enumerate(tournament_id_chunks):
                session, headers = session_manager.get_session()
                ids_string, chunk_size = ",".join(chunk), len(chunk)
                base_url = (f"https://tounesbet.com/Sport/{sport_id}/Category/{sportcategory_id}/TournamentIds/{ids_string}?"
                            f"DateDay=all_days&BetRangeFilter=0&Page_number=1&Tournament_per_page={chunk_size}")
                safe_print(f"  Chunk {i + 1}/{len(tournament_id_chunks)}: Fetching main odds...")
                try:
                    response_main = session.post(base_url, headers=headers, verify=False, timeout=60)
                    matches_from_main = extract_matches(response_main.text, parsing_config)
                    for match in matches_from_main:
                        all_matches_data[match['match_id']] = match
                    safe_print(f"    -> Found {len(matches_from_main)} matches with main odds.")
                except Exception as e:
                    safe_print(f"    ERROR fetching main odds for chunk {i + 1}: {str(e)}")
                time.sleep(random.uniform(0.5, 1.5))
                for market_config in parsing_config.get_special_markets():
                    market_desc, odd_type_id = market_config.get('description', 'special market'), market_config.get('odd_type_id')
                    safe_print(f"  Chunk {i + 1}/{len(tournament_id_chunks)}: Fetching {market_desc} (oddType={odd_type_id})...")
                    url_special = f"{base_url}&onlyOddType={odd_type_id}"
                    try:
                        response_special = session.post(url_special, headers=headers, verify=False, timeout=60)
                        special_lines_data = extract_special_market_lines(response_special.text, market_config)
                        if special_lines_data:
                            for match_id, odds in special_lines_data.items():
                                if match_id in all_matches_data:
                                    if market_config.get('type') == 'totals':
                                        all_matches_data[match_id].pop("under_2.5_odd", None)
                                        all_matches_data[match_id].pop("over_2.5_odd", None)
                                    all_matches_data[match_id].update(odds)
                            safe_print(f"    -> Found and merged {market_desc} for {len(special_lines_data)} matches.")
                    except Exception as e:
                        safe_print(f"    ERROR fetching {market_desc} for chunk {i + 1}: {str(e)}")
                    time.sleep(random.uniform(0.5, 1.5))

        # --- REVISED STRUCTURING LOGIC ---
        # Group all found matches by their tournament ID
        grouped_matches = {}
        for match_id, match_data in all_matches_data.items():
            tid = match_data.get('tournament_id')
            if tid:
                if tid not in grouped_matches:
                    grouped_matches[tid] = []
                grouped_matches[tid].append(match_data)

        country_data = []
        if scrape_per_country:
            # For per-country mode, iterate over the *found* tournaments to build the final list.
            # Create a name map from the initial fetch for quick lookups.
            tournament_name_map = {t['tournament_id']: t['tournament_name'] for t in initial_tournaments}
            for t_id, matches in grouped_matches.items():
                tournament_name = tournament_name_map.get(t_id, f"Unknown Tournament (ID: {t_id})")
                country_data.append({
                    "tournament_name": tournament_name,
                    "tournament_id": t_id,
                    "matches": matches
                })
        else:
            # For the original mode, iterate over the initial list to maintain order and structure.
            for t in initial_tournaments:
                t_id = t['tournament_id']
                country_data.append({
                    "tournament_name": t['tournament_name'],
                    "tournament_id": t_id,
                    "matches": grouped_matches.get(t_id, [])
                })

        current_time = datetime.now(TUNISIA_TZ).isoformat()
        data_with_timestamp = [{"last_updated": current_time}] + country_data
        safe_country_name = re.sub(r'[\\/*?:"<>|]', "", country_name).replace(" ", "_")
        output_file = os.path.join(base_output_dir, f"{safe_country_name}.json")
        with open(output_file, "w", encoding="utf-8") as outfile:
            json.dump(data_with_timestamp, outfile, indent=4, ensure_ascii=False)
        safe_print(f"Data for country {country_name} saved to {output_file}")

    except Exception as e:
        safe_print(f"An unexpected error occurred while processing country {country_name}: {e}")
        import traceback
        traceback.print_exc()


def main():
    global BONUS, SCRAPE_PER_COUNTRY
    # 1. DEFINE AND PARSE COMMAND-LINE ARGUMENTS
    parser = argparse.ArgumentParser(
        description="Scrape match data from tounesbet.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--sport', type=str, default=SPORT,
        help='The sport to scrape. A config must exist at settings/{sport}/parsing.json'
    )
    parser.add_argument(
        '--bonus', action=argparse.BooleanOptionalAction, default=BONUS,
        help='Enable the 1.1x odds bonus for odds > 1.5.'
    )
    parser.add_argument(
        '--loop', action='store_true', default=LOOP,
        help='Run the scraper in a continuous loop.'
    )
    parser.add_argument(
        '--delay', type=int, default=DELAY,
        help='Delay in seconds between scrape cycles when --loop is enabled.'
    )
    parser.add_argument(
        '--scrapecountry', action='store_true', default=SCRAPE_PER_COUNTRY,
        help='Scrape one whole country per request instead of chunking tournaments.'
    )
    args = parser.parse_args()

    # 2. UPDATE GLOBAL CONFIGURATION FROM ARGS
    BONUS = args.bonus
    SCRAPE_PER_COUNTRY = args.scrapecountry
    sport_name = args.sport.lower()

    # --- UPDATED: Load parsing config and Sport ID ---
    try:
        parsing_config = ParsingConfig(sport_name)
    except (FileNotFoundError, json.JSONDecodeError):
        safe_print("Exiting due to parsing configuration error.")
        return

    # Load the sport_id dynamically from settings/<sport>/sport_id.json
    sport_id = load_sport_id(sport_name)

    # 3. MAIN EXECUTION LOOP
    while True:
        base_output_dir = f"scraped_prematch_matches/{sport_name}"
        num_sessions = NUM_SESSIONS
        max_workers = MAX_WORKERS
        tournaments_per_request = TOURNAMENTS_PER_REQUEST

        print("\n=== Scraper Configuration ===")
        print(f"Sport: {sport_name} (ID: {sport_id})")
        print(f"Bonus enabled: {BONUS}")
        print(f"Scraping per country: {SCRAPE_PER_COUNTRY}")
        print(f"Loop enabled: {args.loop}")
        if args.loop: print(f"Delay between loops: {args.delay} seconds")
        print(f"Number of concurrent sessions: {num_sessions}")
        print(f"Number of parallel country threads: {max_workers}")
        if not SCRAPE_PER_COUNTRY:
            print(f"Tournaments per request: {tournaments_per_request}")

        session_manager = SessionManager(num_sessions=num_sessions, retry_attempts=3)

        if len(session_manager.sessions) == 0:
            safe_print("Failed to create any valid sessions. Exiting.")
            return

        session, headers = session_manager.get_session()
        safe_print("Fetching countries...")
        countries = get_country_list(session, headers, sport_id)
        safe_print(f"Found {len(countries)} countries.")

        if not countries:
            if not args.loop:
                return
            safe_print(f"No countries found. Retrying after {args.delay} seconds...")
            time.sleep(args.delay)
            continue

        os.makedirs(base_output_dir, exist_ok=True)

        def create_safe_filename(name):
            return re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_")

        expected_filenames = {f"{create_safe_filename(c['country_name'])}.json" for c in countries}
        try:
            existing_files = {f for f in os.listdir(base_output_dir) if f.endswith('.json')}
            files_to_delete = existing_files - expected_filenames
            if files_to_delete:
                safe_print(f"Found {len(files_to_delete)} outdated file(s) to remove.")
                for filename in files_to_delete:
                    os.remove(os.path.join(base_output_dir, filename))
                    safe_print(f"  - Removed: {filename}")
        except OSError as e:
            safe_print(f"Could not read directory '{base_output_dir}' for cleanup: {e}")

        actual_workers = min(max_workers, len(countries))
        safe_print(f"Using {actual_workers} worker threads to process {len(countries)} countries")

        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # --- UPDATED: Pass parsing_config and scrape_per_country to the worker ---
            process_args = [
                (country, session_manager, sport_id, base_output_dir, tournaments_per_request, parsing_config, SCRAPE_PER_COUNTRY)
                for country in countries
            ]
            list(executor.map(process_country, process_args))

        safe_print("Scraping cycle completed.")

        if not args.loop:
            break

        safe_print(f"\nLoop mode is active. Waiting for {args.delay} seconds before the next run.")
        time.sleep(args.delay)


if __name__ == "__main__":
    main()
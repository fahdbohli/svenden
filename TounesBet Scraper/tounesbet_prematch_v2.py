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
from pathlib import Path
import unicodedata


# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Disable warnings about unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global lock for print operations to prevent overlapping output
print_lock = threading.Lock()

# --------------------- Default Scraper Configuration -------------------------------------
SPORT = "football"              # Default sport to scrape. Use --sport to change it.
BONUS = True                    # Default state for the bonus. Use --bonus to enable it.
LOOP = False                    # Loop state, use --loop to enable it
DELAY = 5                       # Delay in seconds between each cycle if loop is activated, use --delay to modify it
TOURNAMENTS_PER_REQUEST = 15    # How many tournaments to fetch in a single request (used when SCRAPE_PER_COUNTRY is False)
SCRAPE_PER_COUNTRY = True       # Default strategy. If True, scrapes a whole country in one request. Use --scrapecountry to enable.
SCRAPE_BY_ID = False            # If True (and SCRAPE_PER_COUNTRY is True), scrapes match-by-match. Use --scrape-by-id to enable.
TARGET = "all"                  # Target specific match IDs when SCRAPE_BY_ID is enabled. Use --target.
NUM_SESSIONS = 30               # Sessions with unique ddos codes number
MAX_WORKERS = 25                # Workers number
BONUS_RATIO = "1.18"            # Bonus ratio value (x1.1, x1.15, ...)
MINIMUM_BONUS_ODD = "1.5"       # the minimum odd value for the bonus to apply
MERGE_FILES = False             # Set to True to merge all country files into one. Use --merge-files to override.
MERGED_FILENAME = "merged.json" # The filename for the merged output. Use --merged-filename to override.
# ---------------------------------------------------------------------------------

# ----------------- Presets for default configuration ------------------------
PRESET = "football"
# -----------------------------------------------------------------------------

# Preset configuration application
if PRESET == "tennis":
    SPORT = "tennis"
    SCRAPE_BY_ID = True
    MERGE_FILES = True
if PRESET == "football":
    SPORT = "football"
    SCRAPE_BY_ID = False
    MERGE_FILES = False

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

    def __init__(self, sport_name, scrape_by_id_enabled=False): # MODIFIED: Add new argument
        self.sport_name = sport_name
        # MODIFIED: Logic to select the correct parsing file
        if scrape_by_id_enabled:
            config_filename = "single_match_parsing.json"
        else:
            config_filename = "parsing.json"
        self.config_path = Path(f"settings/{self.sport_name}/{config_filename}")
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
                try:
                    sportcategory_id = int(a_tag.get("data-sportcategoryid", "0"))
                    if sportcategory_id == 0: continue # Skip if ID is missing/invalid
                except (ValueError, TypeError):
                    # Skip this country if the ID is not a valid integer
                    continue

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
                try:
                    tournament_id = int(a_tag.get("data-tournamentid", "0"))
                    if tournament_id == 0: continue # Skip if ID is missing/invalid
                except (ValueError, TypeError):
                    # Skip this tournament if the ID is not a valid integer
                    continue

                safe_print(f"  Found tournament: {tournament_name} with id: {tournament_id}")
                tournaments.append({"tournament_name": tournament_name, "tournament_id": tournament_id})
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


def parse_single_match_odds(html: str, config: dict) -> dict:
    """
    Parses single match odds from an HTML string based on a provided configuration.
    This version uses fully customizable templates from the config for total/handicap odds,
    allowing flexible output key naming.
    """
    if not config:
        safe_print("Error: Parsing cannot proceed without a valid configuration.")
        return {}

    soup = BeautifulSoup(html, 'html.parser')
    odds = {}

    def norm_text(text):
        """
        Normalize text: replace non-breaking spaces, handle unicode, lowercase,
        and standardize whitespace.
        """
        if not text:
            return ""
        text = text.replace('\xa0', ' ')
        s = unicodedata.normalize('NFKD', text.lower()).encode('ascii', 'ignore').decode('utf-8')
        return ' '.join(s.strip().split())

    # Pre-compute a map for faster lookups
    title_to_config_map = {}
    for market_type, market_list in config.items():
        if market_type not in ['regular', 'total', 'handicap']:
            continue
        for conf in market_list:
            for title in conf.get('market_titles', []):
                title_to_config_map[title] = (conf, market_type)

    # State variables
    current_market_config = None
    current_market_type = None

    for row in soup.select('.odd_type_group_body .divOddRow'):
        market_name_elem = row.select_one('.oddName span')
        if market_name_elem:
            norm_title = norm_text(market_name_elem.get_text(strip=True))
            config_tuple = title_to_config_map.get(norm_title)
            if config_tuple:
                current_market_config, current_market_type = config_tuple
            else:
                current_market_config, current_market_type = None, None

        if not current_market_config:
            continue

        if current_market_type == 'regular':
            outcome_map = current_market_config.get('outcomes', {})
            for mo in row.select('.match-odd[data-isactive="True"]'):
                label_elem = mo.select_one('.outcome-label-multirow')
                val_elem = mo.select_one('.quoteValue')
                if label_elem and val_elem and val_elem.has_attr('data-oddvaluedecimal'):
                    label = label_elem.get_text(strip=True)
                    if label in outcome_map:
                        output_key = outcome_map[label]
                        odds[output_key] = process_odd(val_elem['data-oddvaluedecimal'])

        elif current_market_type in ['total', 'handicap']:
            # Get the new templates from the config.
            templates = current_market_config.get("outcome_templates", {})
            if not templates:
                continue

            for odds_table in row.select('.divOddsTable'):
                line_elem = odds_table.select_one('.divOddSpecial label')
                if not line_elem: continue
                line_raw = line_elem.get_text(strip=True)

                try:
                    line_val = Decimal(line_raw.replace(',', '.'))
                except (InvalidOperation, TypeError):
                    continue

                for odd_div in odds_table.select('.divOdd.has-specialBet-col'):
                    mo = odd_div.select_one('.match-odd[data-isactive="True"]')
                    if not mo: continue

                    label_elem = mo.select_one('label')
                    val_elem = mo.select_one('.quoteValue')

                    if not (label_elem and val_elem and val_elem.has_attr('data-oddvaluedecimal')):
                        continue

                    label = label_elem.get_text(strip=True).lower()
                    value = process_odd(val_elem['data-oddvaluedecimal'])
                    if value is None: continue

                    # Check if there is a template for the found label (e.g., 'over', '1')
                    template_string = templates.get(label)
                    if not template_string:
                        continue

                    # Determine which line value to use in the template
                    line_to_format = f"{line_val:g}"  # Default case for totals

                    if current_market_type == 'handicap':
                        if label == '1':  # Home team
                            line_to_format = f"{line_val:g}"
                        elif label == '2':  # Away team - use the inverted line
                            line_to_format = f"{-line_val:g}"

                    # Generate the final key using the template from the JSON file
                    output_key = template_string.format(line=line_to_format)
                    odds[output_key] = value
    return odds


def process_odd(value):
    """
    - Cleans the input string (removes non-digits except . and ,; converts ',' to '.')
    - Parses it as Decimal
    - If > Minimum bonus odd, multiplies by bonus ratio
    - Truncates (not rounds) to 3 decimal places
    - Returns a float, or None if parsing fails
    """
    try:
        # 1) clean input
        cleaned = re.sub(r"[^\d.,]", "", value).replace(",", ".")
        odd = Decimal(cleaned)
        # 2) conditionally apply margin
        if BONUS and odd > Decimal(MINIMUM_BONUS_ODD):      # lefhid
            odd *= Decimal(BONUS_RATIO)
        # 3) truncate to 3 decimal places
        odd = odd.quantize(Decimal("0.001"), rounding=ROUND_DOWN)
        # 4) return as float so json.dump emits a number
        return float(odd)
    except (InvalidOperation, TypeError):
        return None


# --- REFACTORED: extract_matches is now data-driven ---
def extract_matches(html, parsing_config):
    """
    Extracts matches and tournament names from the main page content.
    Gracefully handles a minimal config when only basic details are needed.
    """
    soup = BeautifulSoup(html, "html.parser")
    matches = []
    scraped_tournament_names = {}
    tbody = soup.find("tbody", id="matchesTableBody")
    if not tbody:
        return matches, scraped_tournament_names

    # This will be an empty list if using the minimal config, which is fine.
    main_market_configs = parsing_config.get_main_markets()
    current_date = ""

    for tr in tbody.find_all("tr", recursive=False):
        classes = tr.get("class", [])

        if "header_tournament_row" in classes:
            t_id = tr.get("data-tournamentid")
            name_td = tr.find("td", class_="tournament_name_section")
            if t_id and name_td:
                name_div = name_td.find("div", class_="category-tournament-title")
                if name_div:
                    full_name = name_div.get_text(strip=True)
                    tournament_name = full_name.split('/')[-1].strip()
                    if tournament_name:
                        scraped_tournament_names[t_id] = tournament_name

        elif "prematch-header-row" in classes:
            span = tr.find("span")
            current_date = span.get_text(strip=True) if span else current_date

        elif "trMatch" in classes:
            try:
                match_id = int(tr.get("data-matchid", "0").strip())
                tournament_id = int(tr.get("data-tournamentid", "0").strip())
            except (ValueError, TypeError):
                # If conversion fails for any reason, skip this match row
                # as it lacks essential IDs.
                continue
            time_td = tr.find("td", class_="tdMatch")
            time_div = time_td.find("div") if time_td else None
            match_time = time_div.get_text(strip=True) if time_div else ""
            comp1 = tr.find("div", class_="competitor1-name")
            comp2 = tr.find("div", class_="competitor2-name")
            home_team = comp1.get_text(strip=True) if comp1 else ""
            away_team = comp2.get_text(strip=True) if comp2 else ""

            match = {
                "match_id": match_id, "tournament_id": tournament_id, "date": current_date,
                "time": match_time, "home_team": home_team, "away_team": away_team,
            }

            # If main_market_configs is empty (from our minimal config), this loop is simply skipped.
            if main_market_configs:
                for market_config in main_market_configs:
                    td_marker_class = market_config.get("marker_class")
                    odds_to_extract = market_config.get("odds", [])
                    td = tr.find("td", class_=f"betColumn {td_marker_class}")
                    if not td: continue
                    spans = [s for s in td.find_all("span") if not s.get("data-spreadcount")]
                    for odd_config in odds_to_extract:
                        output_name, index = odd_config.get("output_name"), odd_config.get("index")
                        if output_name is not None and index is not None and index < len(spans):
                            match[output_name] = process_odd(spans[index].get_text(strip=True))

            # Only add the match if it has an ID, which is crucial for scrape-by-id mode
            if match_id:
                matches.append(match)

    return matches, scraped_tournament_names


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
        try:
            # --- THIS IS THE CORRECTED LINE ---
            match_id = int(tr.get("data-matchid", "0"))
        except (ValueError, TypeError):
            # If ID is missing or not a number, skip this row
            continue
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


# --- NEW: Worker function for scraping a single match's odds ---
def scrape_single_match_worker(args):
    """
    Worker function designed for a ThreadPoolExecutor.
    Scrapes the odds for a single match and returns the updated match dictionary.
    """
    # Unpack arguments
    match_details, index, total_matches, session_manager, parsing_config, country_name = args

    match_id = match_details.get("match_id")
    if not match_id:
        return None  # Cannot process without an ID

    # Use a more descriptive print statement to see parallel execution
    safe_print(f"  [{country_name}] Scraping match {index + 1}/{total_matches} (ID: {match_id})")

    try:
        session, headers = session_manager.get_session()
        match_odds_url = f"https://tounesbet.com/Match/MatchOddsGrouped?matchId={match_id}"
        response_match = session.post(match_odds_url, headers=headers, verify=False, timeout=60)

        # Parse the odds using the single-match configuration
        match_odds = parse_single_match_odds(response_match.text, parsing_config.config)

        # Merge the scraped odds into the base match details
        match_details.update(match_odds)

        # A smaller delay is fine here since requests are parallel
        time.sleep(random.uniform(0.2, 0.5))

        return match_details

    except Exception as e:
        safe_print(f"    - ERROR scraping odds for match ID {match_id} in {country_name}: {e}")
        return None  # Return None on failure


# --- UPDATED: process_country now accepts and uses parsing_config ---
# --- UPDATED: process_country now uses a nested ThreadPoolExecutor for scrape-by-id mode ---
def process_country(country_info):
    """
    Process a single country by fetching match data and saving it.
    Supports three modes:
    1. SCRAPE_BY_ID: Fetches a list of matches, then scrapes odds for each one INDIVIDUALLY AND IN PARALLEL.
    2. SCRAPE_PER_COUNTRY: Fetches all matches and odds for a country in a few large requests.
    3. TOURNAMENT CHUNKS: Fetches matches and odds in chunks of tournaments (legacy mode).
    """
    # Unpack all arguments passed from the main thread pool
    try:
        (country, session_manager, sport_id, base_output_dir, tournaments_per_request,
         main_parsing_config, single_match_parsing_config, scrape_per_country, scrape_by_id, target) = country_info
    except ValueError:
        safe_print("FATAL ERROR: Mismatch in arguments passed to process_country worker. Exiting thread.")
        return

    country_name = country["country_name"]
    sportcategory_id = country["sportcategory_id"]

    safe_print(f"\nProcessing country: {country_name} (SportCategoryId: {sportcategory_id})")

    try:
        session, headers = session_manager.get_session()
        time.sleep(random.uniform(1, 3))

        all_matches_data = {}
        scraped_tournament_names = {}

        # --- REVISED LOGIC BRANCH: SCRAPE BY INDIVIDUAL MATCH ID (NOW IN PARALLEL) ---
        if scrape_per_country and scrape_by_id:
            safe_print(f"Scraping {country_name} using parallel SCRAPE-BY-ID mode.")

            target_ids = None
            if target and target.lower() != "all":
                target_ids = {int(i.strip()) for i in target.split(',') if i.strip().isdigit()}
                safe_print(
                    f"  Targeting {len(target_ids)} specific match ID(s): {', '.join(map(str, list(target_ids)[:5]))}...")

            base_url = (f"https://tounesbet.com/Sport/{sport_id}/Category/{sportcategory_id}?"
                        f"DateDay=all_days&BetRangeFilter=0&Page_number=1&Tournament_per_page=500")  # Increased page size

            initial_matches = []
            try:
                response_main = session.post(base_url, headers=headers, verify=False, timeout=120)
                initial_matches, scraped_tournament_names = extract_matches(response_main.text, main_parsing_config)
                safe_print(f"  Found {len(initial_matches)} total matches in {country_name}.")
            except Exception as e:
                safe_print(f"  ERROR fetching initial match list for {country_name}: {str(e)}")
                return

            matches_to_scrape = initial_matches
            if target_ids:
                matches_to_scrape = [m for m in initial_matches if m.get("match_id") in target_ids]
                safe_print(f"  Filtered down to {len(matches_to_scrape)} matches based on target ID(s).")

            if not matches_to_scrape:
                safe_print("  No matches to process for this country after filtering.")
            else:
                # --- CORE CHANGE: Use a nested ThreadPoolExecutor to scrape matches in parallel ---
                num_match_workers = min(MAX_WORKERS, len(matches_to_scrape))
                safe_print(
                    f"  Using {num_match_workers} workers to scrape {len(matches_to_scrape)} matches for {country_name}...")

                # Prepare arguments for each worker task
                scrape_tasks = [
                    (match, i, len(matches_to_scrape), session_manager, single_match_parsing_config, country_name)
                    for i, match in enumerate(matches_to_scrape)
                ]

                with ThreadPoolExecutor(max_workers=num_match_workers) as executor:
                    # map() executes the worker on each task and returns results in order
                    results = executor.map(scrape_single_match_worker, scrape_tasks)

                # Process the results, filtering out any that failed (returned None)
                for scraped_match in results:
                    if scraped_match and 'match_id' in scraped_match:
                        all_matches_data[scraped_match['match_id']] = scraped_match

        # --- ORIGINAL LOGIC BRANCH (SCRAPE_PER_COUNTRY or TOURNAMENT CHUNKS) ---
        else:
            # (This entire 'else' block remains unchanged)
            initial_tournaments = get_tournaments(session, headers, sport_id, sportcategory_id)
            safe_print(f"Found {len(initial_tournaments)} initial tournaments for {country_name}")

            if not initial_tournaments and not scrape_per_country:
                safe_country_name = re.sub(r'[\\/*?:"<>|]', "", country_name).replace(" ", "_")
                output_file = os.path.join(base_output_dir, f"{safe_country_name}.json")
                current_time = datetime.now(TUNISIA_TZ).isoformat()
                with open(output_file, "w", encoding="utf-8") as outfile:
                    json.dump([{"last_updated": current_time}], outfile, indent=4, ensure_ascii=False)
                safe_print(f"No tournaments found for {country_name}. Empty data saved to {output_file}")
                return

            if scrape_per_country:
                safe_print(f"Scraping all tournaments for {country_name} in a single request (--scrapecountry).")
                tournaments_to_fetch_per_page = 500
                base_url = (f"https://tounesbet.com/Sport/{sport_id}/Category/{sportcategory_id}?"
                            f"DateDay=all_days&BetRangeFilter=0&Page_number=1&Tournament_per_page={tournaments_to_fetch_per_page}")
                try:
                    response_main = session.post(base_url, headers=headers, verify=False, timeout=120)
                    matches_from_main, scraped_tournament_names = extract_matches(response_main.text,
                                                                                  main_parsing_config)
                    for match in matches_from_main:
                        all_matches_data[match['match_id']] = match
                    safe_print(
                        f"    -> Found {len(matches_from_main)} matches and {len(scraped_tournament_names)} tournament names from page.")
                except Exception as e:
                    safe_print(f"    ERROR fetching main odds for country {country_name}: {str(e)}")

                time.sleep(random.uniform(0.5, 1.5))
                for market_config in main_parsing_config.get_special_markets():
                    market_desc, odd_type_id = market_config.get('description', 'special market'), market_config.get(
                        'odd_type_id')
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
                all_tournament_ids = [t['tournament_id'] for t in initial_tournaments]
                tournament_id_chunks = list(chunk_list(all_tournament_ids, tournaments_per_request))
                safe_print(
                    f"Fetching matches for {len(all_tournament_ids)} tournaments in {len(tournament_id_chunks)} chunks.")
                for i, chunk in enumerate(tournament_id_chunks):
                    session, headers = session_manager.get_session()
                    ids_string, chunk_size = ",".join(map(str, chunk)), len(chunk)
                    base_url = (
                        f"https://tounesbet.com/Sport/{sport_id}/Category/{sportcategory_id}/TournamentIds/{ids_string}?"
                        f"DateDay=all_days&BetRangeFilter=0&Page_number=1&Tournament_per_page={chunk_size}")
                    safe_print(f"  Chunk {i + 1}/{len(tournament_id_chunks)}: Fetching main odds...")
                    try:
                        response_main = session.post(base_url, headers=headers, verify=False, timeout=60)
                        matches_from_main, _ = extract_matches(response_main.text, main_parsing_config)
                        for match in matches_from_main:
                            all_matches_data[match['match_id']] = match
                        safe_print(f"    -> Found {len(matches_from_main)} matches with main odds.")
                    except Exception as e:
                        safe_print(f"    ERROR fetching main odds for chunk {i + 1}: {str(e)}")
                    time.sleep(random.uniform(0.5, 1.5))
                    for market_config in main_parsing_config.get_special_markets():
                        market_desc, odd_type_id = market_config.get('description',
                                                                     'special market'), market_config.get('odd_type_id')
                        safe_print(
                            f"  Chunk {i + 1}/{len(tournament_id_chunks)}: Fetching {market_desc} (oddType={odd_type_id})...")
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
                                safe_print(
                                    f"    -> Found and merged {market_desc} for {len(special_lines_data)} matches.")
                        except Exception as e:
                            safe_print(f"    ERROR fetching {market_desc} for chunk {i + 1}: {str(e)}")
                        time.sleep(random.uniform(0.5, 1.5))

        # --- COMMON STRUCTURING & SAVING LOGIC (FOR ALL MODES) ---
        # (This block remains unchanged)
        grouped_matches = {}
        for match_id, match_data in all_matches_data.items():
            tid = match_data.get('tournament_id')
            if tid:
                if tid not in grouped_matches:
                    grouped_matches[tid] = []
                if scrape_by_id:
                    match_data.pop('tournament_id', None)
                grouped_matches[tid].append(match_data)

        session_final, headers_final = session_manager.get_session()
        initial_tournaments = get_tournaments(session_final, headers_final, sport_id, sportcategory_id)
        if initial_tournaments is None: initial_tournaments = []
        tournament_name_map = {t['tournament_id']: t['tournament_name'] for t in initial_tournaments}
        tournament_name_map.update({int(k): v for k, v in scraped_tournament_names.items()})

        country_data = []
        for t_id, matches in grouped_matches.items():
            tournament_name = tournament_name_map.get(t_id, f"Unnamed Tournament (ID: {t_id})")
            country_data.append({
                "tournament_name": tournament_name,
                "tournament_id": t_id,
                "matches": matches
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
    global BONUS, SCRAPE_PER_COUNTRY, SCRAPE_BY_ID, TARGET
    # 1. DEFINE AND PARSE COMMAND-LINE ARGUMENTS
    parser = argparse.ArgumentParser(
        description="Scrape match data from tounesbet.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--sport', type=str, default=SPORT,
        help='The sport to scrape. A config must exist at settings/{sport}/...'
    )
    parser.add_argument(
        '--bonus', action=argparse.BooleanOptionalAction, default=BONUS,
        help='Enable the odds bonus for odds > Minimum bonus odd.'
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
    parser.add_argument(
        '--scrape-by-id', action='store_true', default=SCRAPE_BY_ID,
        help='(Requires --scrapecountry) Scrape odds on a per-match basis using individual match pages.'
    )
    parser.add_argument(
        '--target', type=str, default=TARGET,
        help='(Requires --scrape-by-id) Target specific match IDs. Can be "all", a single ID, or a comma-separated list of IDs.'
    )
    parser.add_argument(
        '--merge', action='store_true', default=MERGE_FILES,
        help='Merge all scraped country files into a single output file.'
    )
    parser.add_argument(
        '--merged-filename', type=str, default=MERGED_FILENAME,
        help='(Requires --merge) The name of the merged output file.'
    )
    args = parser.parse_args()

    # 2. UPDATE GLOBAL CONFIGURATION FROM ARGS
    BONUS = args.bonus
    SCRAPE_PER_COUNTRY = args.scrapecountry
    SCRAPE_BY_ID = args.scrape_by_id
    TARGET = args.target
    sport_name = args.sport.lower()

    if SCRAPE_BY_ID and not SCRAPE_PER_COUNTRY:
        safe_print("WARNING: --scrape-by-id requires --scrapecountry to be enabled. Ignoring --scrape-by-id.")
        SCRAPE_BY_ID = False

    # --- FINAL, CORRECTED CONFIGURATION LOADING LOGIC ---
    main_parsing_config = None
    single_match_parsing_config = None

    try:
        if SCRAPE_BY_ID:
            # Mode: Scrape by individual match ID.
            # Only single_match_parsing.json is required from disk.
            safe_print("Mode: Scrape-by-ID. Loading 'single_match_parsing.json'...")
            single_match_parsing_config = ParsingConfig(sport_name, scrape_by_id_enabled=True)

            # For the initial match list scan, we create a DUMMY in-memory config object.
            # We will use a simple class to mimic the real ParsingConfig object's structure.
            safe_print("Creating a minimal in-memory config for the initial match list scan. No 'parsing.json' needed.")
            class MinimalConfig:
                def __init__(self):
                    self.config = {"main_markets": [], "special_markets": []}
                def get_main_markets(self):
                    return self.config["main_markets"]
                def get_special_markets(self):
                    return self.config["special_markets"]
            main_parsing_config = MinimalConfig()

        else:
            # Mode: Standard scraping (by country or tournament chunks).
            # parsing.json is required.
            safe_print("Mode: Standard. Loading 'parsing.json'...")
            main_parsing_config = ParsingConfig(sport_name, scrape_by_id_enabled=False)

    except (FileNotFoundError, json.JSONDecodeError) as e:
        safe_print(f"FATAL ERROR: A required parsing configuration file is missing or invalid for the selected mode.")
        safe_print(f"Details: {e}")
        safe_print("Exiting due to parsing configuration error.")
        return
    # --- END OF REVISED LOADING ---

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
        print(f"Scraping by match ID: {SCRAPE_BY_ID}")
        if SCRAPE_BY_ID: print(f"Target: {TARGET}")
        print(f"Loop enabled: {args.loop}")
        if args.loop: print(f"Delay between loops: {args.delay} seconds")

        session_manager = SessionManager(num_sessions=num_sessions, retry_attempts=3)
        if len(session_manager.sessions) == 0:
            safe_print("Failed to create any valid sessions. Exiting.")
            return

        session, headers = session_manager.get_session()
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
            process_args = [
                (country, session_manager, sport_id, base_output_dir, tournaments_per_request,
                 main_parsing_config, single_match_parsing_config, SCRAPE_PER_COUNTRY, SCRAPE_BY_ID, TARGET)
                for country in countries
            ]
            list(executor.map(process_country, process_args))

        safe_print("Scraping cycle completed.")

        # --- NEW MERGING LOGIC ---
        if args.merge:
            safe_print(f"\nMerging scraped files into {args.merged_filename}...")
            all_tournaments = []
            oldest_timestamp_str = None
            oldest_timestamp_dt = None
            files_to_delete = [] # List to keep track of files to remove after merging

            # Generate the list of filenames that should have been created in this cycle
            scraped_filenames = [f"{create_safe_filename(c['country_name'])}.json" for c in countries]

            for filename in scraped_filenames:
                file_path = os.path.join(base_output_dir, filename)
                if not os.path.exists(file_path):
                    safe_print(f"  - WARNING: Expected file not found, skipping: {filename}")
                    continue

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # Mark this file for deletion after successful read
                    files_to_delete.append(file_path)

                    if not data or 'last_updated' not in data[0]:
                        safe_print(f"  - WARNING: No valid data or timestamp in {filename}, skipping.")
                        continue

                    # Process timestamp to find the oldest one
                    current_timestamp_str = data[0]['last_updated']
                    current_timestamp_dt = datetime.fromisoformat(current_timestamp_str)

                    if oldest_timestamp_dt is None or current_timestamp_dt < oldest_timestamp_dt:
                        oldest_timestamp_dt = current_timestamp_dt
                        oldest_timestamp_str = current_timestamp_str

                    # Add all tournament objects (everything after the first timestamp object)
                    all_tournaments.extend(data[1:])

                except (json.JSONDecodeError, IndexError) as e:
                    safe_print(f"  - ERROR: Could not process file {filename}: {e}. It will not be deleted.")
                    # If reading failed, remove it from the deletion list
                    if file_path in files_to_delete:
                        files_to_delete.remove(file_path)
                except Exception as e:
                    safe_print(f"  - UNEXPECTED ERROR while processing file {filename}: {e}. It will not be deleted.")
                    if file_path in files_to_delete:
                        files_to_delete.remove(file_path)

            if oldest_timestamp_str and all_tournaments:
                # Use indent=2 to match your 'merged.json' example file
                merged_data = [{"last_updated": oldest_timestamp_str}] + all_tournaments
                merged_file_path = os.path.join(base_output_dir, args.merged_filename)
                try:
                    with open(merged_file_path, 'w', encoding='utf-8') as f:
                        json.dump(merged_data, f, indent=2, ensure_ascii=False)
                    safe_print(f"Successfully merged {len(all_tournaments)} tournaments from {len(files_to_delete)} files into {merged_file_path}")

                    # --- ADDED CLEANUP LOGIC ---
                    safe_print("Cleaning up individual country files...")
                    for file_path_to_delete in files_to_delete:
                        try:
                            os.remove(file_path_to_delete)
                            safe_print(f"  - Removed: {os.path.basename(file_path_to_delete)}")
                        except OSError as e:
                            safe_print(f"  - ERROR: Failed to remove {os.path.basename(file_path_to_delete)}: {e}")
                    # --- END OF ADDED CLEANUP LOGIC ---

                except Exception as e:
                    safe_print(f"  - FATAL ERROR: Could not write merged file. Individual files will not be deleted. Error: {e}")
            else:
                safe_print("Merging failed: No valid data was found to merge. Individual files will not be deleted.")
        # --- END OF NEW MERGING LOGIC ---

        if not args.loop:
            break
        safe_print(f"\nLoop mode is active. Waiting for {args.delay} seconds before the next run.")
        time.sleep(args.delay)


if __name__ == "__main__":
    main()
import os
import json
import requests
import threading
import time
import random
from bs4 import BeautifulSoup
import cloudscraper
import urllib3
import shutil
import re
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# Disable warnings about unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global lock for print operations to prevent overlapping output
print_lock = threading.Lock()

# --------------------- Scraper Configuration -------------------------------------
BONUS = True                    # Choose to activate or no the 1.1x bonus
TOURNAMENTS_PER_REQUEST = 10    # How many tournaments to fetch in a single request
NUM_SESSIONS = 12               # Sessions with unique ddos codes number
MAX_WORKERS = 7                 # Workers number
# ---------------------------------------------------------------------------------

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def export_failed_response(response_text: str, filename: str = "ddos_protection_missing.html"):
    # Save response to a file for debugging
    with open(filename, "w", encoding="utf-8") as f:
        f.write(response_text)
    print(f"[!] DDoS_Protection cookie not found. Response saved to {filename}")

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
        """Create a new session with a unique DDoS protection code."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=2,  # Exponential backoff
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Define the initial GET request headers with randomized User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
        ]

        user_agent = random.choice(user_agents)

        headers_get = {
            "Host": "tounesbet.com",
            "Cookie": f"_culture=en-us; TimeZone=-60; _vid_t={self._generate_random_cookie()}",
            "Sec-Ch-Ua": '"Not:A-Brand";v="24", "Chromium";v="134"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Accept-Encoding": "gzip, deflate, br",
            "Priority": "u=0, i"
        }

        try:
            # First GET request to retrieve cookies and HTML content
            url_get = "https://tounesbet.com/"
            response_get = session.get(url_get, headers=headers_get, verify=False, timeout=45)

            # Extract the DDoS_Protection value using a regular expression
            match = re.search(r'DDoS_Protection=([0-9a-f]+)', response_get.text)
            if match:
                ddos_value = match.group(1)
                # Manually set the cookie in the session
                session.cookies.set("DDoS_Protection", ddos_value, domain="tounesbet.com", path="/")

                # Create headers for POST requests
                headers_post = {
                    "Host": "tounesbet.com",
                    "Cookie": f"_culture=en-us; TimeZone=-60; DDoS_Protection={ddos_value}",
                    "Content-Length": "0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "*/*",
                    "Sec-Ch-Ua": '"Not:A-Brand";v="24", "Chromium";v="134"',
                    "User-Agent": user_agent,
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Origin": "https://tounesbet.com",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Dest": "empty",
                    "Referer": "https://tounesbet.com/?d=1",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Priority": "u=1, i"
                }

                return session, headers_post
            else:
                safe_print("DDoS_Protection cookie not found in the GET response.")
                export_failed_response(response_get.text)
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


def get_country_list(session, headers_post, sport_id=1181, max_retries=3):
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


def extract_matches(html):
    """
    Extract matches information from the matches page HTML.
    Extracts match id, teams, odds, and current date header.
    Returns a list of match dicts.
    """
    soup = BeautifulSoup(html, "html.parser")
    matches = []
    tbody = soup.find("tbody", id="matchesTableBody")
    if not tbody:
        return matches

    current_date = ""
    for tr in tbody.find_all("tr", recursive=False):
        classes = tr.get("class", [])
        if "prematch-header-row" in classes:
            span = tr.find("span")
            current_date = span.get_text(strip=True) if span else current_date
        elif "trMatch" in classes:
            match_id = tr.get("data-matchid", "").strip()
            tournament_id = tr.get("data-tournamentid", "").strip()
            # Time
            time_td = tr.find("td", class_="tdMatch")
            time_div = time_td.find("div") if time_td else None
            match_time = time_div.get_text(strip=True) if time_div else ""
            # Teams
            comp1 = tr.find("div", class_="competitor1-name")
            comp2 = tr.find("div", class_="competitor2-name")
            home_team = comp1.get_text(strip=True).replace("ü", "ü") if comp1 else ""
            away_team = comp2.get_text(strip=True).replace("ü", "ü") if comp2 else ""
            # Market odds
            # 1X2
            odds1, oddsX, odds2 = "", "", ""
            m1 = tr.find("td", class_="betColumn main-market-no_1")
            if m1:
                spans = m1.find_all("span")
                if len(spans) >= 3:
                    odds1, oddsX, odds2 = [s.get_text(strip=True) for s in spans[:3]]
            # Both score
            both_score, both_noscore = "", ""
            m2 = tr.find("td", class_="betColumn main-market-no_2")
            if m2:
                spans = m2.find_all("span")
                if len(spans) >= 2:
                    both_score, both_noscore = [s.get_text(strip=True) for s in spans[:2]]
            # Under/Over 2.5
            under25, over25 = "", ""
            m3 = tr.find("td", class_="betColumn main-market-no_3")
            if m3:
                # Filter out the span that shows the handicap (e.g., "2.5")
                spans = [s for s in m3.find_all("span") if not s.get("data-spreadcount")]
                if len(spans) >= 2:
                    under25, over25 = [s.get_text(strip=True) for s in spans[:2]]
            # Combination bets 1X, 12, X2
            odd_1X, odd_12, odd_X2 = "", "", ""
            m4 = tr.find("td", class_="betColumn main-market-no_4")
            if m4:
                spans = m4.find_all("span")
                if len(spans) >= 3:
                    odd_1X, odd_12, odd_X2 = [s.get_text(strip=True) for s in spans[:3]]

            match = {
                "match_id": match_id,
                "tournament_id": tournament_id,
                "date": current_date,
                "time": match_time,
                "home_team": home_team,
                "away_team": away_team,
                "1_odd": process_odd(odds1),
                "draw_odd": process_odd(oddsX),
                "2_odd": process_odd(odds2),
                "both_score_odd": process_odd(both_score),
                "both_noscore_odd": process_odd(both_noscore),
                "under_2.5_odd": process_odd(under25),
                "over_2.5_odd": process_odd(over25),
                "1X_odd": process_odd(odd_1X),
                "12_odd": process_odd(odd_12),
                "X2_odd": process_odd(odd_X2),
            }

            matches.append(match)
    return matches


def extract_total_lines(html):
    """
    Extracts all Under/Over total lines from the dedicated response HTML.
    Returns a dictionary mapping match_id to a dict of its total odds.
    e.g., {'10627805': {'under_0.5_odd': 15.00, 'over_0.5_odd': 1.00, ...}}
    """
    soup = BeautifulSoup(html, "html.parser")
    match_odds_data = {}
    tbody = soup.find("tbody", id="matchesTableBody")
    if not tbody:
        return match_odds_data

    for tr in tbody.find_all("tr", class_="trMatch"):
        match_id = tr.get("data-matchid")
        if not match_id:
            continue

        match_odds_data[match_id] = {}
        bet_column = tr.find("td", class_="betColumn")
        if not bet_column:
            continue

        # Find all line spans and their associated odd spans
        line_spans = bet_column.find_all("span", {"data-spreadcount": True, "class": "special-bet-prematch"})

        for line_span in line_spans:
            line_value_str = line_span.get_text(strip=True)
            spread_no = line_span.get('data-spreadno')

            # Find the two odd spans that correspond to this line
            odd_spans = bet_column.find_all("span", {"data-spreadno": spread_no, "class": "match_odd"})

            if len(odd_spans) == 2:
                try:
                    # Clean the line value (e.g., '2.5' from '2.5')
                    line_value_clean = re.sub(r"[^\d.]", "", line_value_str)

                    under_odd_val = None
                    over_odd_val = None

                    for odd_span in odd_spans:
                        outcome = odd_span.get('data-outcomeid', '').lower()
                        odd_val = process_odd(odd_span.get_text(strip=True))
                        if odd_val is None: continue

                        if outcome == 'under':
                            under_odd_val = odd_val
                        elif outcome == 'over':
                            over_odd_val = odd_val

                    if under_odd_val is not None:
                        match_odds_data[match_id][f"under_{line_value_clean}_odd"] = under_odd_val
                    if over_odd_val is not None:
                        match_odds_data[match_id][f"over_{line_value_clean}_odd"] = over_odd_val

                except (ValueError, IndexError):
                    # In case of parsing errors for a line, just skip it
                    continue

    return match_odds_data


def extract_handicap_lines(html):
    """
    Extracts all Asian Handicap lines from the dedicated response HTML.
    Returns a dictionary mapping match_id to a dict of its handicap odds.
    e.g., {'10230406': {'home_handicap_0.0_odd': 1.24, 'away_handicap_0.0_odd': 3.90, ...}}
    """
    soup = BeautifulSoup(html, "html.parser")
    match_odds_data = {}
    tbody = soup.find("tbody", id="matchesTableBody")
    if not tbody:
        return match_odds_data

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
            line_text = line_span.get_text(strip=True)
            spread_no = line_span.get('data-spreadno')

            # Extract the handicap value, e.g., '-0.25' from '-0.25 (0-0)'
            handicap_match = re.match(r"([-\d.]+)", line_text)
            if not handicap_match:
                continue

            home_handicap_val_str = handicap_match.group(1)

            try:
                # The away handicap is the negative of the home handicap
                home_handicap_dec = Decimal(home_handicap_val_str)
                away_handicap_dec = -home_handicap_dec

                # Format to string, using :g to remove trailing .0
                away_handicap_val_str = f"{away_handicap_dec:g}"

                odd_spans = bet_column.find_all("span", {"data-spreadno": spread_no, "class": "match_odd"})

                if len(odd_spans) == 2:
                    home_odd_val = None
                    away_odd_val = None

                    for odd_span in odd_spans:
                        outcome_id = odd_span.get('data-outcomeid')
                        odd_val = process_odd(odd_span.get_text(strip=True))
                        if odd_val is None: continue

                        if outcome_id == '1':  # Home
                            home_odd_val = odd_val
                        elif outcome_id == '2':  # Away
                            away_odd_val = odd_val

                    if home_odd_val is not None:
                        match_odds_data[match_id][f"home_handicap_{home_handicap_val_str}_odd"] = home_odd_val
                    if away_odd_val is not None:
                        match_odds_data[match_id][f"away_handicap_{away_handicap_val_str}_odd"] = away_odd_val

            except (InvalidOperation, ValueError, IndexError):
                continue

    return match_odds_data


def chunk_list(data, size):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(data), size):
        yield data[i:i + size]


def process_country(country_info):
    """Process a single country: fetch all odds types and save combined data."""
    country, session_manager, sport_id, base_output_dir, tournaments_per_request = country_info
    country_name = country["country_name"]
    sportcategory_id = country["sportcategory_id"]

    safe_print(f"\nProcessing country: {country_name} (SportCategoryId: {sportcategory_id})")

    try:
        session, headers = session_manager.get_session()
        time.sleep(random.uniform(1, 3))

        tournaments = get_tournaments(session, headers, sport_id, sportcategory_id)
        safe_print(f"Found {len(tournaments)} tournaments for {country_name}")

        if not tournaments:
            safe_country_name = re.sub(r'[\\/*?:"<>|]', "", country_name).replace(" ", "_")
            output_file = os.path.join(base_output_dir, f"{safe_country_name}.json")
            with open(output_file, "w", encoding="utf-8") as outfile:
                json.dump([], outfile, indent=4, ensure_ascii=False)
            safe_print(f"No tournaments found for {country_name}. Empty data saved to {output_file}")
            return

        all_tournament_ids = [t['tournament_id'] for t in tournaments]
        tournament_id_chunks = list(chunk_list(all_tournament_ids, tournaments_per_request))

        # This will store all match data, keyed by match_id, for easy merging
        all_matches_data = {}

        safe_print(f"Fetching matches for {len(all_tournament_ids)} tournaments in {len(tournament_id_chunks)} chunks.")

        for i, chunk in enumerate(tournament_id_chunks):
            session, headers = session_manager.get_session()
            ids_string = ",".join(chunk)
            chunk_size = len(chunk)

            base_url = (
                f"https://tounesbet.com/Sport/{sport_id}/Category/{sportcategory_id}/TournamentIds/{ids_string}?"
                f"DateDay=all_days&BetRangeFilter=0&Page_number=1&Tournament_per_page={chunk_size}"
            )

            # --- 1. Fetch Main Odds ---
            safe_print(f"  Chunk {i + 1}/{len(tournament_id_chunks)}: Fetching main odds...")
            try:
                response_main = session.post(base_url, headers=headers, verify=False, timeout=60)
                matches_from_main = extract_matches(response_main.text)
                for match in matches_from_main:
                    all_matches_data[match['match_id']] = match
                safe_print(f"    -> Found {len(matches_from_main)} matches with main odds.")
            except Exception as e:
                safe_print(f"    ERROR fetching main odds for chunk {i + 1}: {str(e)}")
            time.sleep(random.uniform(0.5, 1.5))

            # --- 2. Fetch Total O/U Odds (oddtype=533) ---
            safe_print(f"  Chunk {i + 1}/{len(tournament_id_chunks)}: Fetching total O/U odds...")
            url_totals = f"{base_url}&onlyOddType=533"
            try:
                response_totals = session.post(url_totals, headers=headers, verify=False, timeout=60)
                total_lines_data = extract_total_lines(response_totals.text)
                if total_lines_data:
                    for match_id, odds in total_lines_data.items():
                        if match_id in all_matches_data:
                            # Remove default U/O 2.5 if it exists, as it will be replaced by more specific lines
                            all_matches_data[match_id].pop("under_2.5_odd", None)
                            all_matches_data[match_id].pop("over_2.5_odd", None)
                            all_matches_data[match_id].update(odds)
                    safe_print(f"    -> Found and merged O/U odds for {len(total_lines_data)} matches.")
            except Exception as e:
                safe_print(f"    ERROR fetching total O/U odds for chunk {i + 1}: {str(e)}")
            time.sleep(random.uniform(0.5, 1.5))

            # --- 3. Fetch Handicap Odds (oddtype=534) ---
            safe_print(f"  Chunk {i + 1}/{len(tournament_id_chunks)}: Fetching handicap odds...")
            url_handicap = f"{base_url}&onlyOddType=534"
            try:
                response_handicap = session.post(url_handicap, headers=headers, verify=False, timeout=60)
                handicap_lines_data = extract_handicap_lines(response_handicap.text)
                if handicap_lines_data:
                    for match_id, odds in handicap_lines_data.items():
                        if match_id in all_matches_data:
                            all_matches_data[match_id].update(odds)
                    safe_print(f"    -> Found and merged handicap odds for {len(handicap_lines_data)} matches.")
            except Exception as e:
                safe_print(f"    ERROR fetching handicap odds for chunk {i + 1}: {str(e)}")
            time.sleep(random.uniform(0.5, 1.5))

        # Now, restructure the flat 'all_matches_data' dictionary back into the tournament-grouped list
        grouped_matches = {}
        for match_id, match_data in all_matches_data.items():
            tid = match_data.get('tournament_id')
            if tid:
                if tid not in grouped_matches:
                    grouped_matches[tid] = []
                grouped_matches[tid].append(match_data)

        country_data = []
        for t in tournaments:
            t_id = t['tournament_id']
            country_data.append({
                "tournament_name": t['tournament_name'],
                "tournament_id": t_id,
                "matches": grouped_matches.get(t_id, [])
            })

        # Save country data
        safe_country_name = re.sub(r'[\\/*?:"<>|]', "", country_name).replace(" ", "_")
        output_file = os.path.join(base_output_dir, f"{safe_country_name}.json")
        with open(output_file, "w", encoding="utf-8") as outfile:
            json.dump(country_data, outfile, indent=4, ensure_ascii=False)
        safe_print(f"Data for country {country_name} saved to {output_file}")

    except Exception as e:
        safe_print(f"An unexpected error occurred while processing country {country_name}: {str(e)}")


def main():
    # Default values
    sport_id = 1181
    base_output_dir = "scraped_matches"

    # Customizable parameters with default values from top of the file
    num_sessions = NUM_SESSIONS
    max_workers = MAX_WORKERS
    tournaments_per_request = TOURNAMENTS_PER_REQUEST

    # Allow customization through input
    print("\n=== Scraper Configuration ===")
    print(f"Number of concurrent sessions : {num_sessions}")
    print(f"Number of parallel country threads : {max_workers}")
    print(f"Tournaments per request : {tournaments_per_request}")

    safe_print(
        f"\nRunning scraper with {num_sessions} sessions, {max_workers} parallel threads, and {tournaments_per_request} tournaments per request.")

    # Create session manager with user-defined number of sessions
    session_manager = SessionManager(num_sessions=num_sessions, retry_attempts=3)

    if len(session_manager.sessions) == 0:
        safe_print("Failed to create any valid sessions. Exiting.")
        return

    # Use the first session to get countries
    session, headers = session_manager.get_session()

    safe_print("Fetching countries...")
    countries = get_country_list(session, headers, sport_id)
    safe_print(f"Found {len(countries)} countries.")

    if not countries:
        safe_print("No countries found. Exiting.")
        return


    # Ensure the output directory exists
    os.makedirs(base_output_dir, exist_ok=True)

    # Helper function to create safe filenames, consistent with the rest of the script
    def create_safe_filename(name):
        return re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_")

    # Get a set of expected filenames from the current list of countries from the server
    expected_filenames = {f"{create_safe_filename(c['country_name'])}.json" for c in countries}
    safe_print(f"Server response contains {len(expected_filenames)} countries.")

    # Clean up outdated files that no longer exist on the server
    try:
        # Get a set of existing .json files in the directory
        existing_files = {f for f in os.listdir(base_output_dir) if f.endswith('.json')}
        safe_print(f"Found {len(existing_files)} existing .json files in '{base_output_dir}'.")

        # Determine which files are outdated (exist locally but not in the server response)
        files_to_delete = existing_files - expected_filenames

        if files_to_delete:
            safe_print(f"Found {len(files_to_delete)} outdated file(s) to remove.")
            for filename in files_to_delete:
                file_path = os.path.join(base_output_dir, filename)
                try:
                    os.remove(file_path)
                    safe_print(f"  - Removed: {filename}")
                except OSError as e:
                    safe_print(f"  - Error removing {filename}: {e}")
        else:
            safe_print("No outdated files to remove. All local files are current.")

    except OSError as e:
        safe_print(f"Could not read directory '{base_output_dir}' for cleanup: {e}")


    # Process countries with user-defined number of workers
    actual_workers = min(max_workers, len(countries))  # Ensure we don't create more workers than countries
    safe_print(f"Using {actual_workers} worker threads to process {len(countries)} countries")

    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        # Create a list of arguments for each country
        args = [(country, session_manager, sport_id, base_output_dir, tournaments_per_request) for country in
                countries]
        # Execute with customized concurrency
        list(executor.map(process_country, args))

    safe_print("Scraping completed.")


if __name__ == "__main__":
    main()
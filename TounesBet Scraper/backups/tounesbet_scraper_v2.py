import os
import json
import requests
import threading
import time
import random
from bs4 import BeautifulSoup
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

# --------------------- Choose the bonus status------------------------
BONUS = True    # either "True" or "False"
# ----------------------------------------------------------------------

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


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
            time.sleep(0.6)  # Longer delay between session creation to avoid detection

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
        if BONUS == True and odd > Decimal("1.5"):
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
    for tr in tbody.find_all("tr"):
        classes = tr.get("class", [])
        if "prematch-header-row" in classes:
            span = tr.find("span")
            current_date = span.get_text(strip=True) if span else current_date
        elif "trMatch" in classes:
            match_id = tr.get("data-matchid", "").strip()
            # Time
            time_td = tr.find("td", class_="tdMatch")
            time_div = time_td.find("div") if time_td else None
            match_time = time_div.get_text(strip=True) if time_div else ""
            # Teams
            comp1 = tr.find("div", class_="competitor1-name")
            comp2 = tr.find("div", class_="competitor2-name")
            home_team = comp1.get_text(strip=True) if comp1 else ""
            away_team = comp2.get_text(strip=True) if comp2 else ""
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
                spans = [s for s in m3.find_all("span") if s.get_text(strip=True) != "2.5"]
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


def get_matches(session, headers_post, sport_id, sportcategory_id, tournament_id, page_number=1, max_retries=3):
    """
    Step 3.
    Send a POST request for a given tournament to retrieve the matches with retry logic.
    """
    url = (
        f"https://tounesbet.com/Sport/{sport_id}/Category/{sportcategory_id}/Tournament/"
        f"{tournament_id}?DateDay=all_days&BetRangeFilter=0&Page_number={page_number}"
    )

    for attempt in range(max_retries):
        try:
            safe_print(f"Requesting matches at: {url}")
            response = session.post(url, headers=headers_post, verify=False, timeout=45)
            matches = extract_matches(response.text)

            # Validate response - if we expected matches but got none, retry
            if not matches and "No matches available" not in response.text:
                raise Exception("Invalid response - no matches found but expected some")

            return matches

        except Exception as e:
            safe_print(f"Attempt {attempt + 1}/{max_retries} to get matches failed: {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = (attempt + 1) * 5
                safe_print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                safe_print(f"Maximum retries reached. Could not fetch matches for tournament {tournament_id}.")
                return []


def process_tournament(tournament_info):
    """Process a single tournament and return its matches."""
    tournament, session_manager, sport_id, sportcategory_id = tournament_info
    tournament_name = tournament["tournament_name"]
    tournament_id = tournament["tournament_id"]

    # Get a session for this tournament request
    try:
        session, headers = session_manager.get_session()
        safe_print(f"  Fetching matches for tournament: {tournament_name} (ID: {tournament_id})")

        # Add random delay between tournament requests (1-3 seconds)
        time.sleep(random.uniform(0.2, 0.5))

        matches = get_matches(session, headers, sport_id, sportcategory_id, tournament_id)
        safe_print(f"    Found {len(matches)} matches in tournament {tournament_name}")

        return {
            "tournament_name": tournament_name,
            "tournament_id": tournament_id,
            "matches": matches
        }
    except Exception as e:
        safe_print(f"Error processing tournament {tournament_name}: {str(e)}")
        # Return tournament with empty matches list rather than failing completely
        return {
            "tournament_name": tournament_name,
            "tournament_id": tournament_id,
            "matches": []
        }


def process_country(country_info):
    """Process a single country and save its data."""
    country, session_manager, sport_id, base_output_dir = country_info
    country_name = country["country_name"]
    sportcategory_id = country["sportcategory_id"]

    safe_print(f"\nProcessing country: {country_name} (SportCategoryId: {sportcategory_id})")

    try:
        # Get session for tournaments request
        session, headers = session_manager.get_session()

        # Add random delay between country processing (2-5 seconds)
        time.sleep(random.uniform(1, 3))

        # Get tournaments for this country
        tournaments = get_tournaments(session, headers, sport_id, sportcategory_id)
        safe_print(f"Found {len(tournaments)} tournaments for {country_name}")

        if not tournaments:
            # Save empty data for this country and return
            safe_country = country_name.replace(" ", "_").replace(",", "").replace("'", "")
            output_file = os.path.join(base_output_dir, f"{safe_country}.json")
            with open(output_file, "w", encoding="utf-8") as outfile:
                json.dump([], outfile, indent=4, ensure_ascii=False)
            safe_print(f"No tournaments found for country {country_name}. Empty data saved to {output_file}")
            return

        # Process tournaments sequentially to avoid overloading the server
        country_data = []
        for tournament in tournaments:
            # Process one tournament at a time
            result = process_tournament((tournament, session_manager, sport_id, sportcategory_id))
            country_data.append(result)
            # Add random delay between tournament processing (1-3 seconds)
            time.sleep(random.uniform(0.2, 0.5))

        # Save country data
        safe_country = country_name.replace(" ", "_").replace(",", "").replace("'", "")
        output_file = os.path.join(base_output_dir, f"{safe_country}.json")
        with open(output_file, "w", encoding="utf-8") as outfile:
            json.dump(country_data, outfile, indent=4, ensure_ascii=False)
        safe_print(f"Data for country {country_name} saved to {output_file}")

    except Exception as e:
        safe_print(f"Error processing country {country_name}: {str(e)}")


def main():
    # Default values
    sport_id = 1181
    base_output_dir = "../scraped_matches"

    # Customizable parameters with default values
    num_sessions = 12
    max_workers = 7

    # Allow customization through input
    print("\n=== Scraper Configuration ===")
    try:
        # user_sessions = input(f"Number of concurrent sessions (default: {num_sessions}): ")
        user_sessions = num_sessions
        print(f"Number of concurrent sessions : {num_sessions}")
        #if user_sessions.strip():
         #   num_sessions = int(user_sessions)

        # user_workers = input(f"Number of parallel country threads (default: {max_workers}
        user_workers = max_workers
        print(f"Number of parallel country threads : {max_workers}")
        #if user_workers.strip():
         #   max_workers = int(user_workers)
    except ValueError as e:
        safe_print(f"Invalid input: {e}. Using default values.")

    safe_print(f"\nRunning scraper with {num_sessions} sessions and {max_workers} parallel threads.")

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

    # Remove old directory and create fresh
    if os.path.exists(base_output_dir):
        safe_print(f"Removing existing directory: {base_output_dir}")
        shutil.rmtree(base_output_dir)
    os.makedirs(base_output_dir, exist_ok=True)

    # Process countries with user-defined number of workers
    actual_workers = min(max_workers, len(countries))  # Ensure we don't create more workers than countries
    safe_print(f"Using {actual_workers} worker threads to process {len(countries)} countries")

    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        # Create a list of arguments for each country
        args = [(country, session_manager, sport_id, base_output_dir) for country in countries]
        # Execute with customized concurrency
        list(executor.map(process_country, args))

    safe_print("Scraping completed.")


if __name__ == "__main__":
    main()
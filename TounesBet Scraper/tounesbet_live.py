import os
import json
import threading
import time
import random
import re
import sys
from datetime import datetime, timedelta, timezone
import pytz
from bs4 import BeautifulSoup
from curl_cffi import requests
import urllib3
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import unicodedata
from pathlib import Path


# --- Default Configuration (can be overridden by command-line args) -------------------
SPORT = "football"      # Use --sport to choose sport
BONUS = False           # Use --bonus to activate bonus state
LOOP = True             # Use --loop to activate loop state
DELAY = 0.3             # Use --delay to modify the delay value in seconds
INVERSED = False        # Use --inversed to scrape in reverse order
# ---------------------------------------------------------------------------------------

# ---------------------- Scraping requests configuration ---------------------------------
WORKERS = 20         # Max workers number
SESSIONS = 20        # Sessions number

# --- DEBUG FLAG ---------------------------------------------------------------------
# SET TO True TO PRINT ALL MARKET TITLES THE PARSER FINDS.
DEBUG_PARSER = False
# ---------------------------------------------------------------------------------------
# Choose if the code should ignore inactive odds
IGNORE_INACTIVE_ODDS = True

# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

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

# 1) Thread-safe printing
print_lock = threading.Lock()


def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


# 2) SessionManager (unchanged)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get Tunisia Timezone : UTC+1
TUNISIA_TIMEZONE = timezone(timedelta(hours=1))


class SessionManager:
    """Manages a pool of requests.Session objects, each negotiating DDoS_Protection."""

    def __init__(self, num_sessions=5, retry_attempts=3):
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
                safe_print(f"  • Session {i + 1}/{num_sessions} initialized")
            time.sleep(0.2)

        safe_print(f"Successfully created {len(self.sessions)} sessions")

    def _generate_random_cookie(self):
        import base64
        rand = bytearray(random.getrandbits(8) for _ in range(32))
        return base64.b64encode(rand).decode("utf-8")

    def _create_new_session(self):
        session = requests.Session()
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        ]
        ua = random.choice(user_agents)
        headers_get = {
            "Host": "tounesbet.com", "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            url = "https://tounesbet.com/"
            resp = session.get(url, headers=headers_get, verify=False, timeout=45, impersonate="chrome120")
            match = re.search(r'DDoS_Protection=([0-9a-f]+)', resp.text)
            if not match:
                safe_print("  ✗ DDoS_Protection cookie not found in GET response.")
                return None, None
            ddos_val = match.group(1)
            session.cookies.set("DDoS_Protection", ddos_val, domain="tounesbet.com", path="/")

            headers_post = {
                "Host": "tounesbet.com",
                "Cookie": f"_culture=en-us; TimeZone=-60; DDoS_Protection={ddos_val}",
                "User-Agent": ua, "Accept": "*/*", "Origin": "https://tounesbet.com",
                "Referer": "https://tounesbet.com/?d=1", "X-Requested-With": "XMLHttpRequest",
            }
            return session, headers_post
        except Exception as e:
            safe_print(f"  ✗ Error creating session: {e}")
            return None, None

    def get_session(self):
        with self.session_lock:
            if not self.sessions:
                s, h = self._create_new_session()
                if not s: raise Exception("Failed to create a new session on the fly.")
                return s, h
            s = self.sessions[self.current_index]
            h = self.headers_post[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.sessions)
            return s, h

    def add_session(self):
        with self.session_lock:
            s, h = self._create_new_session()
            if s and h:
                self.sessions.append(s)
                self.headers_post.append(h)
                return True
            return False


# 3) Bonus & Time Logic Functions
def process_odd_live(value):
    if value is None:
        return None
    try:
        odd = float(value)
        if BONUS and odd > 1.5:
            return round(odd * 1.1, 3)
        return odd
    except (ValueError, TypeError):
        return None


def compute_start_time(live_time_str):
    """
    Calculates the estimated start datetime of a live match.
    Returns a full datetime object to ensure date and time are always correct.
    """
    tz = pytz.timezone("Africa/Tunis")
    now = datetime.now(tz)
    if live_time_str == "HT":
        delta_minutes = 52
    else:
        try:
            minutes = int(live_time_str.strip().rstrip("'"))
        except (ValueError, TypeError):
            # Return None if the time string is invalid
            return None
        # Approximate break time between halves
        delta_minutes = minutes if minutes < 45 else minutes + 17

    # This is the key calculation: it correctly finds the start datetime,
    # even if it was on the previous day.
    start_datetime_approx = now - timedelta(minutes=delta_minutes)

    # --- Rounding logic from the original code, now applied to the datetime object ---
    total_minutes_in_day = start_datetime_approx.hour * 60 + start_datetime_approx.minute
    # Round to the nearest 30-minute slot (e.g., 14:17 -> 14:30, 14:12 -> 14:00)
    rounded_total_minutes = int((total_minutes_in_day + 15) // 30) * 30

    # Create the final rounded datetime object
    rounded_start_datetime = start_datetime_approx.replace(
        hour=(rounded_total_minutes // 60) % 24,
        minute=rounded_total_minutes % 60,
        second=0,
        microsecond=0
    )
    return rounded_start_datetime


# 4) --- SCRAPING FUNCTIONS (FINAL VERSION) ---

def parse_live_match_odds(html: str) -> dict:
    """
    Final robust parser. Handles both English and French market names.
    """
    soup = BeautifulSoup(html, 'html.parser')
    odds = {}

    def norm_val(v):
        try:
            return float(v.replace(',', '.').strip())
        except (ValueError, TypeError, AttributeError):
            return None

    def norm_text(text):
        # Normalize to remove accents and standardize text for reliable matching.
        if not text: return ""
        s = unicodedata.normalize('NFKD', text.lower()).encode('ascii', 'ignore').decode('utf-8')
        return s.replace("'", "").replace("-", " ").strip()

    # Iterate through each market row on the page
    for row in soup.select('.divOddRow.live_detail_market'):
        mname_elem = row.select_one('.oddName span')
        if not mname_elem:
            continue

        market_title = mname_elem.get_text(strip=True)
        norm_title = norm_text(market_title)

        if DEBUG_PARSER:
            safe_print(f"DEBUG: Found Market Title: '{market_title}' / Normalized: '{norm_title}'")

        # --- Handle all market types based on normalized titles ---

        # 1X2 / Résultat du match
        if norm_title == 'resultat du match' or norm_title == '1x2':
            mapping = {'1': '1_odd', 'x': 'draw_odd', '2': '2_odd'}
            for mo in row.select('.match-odd[data-isactive="True"]'):
                lbl = mo.select_one('.outcome-label-multirow').text.strip().lower()
                val = mo.select_one('.quoteValue')['data-oddvaluedecimal']
                if lbl in mapping: odds[mapping[lbl]] = norm_val(val)
            continue

        # Double Chance
        if norm_title == 'double chance':
            mapping = {'1x': '1X_odd', '12': '12_odd', 'x2': 'X2_odd'}
            for mo in row.select('.match-odd[data-isactive="True"]'):
                lbl = mo.select_one('.outcome-label-multirow').text.strip().lower()
                val = mo.select_one('.quoteValue')['data-oddvaluedecimal']
                if lbl in mapping: odds[mapping[lbl]] = norm_val(val)
            continue

        # Both Teams to Score / Les deux equipes marquent
        if norm_title == 'les deux equipes marquent' or norm_title == 'both teams to score':
            for mo in row.select('.match-odd[data-isactive="True"]'):
                lbl = mo.select_one('.outcome-label-multirow').text.strip().lower()
                val = mo.select_one('.quoteValue')['data-oddvaluedecimal']
                if lbl in ('oui', 'yes'):
                    odds['both_score_odd'] = norm_val(val)
                elif lbl in ('non', 'no'):
                    odds['both_noscore_odd'] = norm_val(val)
            continue

        # Team to score in both halves
        if 'to score in both halves' in norm_title or 'va marquer dans les deux mi temps' in norm_title:
            team_prefix = "home" if "home team" in norm_title or "domicile" in norm_title else "away"
            for mo in row.select('.match-odd[data-isactive="True"]'):
                lbl = mo.select_one('.outcome-label-multirow').text.strip().lower()
                val = mo.select_one('.quoteValue')['data-oddvaluedecimal']
                if lbl in ('oui', 'yes'):
                    odds[f'{team_prefix}_score_both_halves_odd'] = norm_val(val)
                elif lbl in ('non', 'no'):
                    odds[f'{team_prefix}_noscore_both_halves_odd'] = norm_val(val)
            continue

        # Team to score in 2nd half
        if 'to score 2nd half' in norm_title or 'va marquer la 2eme mi temps' in norm_title:
            team_prefix = "home" if "home team" in norm_title or "domicile" in norm_title else "away"
            for mo in row.select('.match-odd[data-isactive="True"]'):
                lbl = mo.select_one('.outcome-label-multirow').text.strip().lower()
                val = mo.select_one('.quoteValue')['data-oddvaluedecimal']
                if lbl in ('oui', 'yes'):
                    odds[f'{team_prefix}_score_second_half_odd'] = norm_val(val)
                elif lbl in ('non', 'no'):
                    odds[f'{team_prefix}_noscore_second_half_odd'] = norm_val(val)
            continue

        # All Under/Over markets
        is_under_over_market = 'under / over' in norm_title or 'under/over' in norm_title or 'total corners' in norm_title or '1st half' in norm_title
        if is_under_over_market:
            prefix = ""
            if "1st half" in norm_title:
                prefix = "first_half_"
            elif "home team" in norm_title or "domicile" in norm_title:
                prefix = "home_"
            elif "away team" in norm_title or "exterieure" in norm_title:
                prefix = "away_"
            elif "corners" in norm_title:
                prefix = "corners_"

            for holder in row.select('.odds_type_holder[data-specialoddsvalue]'):
                line_raw = holder.get('data-specialoddsvalue', '')
                if not re.match(r'^\d+(\.\d+)?$', line_raw): continue
                line_str = str(float(line_raw))

                for div in holder.select('.has-specialBet-col'):
                    mo_tag = div.select_one('.match-odd[data-isactive="True"]')
                    if not mo_tag: continue

                    lbl_tag = mo_tag.select_one('label')
                    val_tag = mo_tag.select_one('.quoteValue')
                    if not (lbl_tag and val_tag and val_tag.has_attr('data-oddvaluedecimal')): continue

                    lbl = lbl_tag.text.strip().lower()
                    val = val_tag['data-oddvaluedecimal']
                    outcome = 'under' if lbl in ('moins', 'under') else 'over' if lbl in ('plus', 'over') else None
                    if outcome:
                        key = f"{prefix}{outcome}_{line_str}_odd"
                        odds[key] = norm_val(val)
            continue

    return odds


def fetch_match_details(session, headers, sport_id, match_id):
    """Makes a POST request to get the detailed odds HTML for a single match."""
    url = f"https://tounesbet.com/Live/Details?SportId={sport_id}&LiveMatchId={match_id}"
    try:
        resp = session.post(url, headers=headers, verify=False, timeout=10)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        safe_print(f"  ✗ Error fetching details for match {match_id}: {e}")
        return None


def get_live_match_list(session, headers_post, sport_id, page=1, page_size=60, pattern=""):
    """
    Simplified to only fetch the list of matches with their basic info.
    Returns a FLAT LIST of match dictionaries, ready for parallel processing.
    """
    url = "https://tounesbet.com/Live"
    body = f"SportId={sport_id}&Page={page}&PageSize={page_size}&Patern={pattern}"
    try:
        resp = session.post(
            url,
            headers={**headers_post, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
            data=body, verify=False, timeout=45
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        flat_match_list = []
        for header_row in soup.find_all("tr", class_="live_match_list_header"):
            title_div = header_row.find("div", class_="category-tournament-title")
            if not title_div: continue
            ct = title_div.get_text(strip=True)
            country_name, tournament_name = [x.strip() for x in ct.split("/", 1)] if "/" in ct else (ct, "")

            nxt = header_row.next_sibling
            while nxt:
                if getattr(nxt, "name", None) != "tr":
                    nxt = nxt.next_sibling
                    continue
                if "live_match_list_header" in nxt.get("class", []): break
                if "trMatch" in nxt.get("class", []) and "live_match_data" in nxt.get("class", []):
                    m_row = nxt
                    mid = m_row.get("data-matchid")
                    if not mid:
                        nxt = nxt.next_sibling
                        continue

                    match_id = int(mid)
                    time_label = m_row.find("label", class_="match_status_label")
                    match_time_str = time_label.get_text(strip=True) if time_label else ""
                    home_div = m_row.find("div", class_="competitor1-name")
                    away_div = m_row.find("div", class_="competitor2-name")

                    match_info = {
                        "match_id": match_id,
                        "live_time_str": match_time_str,
                        "home_team": home_div.get_text(strip=True) if home_div else "",
                        "away_team": away_div.get_text(strip=True) if away_div else "",
                        "country": country_name,
                        "tournament": tournament_name,
                    }
                    flat_match_list.append(match_info)
                nxt = nxt.next_sibling
        return flat_match_list
    except Exception as e:
        safe_print(f"Error fetching live match list: {e}")
        return []


def process_match_and_get_all_odds(match_info, session_manager, sport_id):
    """
    Worker function: takes basic match info, fetches details, parses all odds,
    applies bonus, and returns the final, complete match object.
    """
    session, headers = session_manager.get_session()
    match_id = match_info['match_id']

    safe_print(f"Processing match_id: {match_id} ({match_info['home_team']} vs {match_info['away_team']})")

    details_html = fetch_match_details(session, headers, sport_id, match_id)
    if not details_html:
        return None

    parsed_odds = parse_live_match_odds(details_html)

    # Get the full start datetime object
    start_datetime = compute_start_time(match_info["live_time_str"])

    # Safely format the date and time from the same source
    if start_datetime:
        date_str = start_datetime.strftime("%d/%m/%Y")
        time_str = start_datetime.strftime("%H:%M")
    else:
        # Fallback if time calculation fails
        date_str = datetime.now().strftime("%d/%m/%Y")
        time_str = ""

    final_match = {
        "match_id": match_id,
        "date": date_str,  # Use the correctly derived date
        "time": time_str,  # Use the correctly derived time
        "home_team": match_info["home_team"],
        "away_team": match_info["away_team"],
        "_country": match_info["country"],
        "_tournament": match_info["tournament"],
    }

    # Apply bonus logic to all parsed odds before adding them to the final dict
    for key, val in parsed_odds.items():
        processed_val = process_odd_live(val)
        if processed_val is not None:
            final_match[key] = processed_val

    return final_match


def write_country_file(country_name, processed_matches, base_dir):
    """
    Takes the fully processed matches for a single country,
    organizes them by tournament, and writes the JSON file.
    """
    if not processed_matches:
        safe_print(f"  - No valid match data for {country_name}, skipping file write.")
        return

    # Organize the matches by tournament
    tournaments = {}
    for match in processed_matches:
        # We need to pop these keys to avoid them being in the final match object
        country = match.pop("_country")
        tournament = match.pop("_tournament")
        tournaments.setdefault(tournament, []).append(match)

    # Generate timestamp in ISO 8601 format for Tunisia timezone
    current_time = datetime.now(TUNISIA_TIMEZONE)
    timestamp_iso = current_time.isoformat()

    # Prepare the final data structure for the JSON file
    output_data = [
        {"last_updated": timestamp_iso}
    ]

    # Add tournament data
    for t_name, t_matches in tournaments.items():
        output_data.append({
            "tournament_name": t_name,
            "tournament_id": None,
            "matches": t_matches
        })

    # Write the file
    def make_safe_name(cn):
        return re.sub(r'[^\w-]', '_', cn)

    safe_country_name = make_safe_name(country_name)
    output_file = os.path.join(base_dir, f"{safe_country_name}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    safe_print(f"  ✓✓✓ EXPORTED {len(processed_matches)} matches for {country_name} → {output_file}")


# 5) Main execution logic
def main():
    global BONUS, LOOP, DELAY, DEBUG_PARSER, INVERSED

    parser = argparse.ArgumentParser(
        description="Scrape live match data from tounesbet.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--sport', type=str, default=SPORT, help='The sport to scrape.')
    parser.add_argument('--loop', action='store_true', help='Run the scraper in a continuous loop.')
    parser.add_argument('--no-loop', dest='loop', action='store_false', help='Run the scraper only once.')
    parser.set_defaults(loop=LOOP)

    parser.add_argument('--delay', type=int, default=DELAY, help='Delay in seconds between scrape cycles.')
    parser.add_argument('--bonus', action='store_true', default=BONUS, help='Enable 1.1x odds bonus for odds > 1.5.')
    parser.add_argument('--debug', action='store_true', default=DEBUG_PARSER, help='Enable parser debug logging.')
    parser.add_argument('--inversed', action='store_true', default=INVERSED,
                        help='Scrape matches in the reverse order of the website.')

    args = parser.parse_args()

    BONUS = args.bonus
    LOOP = args.loop
    DELAY = args.delay
    DEBUG_PARSER = args.debug
    INVERSED = args.inversed
    sport_name = args.sport.lower()

    # Load the sport_id dynamically from settings/<sport>/sport_id.json
    sport_id = load_sport_id(sport_name)

    base_output_dir = f"scraped_live_matches/{SPORT}"
    num_sessions = SESSIONS
    max_workers = WORKERS

    safe_print("\n=== LIVE Scraper Configuration ===")
    safe_print(f"  • Sport: {sport_name} (ID: {sport_id})")
    safe_print(f"  • Bonus enabled: {BONUS}")
    safe_print(f"  • Loop enabled: {LOOP}")
    if LOOP:
        safe_print(f"  • Delay between loops: {DELAY} seconds")
    safe_print(f"  • Inversed order: {INVERSED}")
    safe_print(f"  • Sessions: {num_sessions}")
    safe_print(f"  • Parallel detail fetchers: {max_workers}")
    safe_print(f"  • DEBUG MODE: {DEBUG_PARSER}\n")

    session_manager = SessionManager(num_sessions=num_sessions, retry_attempts=3)
    if not session_manager.sessions:
        safe_print("No valid sessions could be created. Exiting.")
        return
    # --- End of identical code ---

    counter = 0
    total_cycle_duration = 0.0
    while True:
        cycle_start_time = time.time()
        counter += 1
        safe_print(f"\n--- Cycle {counter} starting ---")

        sess, hdrs = session_manager.get_session()
        safe_print("Fetching list of live matches...")
        initial_matches = get_live_match_list(sess, hdrs, sport_id, page=1, page_size=100)

        if INVERSED:
            initial_matches.reverse()
            safe_print("--> Inversed mode ON. Processing matches in reverse order.")

        if not initial_matches:
            safe_print("No live matches found on the main list.")
            if not LOOP: break
            time.sleep(DELAY)
            continue

        safe_print(f"Found {len(initial_matches)} matches. Now fetching detailed odds for each...")

        # --- MODIFICATION START: Group matches by country before processing ---
        matches_by_country = {}
        for match in initial_matches:
            country = match.get('country')
            if country:
                matches_by_country.setdefault(country, []).append(match)
        # --- MODIFICATION END ---

        # --- MODIFICATION START: Use as_completed for real-time processing ---
        results_by_country = {country: [] for country in matches_by_country.keys()}
        country_task_counts = {country: len(matches) for country, matches in matches_by_country.items()}
        processed_counts = {country: 0 for country in matches_by_country.keys()}

        # --- File cleanup logic moved here to run once before writing new files ---
        def make_safe_name(cn):
            return re.sub(r'[^\w-]', '_', cn)

        current_safe_names = {make_safe_name(cn) for cn in matches_by_country.keys()}
        os.makedirs(base_output_dir, exist_ok=True)
        for filename in os.listdir(base_output_dir):
            if filename.endswith(".json") and filename[:-5] not in current_safe_names:
                path_to_delete = os.path.join(base_output_dir, filename)
                try:
                    os.remove(path_to_delete)
                    safe_print(f"  • Removed outdated file: {filename}")
                except Exception as e:
                    safe_print(f"  ✗ Failed to delete {filename}: {e}")
        # --- End of file cleanup logic ---

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a dictionary to map a future object back to its country
            future_to_country = {}
            for country, country_matches in matches_by_country.items():
                for match_info in country_matches:
                    future = executor.submit(process_match_and_get_all_odds, match_info, session_manager, sport_id)
                    future_to_country[future] = country

            # Process futures as they complete
            for future in as_completed(future_to_country.keys()):
                country_name = future_to_country[future]
                try:
                    # Get the result from the completed future
                    processed_match_data = future.result()
                    if processed_match_data:
                        results_by_country[country_name].append(processed_match_data)
                except Exception as exc:
                    safe_print(f"A task for {country_name} generated an exception: {exc}")

                # Increment the processed count for the country
                processed_counts[country_name] += 1

                # Check if all matches for this country have been processed
                if processed_counts[country_name] == country_task_counts[country_name]:
                    safe_print(
                        f"  >>> All {processed_counts[country_name]} matches for '{country_name}' are now processed. Exporting...")
                    write_country_file(
                        country_name,
                        results_by_country[country_name],
                        base_output_dir
                    )
        # --- MODIFICATION END ---

        # --- Duration Calculation & Reporting (remains the same) ---
        cycle_end_time = time.time()
        cycle_duration = cycle_end_time - cycle_start_time
        total_cycle_duration += cycle_duration
        average_duration = total_cycle_duration / counter

        safe_print(f"\nAll processing for Cycle {counter} is complete.")

        if not LOOP:
            safe_print(f"\nCycle completed in {cycle_duration:.2f} seconds.")
            break

        safe_print(f"\nCycle {counter} completed in {cycle_duration:.2f} seconds.")
        safe_print(f"Average duration over {counter} cycles: {average_duration:.2f} seconds.")
        safe_print(f"Now waiting {DELAY} seconds...")
        time.sleep(DELAY)

    safe_print("\nLIVE scraping completed.")


if __name__ == "__main__":
    main()
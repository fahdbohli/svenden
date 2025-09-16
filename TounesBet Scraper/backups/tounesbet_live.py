import os
import json
import threading
import time
import random
import re
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import urllib3
import shutil
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOOP = True     # Activate loop mode
DELAY = 2       # Set the delay between each cycle

# Instead of deleting the whole folder after each cycle only delete the inexisting files ### !!!!!

# -------------------------------------------------------------------------------
# 1) Thread-safe printing
# -------------------------------------------------------------------------------
print_lock = threading.Lock()
def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# -------------------------------------------------------------------------------
# 2) SessionManager (same as before)
# -------------------------------------------------------------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SessionManager:
    """Manages a pool of requests.Session objects, each negotiating DDoS_Protection."""
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
                safe_print(f"  • Session {i+1}/{num_sessions} initialized")
            time.sleep(0.6)  # small delay to avoid detection

        safe_print(f"Successfully created {len(self.sessions)} sessions")

    def _generate_random_cookie(self):
        import base64
        rand = bytearray(random.getrandbits(8) for _ in range(32))
        return base64.b64encode(rand).decode("utf-8")

    def _create_new_session(self):
        """GET → extract DDoS_Protection cookie → build POST headers."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
            "Gecko/20100101 Firefox/123.0"
        ]
        ua = random.choice(user_agents)

        headers_get = {
            "Host": "tounesbet.com",
            "Cookie": f"_culture=en-us; TimeZone=-60; _vid_t={self._generate_random_cookie()}",
            "Sec-Ch-Ua": '"Not:A-Brand";v="24", "Chromium";v="134"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                      "image/avif,image/webp,image/apng,*/*;q=0.8,"
                      "application/signed-exchange;v=b3;q=0.7",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Accept-Encoding": "gzip, deflate, br",
            "Priority": "u=0, i"
        }

        try:
            url = "https://tounesbet.com/"
            resp = session.get(url, headers=headers_get, verify=False, timeout=45)
            match = re.search(r'DDoS_Protection=([0-9a-f]+)', resp.text)
            if not match:
                safe_print("  ✗ DDoS_Protection cookie not found in GET response.")
                return None, None

            ddos_val = match.group(1)
            session.cookies.set("DDoS_Protection", ddos_val, domain="tounesbet.com", path="/")

            headers_post = {
                "Host": "tounesbet.com",
                "Cookie": f"_culture=en-us; TimeZone=-60; DDoS_Protection={ddos_val}",
                "Content-Length": "0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "X-Requested-With": "XMLHttpRequest",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "*/*",
                "Sec-Ch-Ua": '"Not:A-Brand";v="24", "Chromium";v="134"',
                "User-Agent": ua,
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

        except Exception as e:
            safe_print(f"  ✗ Error creating session: {e}")
            return None, None

    def get_session(self):
        with self.session_lock:
            if not self.sessions:
                s, h = self._create_new_session()
                if not s:
                    raise Exception("Failed to create a new session on the fly.")
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

# -------------------------------------------------------------------------------
# 3) get_live_match_list: Finds all live matches, grouped by country/tournament.
#    Extracts exactly:
#      • 1×2  (data-matchoddtype="1")
#      • Both to score (data-matchoddtype="2")
#      • Double chance (data-matchoddtype="4")
#      • All Over/Under lines (divOdds with data-specialoddsvalue="N.N")
#    No handicaps are scraped.
# -------------------------------------------------------------------------------
def get_live_match_list(session, headers_post, sport_id=1181, page=1, page_size=40, pattern=""):
    """
    Sends POST to https://tounesbet.com/Live and returns a list of dicts, each:
      {
        "country": "...",
        "tournament": "...",
        "matches": [
          {
            "match_id": 1234567,
            "time": "58'",
            "home_team": "...",
            "away_team": "...",
            "1": 2.38,    # key "1" for home win
            "X": 3.20,    # key "X" for draw
            "2": 3.50,    # key "2" for away win
            "both_Yes": 1.85,    # both to score = Yes
            "both_No": 1.95,     # both to score = No
            "1X": 1.40,          # double chance
            "12": 1.30,
            "X2": 2.10,
            "under_2.5": 1.72,   # for every available total line
            "over_2.5": 2.00,
            "under_3.5": 1.26,
            "over_3.5": 3.47,
            ...
          },
          ...
        ]
      }
    """
    url = "https://tounesbet.com/Live"
    # Build application/x-www-form-urlencoded body
    body = f"SportId={sport_id}&Page={page}&PageSize={page_size}&Patern={pattern}"
    try:
        resp = session.post(
            url,
            headers={**headers_post, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
            data=body,
            verify=False,
            timeout=45
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        live_data = []

        # Iterate over each tournament header
        for header_row in soup.find_all("tr", class_="live_match_list_header"):
            title_div = header_row.find("div", class_="category-tournament-title")
            if not title_div:
                continue
            ct = title_div.get_text(strip=True)  # e.g. "Brazil / Série A"
            if "/" in ct:
                country_name, tournament_name = [x.strip() for x in ct.split("/", 1)]
            else:
                country_name = ct
                tournament_name = ""

            # Collect matches until the next "live_match_list_header"
            matches = []
            nxt = header_row.next_sibling
            while nxt:
                if getattr(nxt, "name", None) != "tr":
                    nxt = nxt.next_sibling
                    continue

                if "live_match_list_header" in nxt.get("class", []):
                    break  # next tournament header reached

                if "trMatch" in nxt.get("class", []) and "live_match_data" in nxt.get("class", []):
                    m_row = nxt
                    mid = m_row.get("data-matchid")
                    if not mid:
                        nxt = nxt.next_sibling
                        continue
                    match_id = int(mid)

                    # Time
                    time_label = m_row.find("label", class_="match_status_label")
                    match_time = time_label.get_text(strip=True) if time_label else ""

                    # Teams
                    home_div = m_row.find("div", class_="competitor1-name")
                    away_div = m_row.find("div", class_="competitor2-name")
                    home_team = home_div.get_text(strip=True) if home_div else ""
                    away_team = away_div.get_text(strip=True) if away_div else ""

                    # Build a flat dict of odds for this match
                    o = {
                        "match_id": match_id,
                        "time": match_time,
                        "home_team": home_team,
                        "away_team": away_team
                    }

                    # 1 X 2 (main-market-no_1)
                    td_1x2 = m_row.find("td", class_="betColumn main-market-no_1")
                    if td_1x2:
                        active_spans = [sp for sp in td_1x2.find_all("span", class_="match_odd_value") if sp.get("data-isactive") == "True"]
                        if len(active_spans) >= 3:
                            try:
                                o["1"] = float(active_spans[0].get("data-oddvaluedecimal"))
                            except:
                                o["1"] = None
                            try:
                                o["X"] = float(active_spans[1].get("data-oddvaluedecimal"))
                            except:
                                o["X"] = None
                            try:
                                o["2"] = float(active_spans[2].get("data-oddvaluedecimal"))
                            except:
                                o["2"] = None

                    # Both teams to score (main-market-no_3, data-matchoddtype="2")
                    td_both = m_row.find("td", class_="betColumn main-market-no_3")
                    if td_both:
                        seen = 0
                        for sp in td_both.find_all("span", class_="match_odd_value"):
                            if sp.get("data-matchoddtype") == "2" and sp.get("data-isactive") == "True":
                                try:
                                    val = float(sp.get("data-oddvaluedecimal"))
                                except:
                                    val = None
                                if seen == 0:
                                    # first active → "Yes"
                                    o["both_Yes"] = val
                                    seen = 1
                                else:
                                    # second active → "No"
                                    o["both_No"] = val
                                    break

                    # Double Chance (main-market-no_4, data-matchoddtype="4")
                    td_dc = m_row.find("td", class_="betColumn main-market-no_4")
                    if td_dc:
                        dc_spans = [sp for sp in td_dc.find_all("span", class_="match_odd_value")
                                    if sp.get("data-matchoddtype") == "4" and sp.get("data-isactive") == "True"]
                        if len(dc_spans) >= 3:
                            try:
                                o["1X"] = float(dc_spans[0].get("data-oddvaluedecimal"))
                            except:
                                o["1X"] = None
                            try:
                                o["12"] = float(dc_spans[1].get("data-oddvaluedecimal"))
                            except:
                                o["12"] = None
                            try:
                                o["X2"] = float(dc_spans[2].get("data-oddvaluedecimal"))
                            except:
                                o["X2"] = None

                    # All Over/Under lines: find <div class="divOdds odds_type_holder match_odds_row" data-specialoddsvalue="N.N">
                    for total_div in m_row.select("div.divOdds.ods_type_holder.match_odds_row[data-specialoddsvalue]"):
                        line = total_div.get("data-specialoddsvalue")  # e.g. "2.5", "3.5", "4.5"…
                        if not line:
                            continue
                        # Within that block, the two <div class="divOdd has-specialBet-col"> each contain a <span class="quoteValue">:
                        odd_spans = total_div.select("div.divOdd.has-specialBet-col span.quoteValue")
                        if len(odd_spans) < 2:
                            continue
                        # Build keys "under_<line>" and "over_<line>"
                        under_key = f"under_{line}"
                        over_key  = f"over_{line}"
                        try:
                            o[under_key] = float(odd_spans[0].get_text(strip=True))
                        except:
                            o[under_key] = None
                        try:
                            o[over_key]  = float(odd_spans[1].get_text(strip=True))
                        except:
                            o[over_key] = None

                    matches.append(o)
                nxt = nxt.next_sibling

            live_data.append({
                "country": country_name,
                "tournament": tournament_name,
                "matches": matches
            })

        return live_data

    except Exception as e:
        safe_print(f"Error fetching live matches: {e}")
        return []

# -------------------------------------------------------------------------------
# 4) process_country_live: Writes out one JSON per country, grouping tournaments.
#    Each JSON is an array of:
#       { "tournament_name": "...", "tournament_id": null, "matches": [ ... ] }
# -------------------------------------------------------------------------------
def process_country_live(country_name, blocks, output_dir):
    """
    country_name: string (e.g. "Brazil")
    blocks: list of all tournament‐blocks for this country (each block has "tournament" & "matches")
    output_dir: top‐level folder
    """
    safe_print(f"\nProcessing country: {country_name}")

    # Group together all tournaments for this country
    tournaments = []
    for blk in blocks:
        tourn_name = blk["tournament"]
        matches = blk["matches"]
        # We need to convert each “matches” entry into the final JSON format:
        #   match_obj: { "match_id", "date", "time", "home_team", "away_team", <odds> }
        final_matches = []
        today_str = datetime.now().strftime("%d/%m/%Y")
        for m in matches:
            match_obj = {
                "match_id": m["match_id"],
                "date": today_str,
                "time": m["time"],
                "home_team": m["home_team"],
                "away_team": m["away_team"]
            }
            # Copy only the allowed odds keys from m into match_obj:
            for key, val in m.items():
                if key in ("match_id", "time", "home_team", "away_team"):
                    continue
                # Allowed keys are:
                #   "1", "X", "2", "both_Yes", "both_No", "1X", "12", "X2",
                #   "under_N.N", "over_N.N" for all N.N
                # We rewrite "1"/"X"/"2" → "1_odd", "draw_odd", "2_odd"
                if key == "1":
                    match_obj["1_odd"] = val
                elif key == "X":
                    match_obj["draw_odd"] = val
                elif key == "2":
                    match_obj["2_odd"] = val
                elif key == "both_Yes":
                    match_obj["both_score_odd"] = val
                elif key == "both_No":
                    match_obj["both_noscore_odd"] = val
                elif key == "1X":
                    match_obj["1X_odd"] = val
                elif key == "12":
                    match_obj["12_odd"] = val
                elif key == "X2":
                    match_obj["X2_odd"] = val
                elif key.startswith("under_"):
                    # key = "under_2.5" → JSON field "under_2.5_odd"
                    match_obj[f"{key}_odd"] = val
                elif key.startswith("over_"):
                    # key = "over_3.5" → JSON field "over_3.5_odd"
                    match_obj[f"{key}_odd"] = val

            final_matches.append(match_obj)

        tournaments.append({
            "tournament_name": tourn_name,
            "tournament_id": None,
            "matches": final_matches
        })

    # Write one JSON file per country
    safe_country = country_name.replace(" ", "_").replace(",", "").replace("'", "")
    output_file = os.path.join(output_dir, f"{safe_country}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tournaments, f, indent=4, ensure_ascii=False)

    safe_print(f"  ✓ Wrote {len(tournaments)} tournaments for {country_name} → {output_file}")

# -------------------------------------------------------------------------------
# 5) main(): orchestrates everything
# -------------------------------------------------------------------------------
def main():
    sport_id = 1181
    base_output_dir = "scraped_live_matches"

    # You can tune these
    num_sessions = 5
    max_workers = 4

    safe_print("\n=== LIVE Scraper Configuration ===")
    safe_print(f"  • Sessions: {num_sessions}")
    safe_print(f"  • Parallel country threads: {max_workers}\n")

    session_manager = SessionManager(num_sessions=num_sessions, retry_attempts=3)
    if not session_manager.sessions:
        safe_print("No valid sessions could be created. Exiting.")
        return
    counter = 0
    while True:
        # Step 1: Fetch the list of live matches (all tournaments, all countries)
        sess, hdrs = session_manager.get_session()
        safe_print("Fetching list of live matches...")
        live_blocks = get_live_match_list(sess, hdrs, sport_id, page=1, page_size=40, pattern="")

        if not live_blocks:
            safe_print("No live matches found (or error retrieving them). Exiting.")
            return

        # Group blocks by country
        country_map = {}
        for blk in live_blocks:
            cn = blk["country"]
            country_map.setdefault(cn, []).append(blk)

        countries = list(country_map.keys())
        safe_print(f"\nFound {len(countries)} live countries: {countries}")

        # -------------------------------------------------------------------
        # REPLACEMENT STARTS HERE (only delete JSON files for countries no longer present):
        # -------------------------------------------------------------------
        # Build set of “safe” country filenames that should remain
        def make_safe_name(country_name):
            return country_name.replace(" ", "_").replace(",", "").replace("'", "")

        current_safe_names = { make_safe_name(cn) for cn in countries }

        if os.path.exists(base_output_dir):
            # Iterate existing JSON files and delete those not in current_safe_names
            for filename in os.listdir(base_output_dir):
                if not filename.endswith(".json"):
                    continue
                raw_name = filename[:-5]  # strip “.json”
                if raw_name not in current_safe_names:
                    path_to_delete = os.path.join(base_output_dir, filename)
                    try:
                        os.remove(path_to_delete)
                        safe_print(f"  • Removed outdated file: {filename}")
                    except Exception as e:
                        safe_print(f"  ✗ Failed to delete {filename}: {e}")
        else:
            # Directory doesn’t exist at all—create it
            os.makedirs(base_output_dir, exist_ok=True)
        # -------------------------------------------------------------------
        # REPLACEMENT ENDS HERE
        # -------------------------------------------------------------------

        # Step 2: Process each country in parallel
        args_list = [(cn, country_map[cn], base_output_dir) for cn in countries]
        actual_workers = min(max_workers, len(args_list))
        safe_print(f"Using {actual_workers} worker threads to process {len(args_list)} countries.\n")

        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            for cn, blocks, outdir in args_list:
                executor.submit(process_country_live, cn, blocks, outdir)

        if not LOOP:
            break
        else:
            counter += 1
            print(f"Cycle number {counter} completed, waiting {DELAY} seconds...")
            time.sleep(DELAY)

    safe_print("\nLIVE scraping completed.")



if __name__ == "__main__":
    main()

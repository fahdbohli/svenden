import os
import json
import time
from time import sleep
import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import HTTPError

# -------------- Scraper Configuration --------------------------------------------------
MODE = "prematch"       # Choose "live" or "prematch"
LOOP = False         # Choose if the code will loop or one time scrape
REVERSED = True    # Choose if the code will scrape in the opposite order
CYCLE_DELAY = 0.5   # Choose the delay in seconds between each cycle if loop is activated
#----------------------------------------------------------------------------------------

# Directory and API endpoints based on MODE
if MODE == "live":
    CHAMPS_URL = "https://1xbet.com/LiveFeed/GetChampsZip"
    ODDS_URL = "https://1xbet.com/LiveFeed/Get1x2_VZip"
    OUTPUT_DIR = "scraped_live_matches"
    # Live-specific endpoints and headers
    LIVE_BASE_HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "X-Requested-With": "XMLHttpRequest",
        "X-Svc-Source": "__BETTING_APP__",
        "X-App-N": "__BETTING_APP__",
    }
    CHAMPS_PARAMS = {
        "sport": 1,
        "lng": "en",
        "country": 187,
        "partner": 213,
        "virtualSports": "true",
        "groupChamps": "true",
    }
    LIVE_ODDS_PARAMS = {
        "sports": "1",
        "champs": None,  # to be set per league
        "count": "50",
        "lng": "en",
        "gr": "70",
        "mode": "4",
        "country": "187",
        "getEmpty": "true",
    }
else:
    CHAMPS_URL = "https://tn.1xbet.com/service-api/LineFeed/GetChampsZip"
    ODDS_URL = "https://1xbet.com/service-api/LineFeed/Get1x2_VZip"
    OUTPUT_DIR = "scraped_matches"
    # Prematch-specific headers
    BASE_HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "X-Requested-With": "XMLHttpRequest",
        "X-Svc-Source": "__BETTING_APP__",
        "X-App-N": "__BETTING_APP__",
    }
    CHAMPS_PARAMS = {
        "sport": 1,
        "lng": "en",
        "country": 187,
        "partner": 213,
        "virtualSports": "true",
        "groupChamps": "true",
    }
    ODDS_PARAMS = {
        "sports": "1",
        "champs": None,  # to be set per league
        "count": "50",
        "lng": "en",
        "tf": "2200000",
        "tz": "1",
        "mode": "4",
        "country": "187",
        "getEmpty": "true",
        "gr": "70",
    }

REQUEST_TIMEOUT = 10

def create_session_with_retries():
    session = requests.Session()
    if MODE == "live":
        session.headers.update(LIVE_BASE_HEADERS)
    else:
        session.headers.update(BASE_HEADERS)

    retry_strategy = Retry(
        total=10,
        backoff_factor=0.01,
        status_forcelist=[429, 500, 502, 503, 504, 529],
        allowed_methods=["GET", "HEAD", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session



def get_country_leagues(session):
    r = session.get(CHAMPS_URL, params=CHAMPS_PARAMS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    leagues = {}
    country_map = {}
    with open('settings/known_countries.json', 'r', encoding='utf-8') as f:
        known_countries = json.load(f)

    multi_league = {item.get("L") for item in r.json().get("Value", []) if item.get("SC")}
    for item in r.json().get("Value", []) or []:
        raw = item.get("L", "Unknown")
        if item.get("SC"):
            country_map[raw] = raw
            leagues.setdefault(raw, [])
            for league in item["SC"]:
                lid = league.get("LI")
                name = league.get("L", raw).split('. ',1)[-1]
                leagues[raw].append({"id": lid, "name": name})
        else:
            found = next((c for c in known_countries if c in raw), None)
            found = found or raw.split('. ')[0]
            country_map[raw] = found
            leagues.setdefault(raw, []).append({
                "id": item.get("LI"),
                "name": raw.replace(found + '. ', '')
            })
    return leagues, country_map

def extract_events(match, full_mapping=True):
    """
    Extract all odds from match event list.
    If full_mapping=True, include prematch-like markets (Asian handicap, over/under at any line).
    Otherwise, restrict to basic live markets.
    """
    events = list(match.get("E", []))
    for ae in match.get("AE", []):
        events.extend(ae.get("ME", []))

    out = {}
    for ev in events:
        if ev.get("CE") is not None:
            continue
        t = ev.get("T")
        p = ev.get("P", 0)
        c = ev.get("C")
        g = ev.get("G")
        # 1X2 + double chance
        if t in range(1, 7):
            key_map = {
                1: "1_odd", 2: "draw_odd", 3: "2_odd",
                4: "1X_odd", 5: "12_odd", 6: "X2_odd"
            }
            out[key_map[t]] = c
        # both to score
        elif g == 19 and t in (180, 181):
            out["both_score_odd" if t == 180 else "both_noscore_odd"] = c
        # over/under
        elif t == 10:
            out[f"under_{p:.1f}_odd"] = c
        elif t == 9:
            out[f"over_{p:.1f}_odd"] = c
        # Asian handicap
        elif full_mapping and t in (7, 8):
            side = "home" if t == 7 else "away"
            out[f"{side}_handicap_{float(p):.1f}_odd"] = c
    return out

def get_matches_for_league_live(session, champs_id):
    headers = {
        "Accept": "*/*",
        "User-Agent": session.headers.get("User-Agent"),
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://1xbet.com/en/live/football/{champs_id}",
    }
    params = LIVE_ODDS_PARAMS.copy()
    params["champs"] = str(champs_id)

    resp = session.get(ODDS_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json().get("Value") or []

    matches = []
    for m in data:
        ts = m.get("S")
        if ts is None:
            continue
        dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
        dt_loc = dt_utc.astimezone(datetime.timezone(datetime.timedelta(hours=1)))

        base = {
            "match_id": m.get("I"),
            "date": dt_loc.strftime("%d/%m/%Y"),
            "time": dt_loc.strftime("%H:%M"),
            "home_team": m.get("O1"),
            "away_team": m.get("O2"),
        }
        # full_mapping=True to include prematch markets
        odds = extract_events(m, full_mapping=True)
        base.update(odds)
        matches.append(base)
    return matches

def get_matches_for_league_prematch(session, champs_id):
    headers = {
        "Accept": "*/*",
        "User-Agent": session.headers.get("User-Agent"),
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"https://1xbet.com/en/line/football/{champs_id}",
    }
    params = ODDS_PARAMS.copy()
    params["champs"] = str(champs_id)

    resp = session.get(ODDS_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json().get("Value") or []

    matches = []
    for m in data:
        ts = m.get("S")
        if ts is None:
            continue
        dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
        dt_loc = dt_utc.astimezone(datetime.timezone(datetime.timedelta(hours=1)))

        base = {
            "match_id": m.get("CI"),
            "date": dt_loc.strftime("%d/%m/%Y"),
            "time": dt_loc.strftime("%H:%M"),
            "home_team": m.get("O1"),
            "away_team": m.get("O2"),
        }
        odds = extract_events(m, full_mapping=True)
        base.update(odds)
        matches.append(base)
    return matches

def fetch_with_manual_retry(session, lid, name):
    for attempt in range(2):
        try:
            if MODE == "live":
                return get_matches_for_league_live(session, lid)
            else:
                return get_matches_for_league_prematch(session, lid)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 529:
                wait = 5 * (attempt + 1)
                print(f"    529 for {name}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise
    raise HTTPError(f"Persistent 529 for league {lid}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = create_session_with_retries()

    # Keep track of files that are created or updated in this cycle
    updated_files_this_cycle = set()


    leagues_by_country, country_map = get_country_leagues(session)
    if REVERSED:
        leagues_by_country = dict(reversed(list(leagues_by_country.items())))
        country_map = dict(reversed(list(country_map.items())))
    sleep_time = 0.01
    for raw, leagues in leagues_by_country.items():
        clean = country_map.get(raw, raw)
        output = []
        for league in leagues:
            lid = league.get("id")
            name = league.get("name")
            print(f"Fetching {clean} - {name} ({lid})…", end=" ")
            try:
                matches = fetch_with_manual_retry(session, lid, name)
            except Exception as e:
                print(f"skipped ({e})")
                continue
            print(f"{len(matches)} matches")
            output.append({"tournament_id": lid, "tournament_name": name, "matches": matches})
            time.sleep(sleep_time)

        if not output:
            print(f"No data for {clean}, skipping.")
            continue

        safe = clean.replace('&', 'and').replace('/', '_')
        path = os.path.join(OUTPUT_DIR, f"{safe}.json")

        # Add the path to the set of files that were updated in this cycle
        updated_files_this_cycle.add(path)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=4)
        print(f"Saved {safe}.json")

    # After the scrape cycle, delete old files that were not updated
    try:
        # Get a list of all .json files currently in the directory
        existing_files = {os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.endswith('.json')}

        # Determine which files to delete (files that exist but were not updated)
        files_to_delete = existing_files - updated_files_this_cycle

        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                print(f"Deleted old file: {os.path.basename(file_path)}")
            except OSError as e:
                print(f"Error deleting file {os.path.basename(file_path)}: {e}")
    except Exception as e:
        print(f"An error occurred during file cleanup: {e}")

if LOOP:
    c = 0
    if __name__ == "__main__":
        while True:
            main()
            c += 1
            print(f"Cycle number {c} completed")
            sleep(CYCLE_DELAY)
else:
    if __name__ == "__main__":
        main()
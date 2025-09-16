import os
import json
import time
import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import HTTPError

# Directory to save scraped JSON files
OUTPUT_DIR = "scraped_matches"

# API endpoints
CHAMPS_URL = "https://tn.1xbet.com/service-api/LineFeed/GetChampsZip"
ODDS_URL = "https://1xbet.com/service-api/LineFeed/Get1x2_VZip"

# Base headers for championships request
BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "X-Requested-With": "XMLHttpRequest",
    "X-Svc-Source": "__BETTING_APP__",
    "X-App-N": "__BETTING_APP__",
}

# Parameters for fetching championships
CHAMPS_PARAMS = {
    "sport": 1,
    "lng": "en",
    "country": 187,
    "partner": 213,
    "virtualSports": "true",
    "groupChamps": "true",
}

# Common parameters for odds request
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

# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 10


def create_session_with_retries():
    session = requests.Session()
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
    known_countries = [
        # ... list truncated for brevity ...
    ]

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


def get_matches_for_league(session, champs_id):
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

        match = {
            "match_id": m.get("CI"),
            "date": dt_loc.strftime("%d/%m/%Y"),
            "time": dt_loc.strftime("%H:%M"),
            "home_team": m.get("O1"),
            "away_team": m.get("O2"),
        }

        # Combine direct and aggregated events
        events = list(m.get("E", []))
        for ae in m.get("AE", []):
            events.extend(ae.get("ME", []))

        for ev in events:
            if ev.get("CE") is not None:
                continue  # skip cancelled
            t = ev.get("T")
            p = ev.get("P", 0)  # default to 0 for zero-handicap
            c = ev.get("C")
            g = ev.get("G")

            # Mainline 1X2 and Double Chance (T=1-6)
            if t in range(1, 7):
                key_map = {
                    1: "1_odd", 2: "draw_odd", 3: "2_odd",
                    4: "1X_odd", 5: "12_odd", 6: "2X_odd"
                }
                match[key_map[t]] = c

            # Both Teams To Score (group 19)
            elif g == 19:
                if t == 180:
                    match["both_score_odd"] = c
                elif t == 181:
                    match["both_noscore_odd"] = c

            # Over/Under any line (T=10 Under, T=9 Over)
            elif t == 10:
                match[f"under_{p:.1f}_odd"] = c
            elif t == 9:
                match[f"over_{p:.1f}_odd"] = c

            # Asian/Total Handicap (T=7 Home, T=8 Away)
            elif t in (7, 8):
                side = "home" if t == 7 else "away"
                base = float(p)
                match[f"{side}_handicap_{base:.1f}_odd"] = c

        matches.append(match)
    return matches


def fetch_with_manual_retry(session, lid, name):
    for attempt in range(2):
        try:
            return get_matches_for_league(session, lid)
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

    leagues_by_country, country_map = get_country_leagues(session)

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
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=4)
        print(f"Saved {safe}.json")

if __name__ == "__main__":
    main()

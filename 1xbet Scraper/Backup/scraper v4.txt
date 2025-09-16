import os
import json
import time
import datetime
import requests

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

# Mapping of market types
BASIC_MAP = {
    1: "1_odd",
    2: "draw_odd",
    3: "2_odd",
    4: "1X_odd",
    5: "12_odd",
    6: "2X_odd"
}

# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 10


def get_country_leagues(session):
    r = session.get(CHAMPS_URL, params=CHAMPS_PARAMS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    leagues = {}
    country_map = {}  # To store clean country names

    # List of known countries - we'll search for these in league names
    known_countries = [
        "Trinidad & Tobago", "Trinidad and Tobago", "Qatar", "Oman", "Nicaragua", "Northern Ireland",
        "Kyrgyzstan", "Kyrgyzstan ", "Iraq", "Dominican Republic", "Cuba", "Botswana",
        "Japan", "Tunisia", "China", "Israel", "Jamaica", "Jordan", "Rwanda", "Uganda ",
        # Add more countries as needed
    ]

    # First pass: collect all countries with multiple leagues
    multi_league_countries = set()
    for item in r.json().get("Value", []) or []:
        if item.get("SC"):  # Country with multiple leagues
            country = item.get("L", "Unknown")
            multi_league_countries.add(country)

    # Second pass: process all leagues
    for item in r.json().get("Value", []) or []:
        if item.get("SC"):  # Country with multiple leagues
            country = item.get("L", "Unknown")
            country_map[country] = country  # Store clean country name
            leagues.setdefault(country, [])

            for league in item["SC"]:
                lid = league.get("LI")
                raw = league.get("L", "")
                name = raw.split('. ', 1)[1] if '. ' in raw else raw
                leagues[country].append({"id": lid, "name": name})
        else:  # Country with single league
            raw_name = item.get("L", "Unknown")

            # Try multiple methods to find the country name
            found_country = None

            # Method 1: Standard format "Country. League Name"
            if ". " in raw_name:
                potential_country = raw_name.split(". ", 1)[0]
                # If this looks like a country name, use it
                if potential_country in multi_league_countries or potential_country in known_countries:
                    found_country = potential_country
                else:
                    # Even if not in our lists, assume first part before period is the country
                    found_country = potential_country

            # Method 2: If not found yet, search for known countries in the league name
            if not found_country:
                for country in known_countries:
                    if raw_name.startswith(country):
                        found_country = country
                        break

            # Method 3: Last resort, fallback to using the whole name
            if not found_country:
                found_country = raw_name

            # Store mapping
            country_map[raw_name] = found_country
            lid = item.get("LI")

            # Extract league name - remove country part if possible
            if found_country and raw_name.startswith(found_country):
                remainder = raw_name[len(found_country):].strip()
                if remainder.startswith('.'):
                    remainder = remainder[1:].strip()
                name = remainder if remainder else raw_name
            else:
                name = raw_name.split('. ', 1)[1] if '. ' in raw_name else raw_name

            leagues.setdefault(raw_name, []).append({"id": lid, "name": name})

    return leagues, country_map


def get_matches_for_league(session, champs_id):
    # Minimal headers for odds request (working configuration)
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
        # convert timestamp (seconds since epoch) to local datetime UTC+1
        dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
        dt_loc = dt_utc.astimezone(datetime.timezone(datetime.timedelta(hours=1)))

        match = {
            "match_id": m.get("CI"),
            "date": dt_loc.strftime("%d/%m/%Y"),
            "time": dt_loc.strftime("%H:%M"),
            "home_team": m.get("O1"),
            "away_team": m.get("O2"),
        }

        # collect both direct events and aggregated events
        events = list(m.get("E", []))
        for ae in m.get("AE", []):
            events.extend(ae.get("ME", []))

        for ev in events:
            if ev.get("CE") is not None:
                continue  # skip cancelled
            t = ev.get("T")
            p = ev.get("P")
            c = ev.get("C")
            # 1X2
            if t in BASIC_MAP:
                match[BASIC_MAP[t]] = c
            # over/under 2.5
            elif t == 9 and p == 2.5:
                match["over_2.5_odd"] = c
            elif t == 10 and p == 2.5:
                match["under_2.5_odd"] = c
            # both to score yes/no
            elif ev.get("G") == 19 and t == 180:
                match["both_score_odd"] = c
            elif ev.get("G") == 19 and t == 181:
                match["both_noscore_odd"] = c

        matches.append(match)
    return matches


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    leagues_by_country, country_map = get_country_leagues(session)

    # Debugging helper - uncomment if needed to see all mappings
    # print("Country mappings:")
    # for full_name, country in country_map.items():
    #     print(f"  {full_name} -> {country}")

    for raw_country, leagues in leagues_by_country.items():
        # Get the clean country name (without league info)
        clean_country = country_map[raw_country]
        print(f"Processing country: {clean_country} ({raw_country})")

        output = []
        for league in leagues:
            lid = league.get("id")
            name = league.get("name")
            print(f"  Fetching {name} ({lid})…", end="")
            try:
                matches = get_matches_for_league(session, lid)
            except Exception as e:
                print(f" skipped ({e})")
                continue
            print(f" {len(matches)} matches")
            output.append({"tournament_id": lid, "tournament_name": name, "matches": matches})
            time.sleep(sleep_time)

        if not output:
            print(f"No data for {clean_country}, skipping.")
            continue

        # Always save with clean country name
        # Remove any non-filename safe characters
        safe_country_name = clean_country.replace('&', 'and').replace('/', '_')
        path = os.path.join(OUTPUT_DIR, f"{safe_country_name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=4)
        print(f"Saved {safe_country_name}.json")


# Personalize sleep time
sleep_time = 0.3

if __name__ == "__main__":
    main()
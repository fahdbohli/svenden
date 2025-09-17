import os
import json
import time
import datetime
import re
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------
# Top‐level Configuration
# ---------------------------------------------------
# MODIFIED: Set MODE to "debug" to run the new debug function
MODE = "debug"  # "prematch", "live", or "debug"
LOOP = False  # True => after finishing one full pass, repeat
DELAY = 0.1  # MODIFIED: Reduced delay between full passes
INVERSED = True  # True => iterate country/category lists in reverse
MAX_WORKERS = 25  # ADDED: Number of concurrent threads for fetching data

# ADDED: Configuration for debug mode
CHAMP_ID_TO_DEBUG = 4623   # The championship containing the match
MATCH_ID_TO_DEBUG = 12966070 # The specific match you want to inspect

# ---------------------------------------------------
# Common Configuration
# ---------------------------------------------------
BASE_URL = "https://sb2frontend-1-altenar2.biahosted.com"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


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
    print(f"--- Requesting URL: {url} with params: {params}") # Added for clarity
    resp = session.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


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
    "eventCount": "0", "sportId": "0"
}

# <... existing functions from fetch_event_details to parse_overview_response go here ...>
# (No changes needed in those functions for this debug task)

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
    sports = menu_json.get("sports", [])
    categories = menu_json.get("categories", [])
    football = next((s for s in sports if s.get("typeId") == 1 or "Football" in s.get("name", "")), None)
    if not football: return []
    cat_ids = set(football.get("catIds", []))
    result = []
    for cat in categories:
        if cat.get("id") in cat_ids:
            result.append({"country_id": cat["id"], "country_name": cat.get("name", "").strip(),
                           "champ_ids": cat.get("champIds", [])})
    return result


MATCHES_COUNTER = 0


def _normalize_line(raw_sv: str) -> str:
    # This is a helper from the live section, but useful in prematch too.
    if not raw_sv: return ""
    s = raw_sv.strip().lstrip("+")
    if "." not in s: return f"{s}.0"
    integer_part, decimal_part = s.split(".", 1)
    if set(decimal_part) == {"0"}: return f"{integer_part}.0"
    if len(decimal_part) == 1: return f"{integer_part}.{decimal_part}"
    return f"{integer_part}.{decimal_part[0]}" if decimal_part[1] == "0" else f"{integer_part}.{decimal_part[:2]}"


def parse_overview_response(overview_json):
    global MATCHES_COUNTER
    odds_list = overview_json.get("odds", [])
    odd_map = {o["id"]: o for o in odds_list}
    markets = overview_json.get("markets", [])
    market_map = {m["id"]: m for m in markets}
    for m in markets:
        for line in m.get("lines", []): market_map[line["id"]] = line
    matches = []
    for ev in overview_json.get("events", []):
        MATCHES_COUNTER += 1
        match_id = ev.get("id")
        raw_name = ev.get("name", "")
        parts = re.split(r"\s+vs\.?\s+", raw_name, flags=re.IGNORECASE)
        home_team, away_team = (parts[0].strip(), parts[1].strip()) if len(parts) == 2 else ("", "")
        dt_utc = datetime.datetime.strptime(ev.get("startDate"), "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc)
        dt_local = (dt_utc + datetime.timedelta(hours=1)).astimezone(datetime.timezone(datetime.timedelta(hours=0)))
        base = {"match_id": match_id, "date": dt_local.strftime("%d/%m/%Y"), "time": dt_local.strftime("%H:%M"),
                "home_team": home_team, "away_team": away_team}
        for m_id in ev.get("marketIds", []):
            market = market_map.get(m_id)
            if not market: continue
            m_name = market.get("name", "").strip().lower()
            if m_name == "1x2":
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm, price = odd["name"].upper(), odd["price"]
                        if nm == "1":
                            base["1_odd"] = price
                        elif nm in ("X", "N"):
                            base["draw_odd"] = price
                        elif nm == "2":
                            base["2_odd"] = price
            elif m_name == "double chance":
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        raw_nm, price = odd["name"].strip().upper(), odd["price"]
                        if raw_nm in ("1X", "1 OR DRAW"):
                            base["1X_odd"] = price
                        elif raw_nm in ("12", "1 OR 2"):
                            base["12_odd"] = price
                        elif raw_nm in ("X2", "DRAW OR 2"):
                            base["X2_odd"] = price
            elif m_name in ("gg/ng", "gg/ng"):
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm, price = odd["name"].upper(), odd["price"]
                        if nm == "GG":
                            base["both_score_odd"] = price
                        elif nm == "NG":
                            base["both_noscore_odd"] = price
            elif m_name == "total":
                lines = market.get("lines", [])
                if lines:
                    for line in lines:
                        for oid in line.get("oddIds", []):
                            odd = odd_map.get(oid)
                            if odd and odd["oddStatus"] == 0:
                                sv = _normalize_line(odd.get("sv", "").strip())
                                if not sv: continue
                                nm, price = odd.get("name", "").lower(), odd.get("price")
                                if nm.startswith("over") or nm.startswith("plus"):
                                    base[f"over_{sv}_odd"] = price
                                elif nm.startswith("under") or nm.startswith("moins"):
                                    base[f"under_{sv}_odd"] = price
                else:
                    sv = _normalize_line(market.get("sv", "").strip())
                    for oid in market.get("oddIds", []):
                        odd = odd_map.get(oid)
                        if odd and odd["oddStatus"] == 0:
                            nm, price = odd["name"].lower(), odd["price"]
                            if nm.startswith("over") or nm.startswith("plus"):
                                base[f"over_{sv}_odd"] = price
                            elif nm.startswith("under") or nm.startswith("moins"):
                                base[f"under_{sv}_odd"] = price
            elif m_name == "handicap":
                lines = market.get("lines", [])
                if lines:
                    for line in lines:
                        sv = _normalize_line(line.get("sv", "").strip())
                        if not sv: continue
                        away_sv = sv.lstrip("-") if sv.startswith("-") else f"-{sv}"
                        for oid in line.get("oddIds", []):
                            odd = odd_map.get(oid)
                            if odd and odd["oddStatus"] == 0:
                                name, price = odd.get("name", "").strip(), odd.get("price")
                                m_side = re.match(r"^([12])", name)
                                if m_side:
                                    side_digit = m_side.group(1)
                                    if side_digit == "1":
                                        base[f"home_handicap_{sv}_odd"] = price
                                    else:
                                        base[f"away_handicap_{away_sv}_odd"] = price
                else:
                    sv = _normalize_line(market.get("sv", "").strip())
                    if not sv: continue # Changed from return to continue
                    away_sv = sv.lstrip("-") if sv.startswith("-") else f"-{sv}"
                    for oid in market.get("oddIds", []):
                        odd = odd_map.get(oid)
                        if odd and odd["oddStatus"] == 0:
                            nm, price = odd.get("name", "").strip().upper(), odd.get("price")
                            if nm == "1":
                                base[f"home_handicap_{sv}_odd"] = price
                            elif nm == "2":
                                base[f"away_handicap_{away_sv}_odd"] = price
        matches.append(base)
    return matches

# --- ADDED: New function specifically for debugging a championship ---
def debug_prematch_championship(champ_id):
    """
    Fetches the raw data for a single prematch championship and prints the JSON response.
    This helps inspect the server's data for specific matches.
    """
    print(f"--- STARTING DEBUG FOR CHAMPIONSHIP ID: {champ_id} ---")
    session = create_session_with_retries()
    try:
        # Prepare parameters for the GetOverviewWithGroups endpoint
        params = OVERVIEW_COMMON_PARAMS.copy()
        params["champIds"] = str(champ_id)

        # Fetch the data
        overview_json = fetch_json(session, OVERVIEW_ENDPOINT, params)

        # Save the entire server response to a file
        output_filename = "server_response.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(overview_json, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Raw server response successfully saved to: {output_filename}\n")

        # Optional: Try to find and print the specific match from the response
        target_match = None
        for event in overview_json.get("events", []):
            if event.get("id") == MATCH_ID_TO_DEBUG:
                target_match = event
                break

        if target_match:
            print(f"✅ Found Match ID {MATCH_ID_TO_DEBUG} in the response:")
            print(json.dumps(target_match, indent=2))
        else:
            print(f"❌ Could not find Match ID {MATCH_ID_TO_DEBUG} in the 'events' list from the server.")

    except requests.HTTPError as e:
        print(f"HTTP Error during debug: {e}")
        print(f"Response Body: {e.response.text}")
    except Exception as e:
        print(f"An unexpected error occurred during debug: {e}")


# --- CACHING VARIABLES FOR SPORT MENU ---
sport_menu_cache = None
cache_expiry_time = None

# <... existing scrape_prematch and all LIVE functions go here ...>
# (No changes needed in those functions for this debug task)

def fetch_and_parse_championship_prematch(session, champ_id, all_champs_map):
    """Worker function to fetch and parse a single championship."""
    try:
        params = OVERVIEW_COMMON_PARAMS.copy()
        params["champIds"] = str(champ_id)
        overview_json = fetch_json(session, OVERVIEW_ENDPOINT, params)

        t_name = str(champ_id)
        for grp in overview_json.get("availableChamps", []):
            if grp.get("id") == champ_id:
                t_name = grp.get("name", t_name)
                break

        matches = parse_overview_response(overview_json)
        if matches:
            return {
                "tournament_id": int(champ_id),
                "tournament_name": t_name,
                "matches": matches
            }
    except Exception as e:
        print(f"  ↳ Could not process champ_id {champ_id}: {e}")
    return None

def scrape_prematch():
    """
    MODIFIED: Main prematch loop using ThreadPoolExecutor for concurrency.
    """
    global sport_menu_cache, cache_expiry_time
    while True:
        session = create_session_with_retries()

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

        if INVERSED: countries = list(reversed(countries))

        new_safe_countries = {re.sub(r"[^\w\-]+", "_", c["country_name"]) for c in countries}
        out_dir = "scraped_matches"
        os.makedirs(out_dir, exist_ok=True)

        for filename in os.listdir(out_dir):
            if filename.lower().endswith(".json") and filename[:-5] not in new_safe_countries:
                try:
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
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_champ = {
                    executor.submit(fetch_and_parse_championship_prematch, session, champ_id, all_champs_map): champ_id
                    for champ_id in final_champ_ids}

                for future in future_to_champ:
                    result = future.result()
                    if result:
                        country_data.append(result)

            if country_data:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(country_data, f, ensure_ascii=False, indent=4)
                print(f"→ Saved PREMATCH for {c_name} → {out_path} ({len(country_data)} tournaments)")
            else:
                print(f"→ No PREMATCH matches for {c_name}, skipping.")

        if not LOOP:
            print(f"Scraping successfully completed, found {MATCHES_COUNTER} matches.")
            break

        print(f"\n--- Prematch pass complete. Waiting {DELAY} seconds before next loop... ---\n")
        time.sleep(DELAY)

# ... All LIVE functions ...
def scrape_live():
    # ... implementation of scrape_live() ...
    pass


# ---------------------------------------------------
# Entry Point
# ---------------------------------------------------
if __name__ == "__main__":
    # MODIFIED: Added a 'debug' mode to the entry point
    if MODE.upper() == "PREMATCH":
        scrape_prematch()
    elif MODE.upper() == "LIVE":
        scrape_live()
    elif MODE.upper() == "DEBUG":
        debug_prematch_championship(CHAMP_ID_TO_DEBUG)
    else:
        print("Please set MODE = 'PREMATCH', 'LIVE', or 'DEBUG' at the top.")
import os
import json
import time
import datetime
import re
import random
import requests
import argparse  # <-- ADDED for command-line arguments
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

# ----------------Default scraper configuration --------------------------------------------------
MODE = "prematch"               # Choose the mode "live" or "prematch", use --mode
SPORT = "football"          # Choose the sport "football" or ..., use --sport
LOOP = False                 # Choose if the code will loop or one time scrape
CYCLE_DELAY = 1             # Choose the delay in seconds between each cycle if loop is activated
REVERSE = True             # Choose the reverse state
MAX_WORKERS = 10            # Number of concurrent threads for fetching data
# ----------------------------------------------------------------------------------------


# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

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
                        sv = _normalize_line(line.get("sv", "").strip())
                        if not sv: continue
                        for oid in line.get("oddIds", []):
                            odd = odd_map.get(oid)
                            if odd and odd["oddStatus"] == 0:
                                nm, price = odd.get("name", "").lower(), odd.get("price")
                                if nm.startswith("over") or nm.startswith("plus"):
                                    base[f"over_{sv}_odd"] = price
                                elif nm.startswith("under") or nm.startswith("moins"):
                                    base[f"under_{sv}_odd"] = price
                top_level_odd_ids = market.get("oddIds", [])
                if top_level_odd_ids:
                    for oid in top_level_odd_ids:
                        odd = odd_map.get(oid)
                        if not odd or odd.get("oddStatus") != 0:
                            continue
                        price = odd.get("price")
                        name_full = odd.get("name", "").lower().strip()
                        parts = name_full.split()
                        if len(parts) == 2:
                            side, raw_val = parts
                            sv = _normalize_line(raw_val)
                            if not sv: continue
                            if side.startswith("over") or side.startswith("plus"):
                                base[f"over_{sv}_odd"] = price
                            elif side.startswith("under") or side.startswith("moins"):
                                base[f"under_{sv}_odd"] = price
            elif m_name == "handicap":
                lines = market.get("lines", [])
                if lines:
                    for line in lines:
                        sv = _normalize_line(line.get("sv", "").strip())
                        if not sv: continue
                        try:
                            val = float(sv)
                            away_val = -val
                            away_sv = _normalize_line(str(away_val))
                        except ValueError:
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
                top_level_odd_ids = market.get("oddIds", [])
                if top_level_odd_ids:
                    for oid in top_level_odd_ids:
                        odd = odd_map.get(oid)
                        if not odd or odd.get("oddStatus") != 0:
                            continue
                        name_full, price = odd.get("name", "").strip(), odd.get("price")
                        m_h = re.match(r"([12])\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s*\)", name_full)
                        if not m_h:
                            continue
                        side_digit, raw_val = m_h.groups()
                        clean_val = _normalize_line(raw_val)
                        if not clean_val:
                            continue
                        if side_digit == "1":
                            base[f"home_handicap_{clean_val}_odd"] = price
                        else:
                            base[f"away_handicap_{clean_val}_odd"] = price
        matches.append(base)
    return matches


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


sport_menu_cache = None
cache_expiry_time = None


def scrape_prematch(loop, inversed, delay):  # <-- MODIFIED: Accept arguments
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

        if inversed: countries = list(reversed(countries))  # <-- MODIFIED: Use argument

        new_safe_countries = {re.sub(r"[^\w\-]+", "_", c["country_name"]) for c in countries}
        out_dir = "scraped_prematch_matches"
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

        if not loop:  # <-- MODIFIED: Use argument
            print(f"Scraping successfully completed, found {MATCHES_COUNTER} matches.")
            break

        print(f"\n--- Prematch pass complete. Waiting {delay} seconds before next loop... ---\n") # <-- MODIFIED
        time.sleep(delay)  # <-- MODIFIED: Use argument


# ---------------------------------------------------
# LIVE
# ---------------------------------------------------
LIVE_OVERVIEW_ENDPOINT = "/api/widget/GetLiveOverview"
LIVE_PARAMS_TEMPLATE = {
    "culture": "en-GB", "timezoneOffset": "-60", "integration": "webetx2", "deviceType": "1",
    "numFormat": "en-GB", "countryCode": "TN", "sportId": "0"
}


def parse_live_event_basic(ev, odd_map, market_map):
    match_id = ev.get("id")
    raw_name = ev.get("name", "")
    parts = re.split(r"\s+vs\.?\s+", raw_name, flags=re.IGNORECASE)
    home_team, away_team = (parts[0].strip(), parts[1].strip()) if len(parts) == 2 else ("", "")
    dt_utc = datetime.datetime.strptime(ev.get("startDate"), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
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
                    raw, price = odd["name"].strip().upper(), odd["price"]
                    if raw in ("1X", "1 OR DRAW"):
                        base["1X_odd"] = price
                    elif raw in ("12", "1 OR 2"):
                        base["12_odd"] = price
                    elif raw in ("X2", "DRAW OR 2"):
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
                            nm, price = odd["name"].lower().split(), odd["price"]
                            if len(nm) == 2:
                                side, val = nm
                                if side == "over":
                                    base[f"over_{val}_odd"] = price
                                elif side == "under":
                                    base[f"under_{val}_odd"] = price
            else:
                line_val = market.get("sv", "").strip()
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm, price = odd["name"].lower(), odd["price"]
                        if nm.startswith("over") or nm.startswith("plus"):
                            base[f"over_{line_val}_odd"] = price
                        elif nm.startswith("under") or nm.startswith("moins"):
                            base[f"under_{line_val}_odd"] = price
        elif m_name == "handicap":
            lines = market.get("lines", [])
            if lines:
                for line in lines:
                    for oid in line.get("oddIds", []):
                        odd = odd_map.get(oid)
                        if odd and odd["oddStatus"] == 0:
                            nm, price = odd["name"].strip(), odd["price"]
                            m_h = re.match(r"([12])\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s*\)", nm)
                            if m_h:
                                side_digit, val_str = m_h.groups()
                                if side_digit == "1":
                                    base[f"home_handicap_{val_str}_odd"] = price
                                else:
                                    base[f"away_handicap_{val_str.lstrip('+')}_odd"] = price
            else:
                line_val = market.get("sv", "").strip()
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm, price = odd["name"].strip().upper(), odd["price"]
                        if nm == "1":
                            base[f"home_handicap_{line_val}_odd"] = price
                        elif nm == "2":
                            base[f"away_handicap_{line_val.lstrip('+')}_odd"] = price
    return base


def _normalize_line(raw_sv: str) -> str:
    if not raw_sv: return ""
    s = raw_sv.strip().lstrip("+")
    if "." not in s: return f"{s}.0"
    integer_part, decimal_part = s.split(".", 1)
    if set(decimal_part) == {"0"}: return f"{integer_part}.0"
    if len(decimal_part) == 1: return f"{integer_part}.{decimal_part}"
    return f"{integer_part}.{decimal_part[0]}" if decimal_part[1] == "0" else f"{integer_part}.{decimal_part[:2]}"


def parse_event_details(event_json):
    odd_map = {o["id"]: o for o in event_json.get("odds", [])}
    extra_data = {}
    for m in event_json.get("markets", []):
        m_name = m.get("name", "").strip().lower()
        group_ids = m.get("desktopOddIds", []) or m.get("mobileOddIds", [])
        if m_name == "total":
            for sub in group_ids:
                for oid in sub:
                    odd = odd_map.get(oid)
                    if not odd or odd.get("oddStatus") != 0: continue
                    sv = _normalize_line(odd.get("sv", "").strip())
                    if not sv: continue
                    nm, price = odd.get("name", "").lower(), odd.get("price")
                    if nm.startswith("over") or nm.startswith("plus"):
                        extra_data[f"over_{sv}_odd"] = price
                    elif nm.startswith("under") or nm.startswith("moins"):
                        extra_data[f"under_{sv}_odd"] = price
        elif m_name == "handicap":
            for sub in group_ids:
                for oid in sub:
                    odd = odd_map.get(oid)
                    if not odd or odd.get("oddStatus") != 0: continue
                    nm_full, price = odd.get("name", "").strip(), odd.get("price")
                    m_h = re.match(r"([12])\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s*\)", nm_full)
                    if not m_h: continue
                    side_digit, raw_val = m_h.groups()
                    clean_val = _normalize_line(raw_val)
                    if side_digit == "1":
                        extra_data[f"home_handicap_{clean_val}_odd"] = price
                    else:
                        extra_data[f"away_handicap_{clean_val}_odd"] = price
    return extra_data


def fetch_and_parse_live_event(session, ev, odd_map, market_map):
    """Worker function to process a single live event, including fetching its details."""
    try:
        base_data = parse_live_event_basic(ev, odd_map, market_map)
        event_id = ev.get("id")
        event_json = fetch_event_details(session, event_id)
        extra_data = parse_event_details(event_json)
        base_data.update(extra_data)
        return base_data
    except Exception as e:
        print(f"  ↳ Failed to fetch details for live event {ev.get('id')}: {e}")
        return None


def scrape_live(loop, inversed, delay):  # <-- MODIFIED: Accept arguments
    """
    MODIFIED: Main live loop using ThreadPoolExecutor for fetching event details concurrently.
    """
    while True:
        session = create_session_with_retries()
        session.get("https://www.clubx2.com/", headers={"User-Agent": HEADERS["User-Agent"]}, timeout=10)
        live_json = fetch_json(session, LIVE_OVERVIEW_ENDPOINT, LIVE_PARAMS_TEMPLATE.copy())

        odd_map = {o["id"]: o for o in live_json.get("odds", [])}
        market_map = {m["id"]: m for m in live_json.get("markets", [])}
        for m in live_json.get("markets", []):
            for line in m.get("lines", []): market_map[line["id"]] = line

        all_events = live_json.get("events", [])
        champ_name_map = {c["id"]: c.get("name", str(c["id"])) for c in live_json.get("champs", [])}
        categories = live_json.get("categories", [])
        if inversed: categories = list(reversed(categories))  # <-- MODIFIED: Use argument

        out_dir = "scraped_live_matches"
        os.makedirs(out_dir, exist_ok=True)
        new_safe_countries = {re.sub(r"[^\w\-]+", "_", cat.get("name", "").strip()) for cat in categories}

        for filename in os.listdir(out_dir):
            if filename.lower().endswith(".json") and filename[:-5] not in new_safe_countries:
                try:
                    os.remove(os.path.join(out_dir, filename))
                    print(f"→ Removed old LIVE file: {filename}")
                except OSError:
                    pass

        for cat in categories:
            country_name, safe_country = cat.get("name", "").strip(), re.sub(r"[^\w\-]+", "_",
                                                                             cat.get("name", "").strip())
            out_path = os.path.join(out_dir, f"{safe_country}.json")
            country_data = []

            for champ_id in cat.get("champIds", []):
                t_name = champ_name_map.get(champ_id, str(champ_id))
                matches_in_champ = [ev for ev in all_events if ev.get("champId") == champ_id]
                if not matches_in_champ: continue

                matches_list = []
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_event = {executor.submit(fetch_and_parse_live_event, session, ev, odd_map, market_map): ev
                                       for ev in matches_in_champ}

                    for future in future_to_event:
                        result = future.result()
                        if result:
                            matches_list.append(result)

                if matches_list:
                    country_data.append(
                        {"tournament_id": int(champ_id), "tournament_name": t_name, "matches": matches_list})

            if country_data:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(country_data, f, ensure_ascii=False, indent=4)
                print(f"→ Saved LIVE for {country_name} → {out_path}")
            else:
                print(f"→ No LIVE matches for {country_name}, skipping.")

        if not loop: break  # <-- MODIFIED: Use argument
        print(f"\n--- Live pass complete. Waiting {delay} seconds before next loop... ---\n") # <-- MODIFIED
        time.sleep(delay)  # <-- MODIFIED: Use argument


# ---------------------------------------------------
# Entry Point
# ---------------------------------------------------
if __name__ == "__main__":
    # --- MODIFIED: Switched to command-line argument parsing ---
    parser = argparse.ArgumentParser(
        description="Scrape match data from ClubX2.",
        formatter_class=argparse.RawTextHelpFormatter  # For better help text formatting
    )

    parser.add_argument(
        "--mode",
        choices=["prematch", "live"],
        type=str.lower,
        default=MODE,
        help="The scraping mode:\n"
             "'prematch' - Scrapes upcoming matches.\n"
             "'live'     - Scrapes currently live matches."
    )

    parser.add_argument(
        "--sport",
        type=str,
        default=SPORT,
        help=f"The sport to scrape (currently hardcoded for football, for future use). Default: {SPORT}",
    )

    parser.add_argument(
        "--loop",
        action="store_true",
        default=LOOP,
        help="Enable loop mode to repeat scraping after each full pass."
    )

    parser.add_argument(
        "--inversed",
        action="store_true",
        default=REVERSE,
        help="Iterate through countries/categories in reverse order."
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=CYCLE_DELAY,
        help=f"Delay in seconds between full passes when loop mode is enabled. Default: {CYCLE_DELAY}"
    )

    args = parser.parse_args()

    # The 'sport' argument is parsed but not passed yet, as the functions are not designed for it.
    # This matches the original code's comment.

    print(f"--- Starting Scraper ---")
    print(f"Mode: {args.mode.upper()}")
    print(f"Loop: {'Enabled' if args.loop else 'Disabled'}")
    if args.loop:
        print(f"Delay: {args.delay}s")
    print(f"Inversed: {'Enabled' if args.inversed else 'Disabled'}")
    print("------------------------\n")


    if args.mode == "prematch":
        scrape_prematch(loop=args.loop, inversed=args.inversed, delay=args.delay)
    elif args.mode == "live":
        scrape_live(loop=args.loop, inversed=args.inversed, delay=args.delay)
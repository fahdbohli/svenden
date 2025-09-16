import os
import json
import time
import datetime
import re
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------
# Top‐level Configuration
# ---------------------------------------------------
MODE     = "prematch"   # "prematch" or "live"
LOOP     = False         # True => after finishing one full pass, repeat
DELAY    = 0.4          # seconds to wait between loops (only if LOOP=True)
INVERSED = False        # True => iterate country/category lists in reverse

# ---------------------------------------------------
# Common Configuration
# ---------------------------------------------------
BASE_URL = "https://sb2frontend-1-altenar2.biahosted.com"
HEADERS  = {
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
OVERVIEW_ENDPOINT   = "/api/Widget/GetOverviewWithGroups"
EVENT_DETAILS_ENDPOINT = "/api/widget/GetEventDetails"

MENU_PARAMS = {
    "culture":        "en-GB",
    "timezoneOffset": "-60",
    "integration":    "webetx2",
    "deviceType":     "1",
    "numFormat":      "en-GB",
    "countryCode":    "TN",
    "period":         "0"
}
OVERVIEW_COMMON_PARAMS = {
    "culture":        "en-GB",
    "timezoneOffset": "-60",
    "integration":    "webetx2",
    "deviceType":     "1",
    "numFormat":      "en-GB",
    "countryCode":    "TN",
    "eventCount":     "0",
    "sportId":        "0"
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
    params = {
        "culture":        "en-GB",
        "timezoneOffset": "-60",
        "integration":    "webetx2",
        "deviceType":     "1",
        "numFormat":      "en-GB",
        "countryCode":    "TN",
        "eventId":        str(event_id)
    }

    headers = {
        "Accept":             "application/json, text/plain, */*",
        "Accept-Encoding":    "gzip, deflate, br, zstd",
        "Accept-Language":    "en-US,en;q=0.9,en-GB;q=0.8,en;q=0.7",
        "Cache-Control":      "no-cache",
        "Origin":             "https://www.clubx2.com",
        "Referer":            "https://www.clubx2.com/",
        "Sec-Ch-Ua":          "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
        "Sec-Ch-Ua-Mobile":   "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest":     "empty",
        "Sec-Fetch-Mode":     "cors",
        "Sec-Fetch-Site":     "cross-site",
        "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 "
                              "Safari/537.36"
    }

    # human-like delay between 0.1 and 1.0 seconds
    min_d, max_d = 0.01, 0.1
    delay = random.uniform(min_d, max_d)
    time.sleep(delay)

    response = cc_requests.get(
        url,
        params=params,
        headers=headers,
        impersonate="chrome120",
        timeout=30
    )
    if response.status_code != 200:
        raise requests.HTTPError(
            f"{response.status_code} Client Error: {response.text[:200]}",
            response=response
        )

    return response.json()

def parse_sport_menu(menu_json):
    sports     = menu_json.get("sports", [])
    categories = menu_json.get("categories", [])
    football = next((s for s in sports if s.get("typeId") == 1 or "Football" in s.get("name","")), None)
    if not football:
        return []

    cat_ids = set(football.get("catIds", []))
    result = []
    for cat in categories:
        if cat.get("id") in cat_ids:
            result.append({
                "country_id":   cat["id"],
                "country_name": cat.get("name","").strip(),
                "champ_ids":    cat.get("champIds", [])
            })
    return result

def parse_overview_response(overview_json):
    odds_list = overview_json.get("odds", [])
    odd_map   = {o["id"]: o for o in odds_list}

    markets    = overview_json.get("markets", [])
    market_map = {}
    for m in markets:
        market_map[m["id"]] = m
        for line in m.get("lines", []):
            market_map[line["id"]] = line

    matches = []
    for ev in overview_json.get("events", []):
        match_id = ev.get("id")
        raw_name = ev.get("name", "")
        parts    = re.split(r"\s+vs\.?\s+", raw_name, flags=re.IGNORECASE)
        if len(parts) == 2:
            home_team, away_team = parts[0].strip(), parts[1].strip()
        else:
            home_team = away_team = ""

        # Add one hour to startDate
        start_iso = ev.get("startDate")
        dt_utc    = datetime.datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ") \
                         .replace(tzinfo=datetime.timezone.utc)
        dt_plus1  = dt_utc + datetime.timedelta(hours=1)
        dt_local  = dt_plus1.astimezone(datetime.timezone(datetime.timedelta(hours=0)))
        date_str  = dt_local.strftime("%d/%m/%Y")
        time_str  = dt_local.strftime("%H:%M")

        base = {
            "match_id":  match_id,
            "date":      date_str,
            "time":      time_str,
            "home_team": home_team,
            "away_team": away_team
        }

        for m_id in ev.get("marketIds", []):
            market = market_map.get(m_id)
            if not market:
                continue
            m_name = market.get("name", "").strip().lower()

            # 1x2
            if m_name == "1x2":
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm    = odd["name"].upper()
                        price = odd["price"]
                        if nm == "1":
                            base["1_odd"] = price
                        elif nm in ("X", "N"):
                            base["draw_odd"] = price
                        elif nm == "2":
                            base["2_odd"] = price

            # Double chance
            elif m_name == "double chance":
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        raw_nm = odd["name"].strip().upper()
                        price  = odd["price"]
                        if raw_nm in ("1X", "1 OR DRAW"):
                            base["1X_odd"] = price
                        elif raw_nm in ("12", "1 OR 2"):
                            base["12_odd"] = price
                        elif raw_nm in ("X2", "DRAW OR 2"):
                            base["X2_odd"] = price

            # GG/NG
            elif m_name in ("gg/ng", "gg/ng"):
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm    = odd["name"].upper()
                        price = odd["price"]
                        if nm == "GG":
                            base["both_score_odd"] = price
                        elif nm == "NG":
                            base["both_noscore_odd"] = price

            # Total (Over/Under)
            elif m_name == "total":
                lines = market.get("lines", [])
                if lines:
                    # nested-lines (rare in PREMATCH)
                    for line in lines:
                        for oid in line.get("oddIds", []):
                            odd = odd_map.get(oid)
                            if odd and odd["oddStatus"] == 0:
                                raw_sv = odd.get("sv", "").strip()    # e.g. "2", "2.5", "3.5"
                                sv     = _normalize_line(raw_sv)
                                if not sv:
                                    continue

                                nm    = odd.get("name", "").lower()    # e.g. "Over 2.5"
                                price = odd.get("price")
                                if nm.startswith("over") or nm.startswith("plus"):
                                    base[f"over_{sv}_odd"] = price
                                elif nm.startswith("under") or nm.startswith("moins"):
                                    base[f"under_{sv}_odd"] = price
                else:
                    # single-line Total (use market's own 'sv')
                    raw_sv = market.get("sv", "").strip()  # e.g. "2.5"
                    sv = _normalize_line(raw_sv)
                    for oid in market.get("oddIds", []):
                        odd = odd_map.get(oid)
                        if odd and odd["oddStatus"] == 0:
                            nm    = odd["name"].lower()       # e.g. "Over" or "Under"
                            price = odd["price"]
                            if nm.startswith("over") or nm.startswith("plus"):
                                base[f"over_{sv}_odd"] = price
                            elif nm.startswith("under") or nm.startswith("moins"):
                                base[f"under_{sv}_odd"] = price

            # Handicap
            elif m_name == "handicap":
                lines = market.get("lines", [])
                if lines:
                    # nested‐lines (unlikely in PREMATCH)
                    for line in lines:
                        raw_sv = line.get("sv", "").strip()  # e.g. "-0.25", "+0.5"
                        sv = _normalize_line(raw_sv)  # e.g. "-0.25" or "0.5"
                        if not sv:
                            continue

                        # Prepare inverted sv for the away side:
                        if sv.startswith("-"):
                            away_sv = sv.lstrip("-")  # "-0.25" → "0.25"
                        else:
                            away_sv = f"-{sv}"  # "0.5"   → "-0.5"

                        for oid in line.get("oddIds", []):
                            odd = odd_map.get(oid)
                            if odd and odd["oddStatus"] == 0:
                                name = odd.get("name", "").strip()  # e.g. "1 (-0.25)" or "2 (+0.25)"
                                price = odd.get("price")
                                m_side = re.match(r"^([12])", name)
                                if not m_side:
                                    continue
                                side_digit = m_side.group(1)

                                if side_digit == "1":
                                    # Home gets the original sv
                                    base[f"home_handicap_{sv}_odd"] = price
                                else:
                                    # Away gets the inverted sv
                                    base[f"away_handicap_{away_sv}_odd"] = price
                else:
                    # single‐line Handicap: each odd's own name is "1" or "2"
                    raw_sv = market.get("sv", "").strip()  # e.g. "+0.5" or "-1.25"
                    sv = _normalize_line(raw_sv)  # e.g. "0.5" or "-1.25"
                    if not sv:
                        return

                    # Invert for away:
                    if sv.startswith("-"):
                        away_sv = sv.lstrip("-")
                    else:
                        away_sv = f"-{sv}"

                    for oid in market.get("oddIds", []):
                        odd = odd_map.get(oid)
                        if not odd or odd["oddStatus"] != 0:
                            continue

                        nm = odd.get("name", "").strip().upper()  # "1" or "2"
                        price = odd.get("price")
                        if nm == "1":
                            base[f"home_handicap_{sv}_odd"] = price
                        elif nm == "2":
                            base[f"away_handicap_{away_sv}_odd"] = price

        matches.append(base)

    return matches

def scrape_prematch():
    """
    Main prematch loop. If LOOP=True, repeat indefinitely (with DELAY).
    INVERSED=True reverses the order of countries.
    """
    while True:
        session = create_session_with_retries()
        menu_json  = fetch_json(session, SPORT_MENU_ENDPOINT, MENU_PARAMS)
        countries  = parse_sport_menu(menu_json)

        # Possibly reverse order:
        if INVERSED:
            countries = list(reversed(countries))

        # Build set of “safe_country” names we expect to output this run:
        new_safe_countries = {
            re.sub(r"[^\w\-]+", "_", c["country_name"])
            for c in countries
        }

        out_dir = "scraped_matches"
        os.makedirs(out_dir, exist_ok=True)

        # Delete any .json not in new_safe_countries
        for filename in os.listdir(out_dir):
            if not filename.lower().endswith(".json"):
                continue
            base = filename[:-5]
            if base not in new_safe_countries:
                try:
                    os.remove(os.path.join(out_dir, filename))
                    print(f"→ Removed old file: {filename}")
                except OSError:
                    pass

        # Now loop through each country
        for country in countries:
            c_name       = country["country_name"]
            safe_country = re.sub(r"[^\w\-]+", "_", c_name)
            out_path     = os.path.join(out_dir, f"{safe_country}.json")
            country_data = []

            for champ_id in country["champ_ids"]:
                params        = OVERVIEW_COMMON_PARAMS.copy()
                params["champIds"] = str(champ_id)
                overview_json = fetch_json(session, OVERVIEW_ENDPOINT, params)

                # Find tournament name
                t_name = str(champ_id)
                for grp in overview_json.get("availableChamps", []):
                    if grp.get("id") == champ_id:
                        t_name = grp.get("name", t_name)
                        break

                matches = parse_overview_response(overview_json)
                if matches:
                    country_data.append({
                        "tournament_id":   int(champ_id),
                        "tournament_name": t_name,
                        "matches":         matches
                    })
                time.sleep(0.02)

            if country_data:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(country_data, f, ensure_ascii=False, indent=4)
                print(f"→ Saved PREMATCH for {c_name} → {out_path}")
            else:
                print(f"→ No PREMATCH matches for {c_name}, skipping.")

        if not LOOP:
            break

        print(f"\n--- Prematch pass complete. Waiting {DELAY} seconds before next loop... ---\n")
        time.sleep(DELAY)

# ---------------------------------------------------
# LIVE
# ---------------------------------------------------
LIVE_OVERVIEW_ENDPOINT = "/api/widget/GetLiveOverview"
EVENT_DETAILS_ENDPOINT = "/api/widget/GetEventDetails"

LIVE_PARAMS_TEMPLATE = {
    "culture": "en-GB",
    "timezoneOffset": "-60",
    "integration": "webetx2",
    "deviceType": "1",
    "numFormat": "en-GB",
    "countryCode": "TN",
    "sportId": "0"
}

def parse_live_event_basic(ev, odd_map, market_map):
    """
    Extract “basic” odds (1×2, Double Chance, GG/NG, single‐line Totals/Handicaps)
    from GetLiveOverview.
    """
    match_id = ev.get("id")
    raw_name = ev.get("name","")
    parts    = re.split(r"\s+vs\.?\s+", raw_name, flags=re.IGNORECASE)
    if len(parts) == 2:
        home_team, away_team = parts[0].strip(), parts[1].strip()
    else:
        home_team = away_team = ""

    # Add one hour to startDate
    start_iso = ev.get("startDate")
    dt_utc    = datetime.datetime.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ") \
                     .replace(tzinfo=datetime.timezone.utc)
    dt_plus1  = dt_utc + datetime.timedelta(hours=1)
    dt_local  = dt_plus1.astimezone(datetime.timezone(datetime.timedelta(hours=0)))
    date_str  = dt_local.strftime("%d/%m/%Y")
    time_str  = dt_local.strftime("%H:%M")

    base = {
        "match_id":   match_id,
        "date":       date_str,
        "time":       time_str,
        "home_team":  home_team,
        "away_team":  away_team
    }

    for m_id in ev.get("marketIds", []):
        market = market_map.get(m_id)
        if not market:
            continue
        m_name = market.get("name","").strip().lower()

        # ----- 1×2 -----
        if m_name == "1x2":
            for oid in market.get("oddIds", []):
                odd = odd_map.get(oid)
                if odd and odd["oddStatus"] == 0:
                    nm    = odd["name"].upper()
                    price = odd["price"]
                    if nm == "1":
                        base["1_odd"] = price
                    elif nm in ("X","N"):
                        base["draw_odd"] = price
                    elif nm == "2":
                        base["2_odd"] = price

        # ----- Double Chance -----
        elif m_name == "double chance":
            for oid in market.get("oddIds", []):
                odd = odd_map.get(oid)
                if odd and odd["oddStatus"] == 0:
                    raw   = odd["name"].strip().upper()
                    price = odd["price"]
                    if raw in ("1X", "1 OR DRAW"):
                        base["1X_odd"] = price
                    elif raw in ("12", "1 OR 2"):
                        base["12_odd"] = price
                    elif raw in ("X2", "DRAW OR 2"):
                        base["X2_odd"] = price

        # ----- GG/NG -----
        elif m_name in ("gg/ng", "gg/ng"):
            for oid in market.get("oddIds", []):
                odd = odd_map.get(oid)
                if odd and odd["oddStatus"] == 0:
                    nm    = odd["name"].upper()
                    price = odd["price"]
                    if nm == "GG":
                        base["both_score_odd"] = price
                    elif nm == "NG":
                        base["both_noscore_odd"] = price

        # ----- Total (single‐line) -----
        elif m_name == "total":
            lines = market.get("lines", [])
            if lines:
                # Nested lines (rare)
                for line in lines:
                    for oid in line.get("oddIds", []):
                        odd = odd_map.get(oid)
                        if odd and odd["oddStatus"] == 0:
                            nm    = odd["name"].lower().split()
                            price = odd["price"]
                            if len(nm) == 2:
                                side, val = nm
                                if side == "over":
                                    base[f"over_{val}_odd"] = price
                                elif side == "under":
                                    base[f"under_{val}_odd"] = price
            else:
                # Single‐line Total uses the market “sv”
                line_val = market.get("sv","").strip()  # e.g. "0.5"
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm    = odd["name"].lower()
                        price = odd["price"]
                        if nm.startswith("over") or nm.startswith("plus"):
                            base[f"over_{line_val}_odd"] = price
                        elif nm.startswith("under") or nm.startswith("moins"):
                            base[f"under_{line_val}_odd"] = price

        # ----- Handicap (single‐line) -----
        elif m_name == "handicap":
            lines = market.get("lines", [])
            if lines:
                # Nested lines (rare)
                for line in lines:
                    for oid in line.get("oddIds", []):
                        odd = odd_map.get(oid)
                        if odd and odd["oddStatus"] == 0:
                            nm    = odd["name"].strip()
                            price = odd["price"]
                            m_h   = re.match(r"([12])\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s*\)", nm)
                            if m_h:
                                side_digit, val_str = m_h.groups()
                                if side_digit == "1":
                                    base[f"home_handicap_{val_str}_odd"] = price
                                else:
                                    val_clean = val_str.lstrip("+")
                                    base[f"away_handicap_{val_clean}_odd"] = price
            else:
                # Single‐line uses market “sv”
                line_val = market.get("sv","").strip()  # e.g. "-0.5"
                for oid in market.get("oddIds", []):
                    odd = odd_map.get(oid)
                    if odd and odd["oddStatus"] == 0:
                        nm    = odd["name"].strip().upper()
                        price = odd["price"]
                        if nm == "1":
                            base[f"home_handicap_{line_val}_odd"] = price
                        elif nm == "2":
                            clean_val = line_val.lstrip("+")
                            base[f"away_handicap_{clean_val}_odd"] = price

    return base


def _normalize_line(raw_sv: str) -> str:
    """
    Normalize any raw_sv-like string into the form:
      • strip a leading '+'
      • always include exactly one decimal place if it’s an integer (e.g. "2"→"2.0")
      • if decimal part ends in '0' alone (e.g. "50"), reduce to one digit ("5")
      • otherwise keep up to two digits after the decimal (e.g. "1.25"→"1.25", "1.234"→"1.23")
    """
    if not raw_sv:
        return ""
    s = raw_sv.strip()
    if s.startswith("+"):
        s = s[1:]
    if "." not in s:
        return f"{s}.0"
    integer_part, decimal_part = s.split(".", 1)
    if set(decimal_part) == {"0"}:
        return f"{integer_part}.0"
    if len(decimal_part) == 1:
        return f"{integer_part}.{decimal_part}"
    # Two or more digits: if second digit is "0", collapse to one digit
    if decimal_part[1] == "0":
        return f"{integer_part}.{decimal_part[0]}"
    else:
        return f"{integer_part}.{decimal_part[:2]}"


def parse_event_details(event_json):
    """
    Given a GetEventDetails response, return a dict of ALL
    Total & Handicap lines for that event, keyed like:
      over_<val>_odd, under_<val>_odd,
      home_handicap_<val>_odd, away_handicap_<val>_odd.
    """
    odds_list = event_json.get("odds", [])
    odd_map   = {o["id"]: o for o in odds_list}

    details_markets = event_json.get("markets", [])
    extra_data = {}

    for m in details_markets:
        m_name = m.get("name", "").strip().lower()

        # ----- TOTAL (Over/Under) -----
        if m_name == "total":
            group_ids = m.get("desktopOddIds", []) or m.get("mobileOddIds", [])
            for sub in group_ids:
                for oid in sub:
                    odd = odd_map.get(oid)
                    if not odd or odd.get("oddStatus") != 0:
                        continue

                    raw_sv = odd.get("sv", "").strip()  # e.g. "2.5" or "3"
                    sv = _normalize_line(raw_sv)
                    if not sv:
                        continue

                    nm    = odd.get("name", "").lower()  # "Over 2.5" / "Under 3"
                    price = odd.get("price")

                    if nm.startswith("over") or nm.startswith("plus"):
                        extra_data[f"over_{sv}_odd"] = price
                    elif nm.startswith("under") or nm.startswith("moins"):
                        extra_data[f"under_{sv}_odd"] = price

        # ----- HANDICAP -----
        elif m_name == "handicap":
            group_ids = m.get("desktopOddIds", []) or m.get("mobileOddIds", [])
            for sub in group_ids:
                for oid in sub:
                    odd = odd_map.get(oid)
                    if not odd or odd.get("oddStatus") != 0:
                        continue

                    # Pull the signed line directly from the "name", not from sv.
                    # e.g. name="1 (-0.5)" or "2 (+0.5)"
                    nm_full = odd.get("name", "").strip()
                    price   = odd.get("price")

                    # Extract “1” or “2” plus the signed number in parentheses
                    m_h = re.match(r"([12])\s*\(\s*([+-]?[0-9]*\.?[0-9]+)\s*\)", nm_full)
                    if not m_h:
                        continue

                    side_digit, raw_val = m_h.groups()  # raw_val might be "+0.5" or "-0.5"
                    clean_val = _normalize_line(raw_val)

                    if side_digit == "1":
                        extra_data[f"home_handicap_{clean_val}_odd"] = price
                    else:
                        extra_data[f"away_handicap_{clean_val}_odd"] = price

    return extra_data



def scrape_live():
    """
    Main live loop. If LOOP=True, repeat indefinitely (with DELAY).
    INVERSED=True reverses the order of categories.
    """
    while True:
        session = create_session_with_retries()

        # STEP 1: Visit homepage to collect cookies
        homepage_headers = {
            "User-Agent":      HEADERS["User-Agent"],
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"
        }
        session.get("https://www.clubx2.com/", headers=homepage_headers, timeout=10)

        # STEP 2: Fetch live overview
        live_params = LIVE_PARAMS_TEMPLATE.copy()
        live_json   = fetch_json(session, LIVE_OVERVIEW_ENDPOINT, live_params)

        odds_list  = live_json.get("odds", [])
        odd_map    = {o["id"]: o for o in odds_list}
        markets    = live_json.get("markets", [])
        market_map = {}
        for m in markets:
            market_map[m["id"]] = m
            for line in m.get("lines", []):
                market_map[line["id"]] = line

        all_events     = live_json.get("events", [])
        champ_name_map = {c["id"]: c.get("name", str(c["id"])) for c in live_json.get("champs", [])}
        categories     = live_json.get("categories", [])

        # Possibly reverse order:
        if INVERSED:
            categories = list(reversed(categories))

        out_dir = "scraped_live_matches"
        os.makedirs(out_dir, exist_ok=True)

        # Build the set of safe_country names we expect this run
        new_safe_countries = {
            re.sub(r"[^\w\-]+", "_", cat.get("name","").strip())
            for cat in categories
        }

        # Delete any .json not in new_safe_countries
        for filename in os.listdir(out_dir):
            if not filename.lower().endswith(".json"):
                continue
            base = filename[:-5]
            if base not in new_safe_countries:
                try:
                    os.remove(os.path.join(out_dir, filename))
                    print(f"→ Removed old LIVE file: {filename}")
                except OSError:
                    pass

        # Iterate each category
        for cat in categories:
            country_name = cat.get("name", "").strip()
            safe_country = re.sub(r"[^\w\-]+", "_", country_name)
            out_path     = os.path.join(out_dir, f"{safe_country}.json")
            champ_ids    = cat.get("champIds", [])
            country_data = []

            for champ_id in champ_ids:
                t_name       = champ_name_map.get(champ_id, str(champ_id))
                matches_list = []

                for ev in all_events:
                    if ev.get("champId") != champ_id:
                        continue

                    base_data = parse_live_event_basic(ev, odd_map, market_map)

                    event_id = ev.get("id")
                    try:
                        event_json = fetch_event_details(session, event_id)
                    except Exception as e:
                        print(f"  ↳ Failed to fetch details for event {event_id}: {e}")
                        continue

                    extra_data = parse_event_details(event_json)
                    base_data.update(extra_data)

                    matches_list.append(base_data)
                    time.sleep(0.02)

                if matches_list:
                    country_data.append({
                        "tournament_id":   int(champ_id),
                        "tournament_name": t_name,
                        "matches":         matches_list
                    })

            if country_data:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(country_data, f, ensure_ascii=False, indent=4)
                print(f"→ Saved LIVE for {country_name} → {out_path}")
            else:
                print(f"→ No LIVE matches for {country_name}, skipping.")

        if not LOOP:
            break

        print(f"\n--- Live pass complete. Waiting {DELAY} seconds before next loop... ---\n")
        time.sleep(DELAY)

# ---------------------------------------------------
# Entry Point
# ---------------------------------------------------
if __name__ == "__main__":
    if MODE.upper() == "PREMATCH":
        scrape_prematch()
    elif MODE.upper() == "LIVE":
        scrape_live()
    else:
        print("Please set MODE = 'PREMATCH' or 'LIVE' at the top.")

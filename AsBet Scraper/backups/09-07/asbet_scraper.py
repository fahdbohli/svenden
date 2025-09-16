import os
import json
import uuid
import time
import re
import threading
import datetime
import websocket
import argparse
from time import sleep

# ----------------Default scraper configuration --------------------------------------------------
MODE = "prematch"           # Choose the mode "live" or "prematch"
SPORT = "football"          # Choose the sport "football" or ...
LOOP = False                # Choose if the code will loop or one time scrape
CYCLE_DELAY = 0.5             # Choose the delay in seconds between each cycle if loop is activated
# ----------------------------------------------------------------------------------------

# Parse command line arguments
parser = argparse.ArgumentParser(description='ASBet scraper with configurable parameters')
parser.add_argument('--mode', choices=['live', 'prematch'], default=MODE,
                    help=f'Choose the mode: live or prematch (default: {MODE})')
parser.add_argument('--sport', default=SPORT,
                    help=f'Choose the sport (default: {SPORT})')
parser.add_argument('--loop', action='store_true', default=LOOP,
                    help=f'Enable looping mode (default: {LOOP})')
parser.add_argument('--delay', type=float, default=CYCLE_DELAY,
                    help=f'Delay in seconds between each cycle if loop is activated (default: {CYCLE_DELAY})')

args = parser.parse_args()

# ----------------Scraper configuration --------------------------------------------------
MODE = args.mode           # Choose the mode "live" or "prematch"
SPORT = args.sport         # Choose the sport "football" or ...
LOOP = args.loop           # Choose if the code will loop or one time scrape
CYCLE_DELAY = args.delay   # Choose the delay in seconds between each cycle if loop is activated
# ----------------------------------------------------------------------------------------

# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# ----- HELPER FUNCTION -----
def slugify(text: str, remove_digits=True) -> str:
    """
    Converts a string into a URL-friendly slug.
    - Removes digits if specified.
    - Converts to lowercase.
    - Replaces spaces and underscores with a hyphen.
    - Removes all other non-alphanumeric characters.
    """
    if not isinstance(text, str):
        return ""

    if remove_digits:
        # Remove all digits from the string
        text = re.sub(r'\d+', '', text)

    # Convert to lowercase and remove non-word characters (but keep spaces/hyphens)
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text).strip()

    # Replace one or more spaces/underscores with a single hyphen
    text = re.sub(r'[\s_]+', '-', text)

    return text

# Generate a unique request ID
def make_rid():
    return uuid.uuid4().hex


# Fixed RIDs for session and initial "count" request
SESSION_RID = make_rid()
GET_RID = make_rid()

# Directory to save scraped JSON files
if MODE == "live":
    OUTPUT_DIR = "scraped_live_matches"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
if MODE == "prematch":
    OUTPUT_DIR = "scraped_prematch_matches"
    os.makedirs(OUTPUT_DIR, exist_ok=True)


# Map to track competition-specific requests and results: rid -> (region_alias, region_id, comp_id, comp_name)
competition_requests = {}
# Accumulate responses: region_alias -> list of tournament dicts
responses_data = {}
# Track completion per region: region_alias -> expected_count
region_expected_counts = {}
region_processed_counts = {}

# Counters for scheduling and completion
expected_requests = 0
processed_requests = 0

# Supported market types
MARKET_TYPES = [
    "P1XP2",  # Mainline (1X2)
    "1X12X2",  # Double chance
    "BothTeamsToScore",  # Both to score
    "OverUnder",  # Over/Under
    "AsianHandicap",  # Total handicap / Asian Handicap
]

# Command: open session
request_session_cmd = {
    "command": "request_session",
    "params": {
        "language": "eng",
        "site_id": 18756444,
        "source": 42,
        "release_date": "04/09/2025-19:23",
        "afec": "2KWqQHPpEm4ZSsgFmyqxfjoJOJM3fbFZKWHg"
    },
    "rid": SESSION_RID
}


# Command: get counts of games per competition
if MODE == "live":
    request_counts_cmd = {
    "command": "get",
    "params": {
        "source": "betting",
        "what": {
            "sport": ["id", "name", "alias", "order"],
            "region": ["id", "name", "alias", "order"],
            "competition": ["id", "name"],
            "game": "@count"
        },
        "where": {
            "sport": {"alias": "Soccer"},
            "game": {"@and": [{"is_live": 1}, {"is_blocked": 0}]}
        }
    },
    "rid": GET_RID
}
if MODE == "prematch":
    request_counts_cmd = {
        "command": "get",
        "params": {
            "source": "betting",
            "what": {
                "sport": ["id", "name", "alias", "order"],
                "region": ["id", "name", "alias", "order"],
                "competition": ["id", "name"],
                "game": "@count"
            },
            "where": {
                "sport": { "alias": "Soccer" },
                "game": {
                    "@and": [
                        { "is_blocked": 0 },
                        {"is_live": 0},
                        {
                            "@or": [
                                { "visible_in_prematch": 1 },
                                { "type": { "@in": [0, 2] } }
                            ]
                        }
                    ]
                }
            }
        },
        "rid": GET_RID
    }

# ------------------------------ Requests versions ----------------------------
# "game": {"is_live": 1}
# "game": {"@or": [{"visible_in_prematch": mode}, {"type": {"@in": [0, 2]}}]}    # original
# "game": {"@or": [{"visible_in_prematch": mode}, {"is_live": 1}]}
# "game": {"@and": [{"is_live": 1}, {"is_blocked": 0}]}

# ------------------------------------------------------------------------------

# Build per-competition detailed get command; subscribe=False for one-shot
def make_match_request(rid, comp_id):
    return {
        "command": "get",
        "params": {
            "source": "betting",
            "what": {
                "sport": ["name"],
                "region": ["name"],
                "competition": ["name"],
                "game": ["id", "markets_count", "type", "start_ts",
                         "team1_id", "team1_name", "team2_id", "team2_name", "sport_alias", "region_alias",
                         "is_blocked", "game_number"],
                "market": ["id", "group_id", "group_name", "group_order", "type", "name_template", "name", "order",
                           "display_key", "col_count", "base"],
                "event": ["id", "type_1", "price", "name", "base", "order"]
            },
            "where": {
                "sport":       { "alias": "Soccer" },
                "competition": { "id": comp_id },
                "game":        { "is_blocked": 0 },
                "market":      { "type": { "@in": MARKET_TYPES } }
            },
            "subscribe": False
        },
        "rid": rid
    }


def cleanup_obsolete_files(current_countries):
    """Remove JSON files for countries that no longer exist in competitions"""
    for filename in os.listdir(OUTPUT_DIR):
        if filename.lower().endswith('.json'):
            country_name = filename[:-5]  # Remove .json extension
            if country_name not in current_countries:
                try:
                    os.remove(os.path.join(OUTPUT_DIR, filename))
                    print(f"🗑️ Removed obsolete file: {filename}")
                except Exception as e:
                    print(f"⚠️ Could not remove {filename}: {e}")


def export_country_data(alias):
    """Export data for a specific country immediately"""
    if alias in responses_data:
        fn = os.path.join(OUTPUT_DIR, f"{alias}.json")
        with open(fn, "w", encoding="utf-8") as f:
            json.dump(responses_data[alias], f, indent=4, ensure_ascii=False)
        print(f"✅ Exported {alias} to {fn}")


# WebSocket event handlers
def on_open(ws):
    print("🟢 Connection opened")
    ws.send(json.dumps(request_session_cmd))


def on_message(ws, message):
    global expected_requests, processed_requests
    data = json.loads(message)
    rid = data.get("rid")

    # Session response
    if rid == SESSION_RID:
        if data.get("code") == 0:
            print("✅ Session established, requesting counts…")
            ws.send(json.dumps(request_counts_cmd))
        else:
            print("⚠️ Session failed:", data)

    # Counts response: schedule detailed requests
    elif rid == GET_RID:
        print("📊 Counts received, scheduling match requests…")

        # ─────── Reset between cycles ───────
        competition_requests.clear()      # drop last cycle's queued RIDs
        processed_requests = 0            # reset how many requests we've handled
        expected_requests = 0             # reset the total‐to‐process count
        # ────────────────────────────────────

        sport_payload = data.get("data", {}).get("data", {}).get("sport", {})
        _, sport_node = next(iter(sport_payload.items()), (None, {}))
        regions = sport_node.get("region", {})

        # Collect current countries and initialize per‐region counters
        current_countries = set()
        for region_key, region in regions.items():
            alias = region.get("alias")
            current_countries.add(alias)
            region_id = region.get("id")
            responses_data[alias] = []
            region_expected_counts[alias] = 0
            region_processed_counts[alias] = 0

            for comp_id, comp in region.get("competition", {}).items():
                comp_name = comp.get("name")
                rid_new = make_rid()
                competition_requests[rid_new] = (alias, region_id, comp_id, comp_name)
                region_expected_counts[alias] += 1

        # Remove JSON files for any countries no longer present
        cleanup_obsolete_files(current_countries)

        # Schedule and dispatch one‐shot detailed requests
        expected_requests = len(competition_requests)
        print(f"⚙️ Scheduled {expected_requests} competition requests")
        for rid_new, (_, _, comp_id, _) in competition_requests.items():
            cmd = make_match_request(rid_new, int(comp_id))
            ws.send(json.dumps(cmd))

    # Detailed match response with retry/skip logic
    elif rid in competition_requests:
        alias, region_id, comp_id, comp_name = competition_requests[rid]

        # --- Retry/Skip Logic ---
        MAX_RETRIES = 4
        RETRY_DELAY_S = 0.3  # Total retry time: (4-1) * 0.3 = 0.9s
        success = False

        for attempt in range(MAX_RETRIES):
            try:
                payload = data.get("data", {}).get("data", {})
                sport_node = next(iter(payload.get("sport", {}).values()), {})
                region_node = sport_node.get("region", {}).get(str(region_id), {})
                comp_node = region_node.get("competition", {}).get(str(comp_id), {})
                games = comp_node.get("game", {})

                games = comp_node.get("game", {})

                matches = []
                for game in games.values():
                    ts = game.get("start_ts")
                    # Defensively skip games that are missing a timestamp
                    if ts is None:
                        print(
                            f"⚠️ Skipping game {game.get('id')} in competition '{comp_name}' due to missing start_ts.")
                        continue

                    dt_utc = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                    tz = datetime.timezone(datetime.timedelta(hours=1))
                    dt = dt_utc.astimezone(tz)
                    date_str = dt.strftime("%d/%m/%Y")
                    time_str = dt.strftime("%H:%M")

                    home_team_name = game.get("team1_name")
                    away_team_name = game.get("team2_name")

                    m = {
                        "match_id": game.get("id"),
                        "date": date_str,
                        "time": time_str,
                        "home_team": home_team_name,
                        "away_team": away_team_name
                    }

                    # ----- START: ADD URL BUILDING LOGIC -----
                    try:
                        mode_map = {"prematch": "pre-match", "live": "live"}
                        url_mode = mode_map.get(MODE, "pre-match")  # Default to pre-match
                        sport_alias = game.get("sport_alias", "Soccer")

                        # Slugify components according to the rules
                        tournament_name_slug = slugify(comp_name, remove_digits=True)
                        team_names_slug = slugify(f"{home_team_name}-{away_team_name}")

                        # Ensure all necessary parts for the URL exist
                        if all([sport_alias, alias, comp_id, tournament_name_slug, m["match_id"], team_names_slug]):
                            match_url = (
                                f"https://www.asbet.org/en/sports/{url_mode}/event-view/"
                                f"{sport_alias}/{alias}/{comp_id}/"
                                f"{tournament_name_slug}/{m['match_id']}/{team_names_slug}"
                            )
                            m["match_url"] = match_url
                    except Exception as e:
                        print(f"⚠️ Could not build URL for match {m['match_id']}: {e}")

                    # Extract odds by market type
                    for mkt in game.get("market", {}).values():
                        mtype = mkt.get("type")
                        base = mkt.get("base")
                        for ev in mkt.get("event", {}).values():
                            t = ev.get("type_1")
                            p = ev.get("price")

                            if mtype == "P1XP2":
                                if t == "W1": m["1_odd"] = p
                                elif t == "X": m["draw_odd"] = p
                                elif t == "W2": m["2_odd"] = p
                            elif mtype == "1X12X2":
                                if t == "1X": m["1X_odd"] = p
                                elif t == "12": m["12_odd"] = p
                                elif t == "X2": m["X2_odd"] = p
                            elif mtype == "BothTeamsToScore":
                                if t == "Yes": m["both_score_odd"] = p
                                elif t == "No": m["both_noscore_odd"] = p
                            elif mtype == "OverUnder":
                                try:
                                    b = float(base)
                                    if b * 2 == int(b * 2):
                                        if t == "Under": m[f"under_{b}_odd"] = p
                                        elif t == "Over": m[f"over_{b}_odd"] = p
                                # Catch errors if `base` is not a valid number (e.g., None)
                                except (ValueError, TypeError):
                                    pass
                            elif mtype == "AsianHandicap":
                                ev_base = ev.get("base")
                                if t == "Home": m[f"home_handicap_{ev_base}_odd"] = p
                                elif t == "Away": m[f"away_handicap_{ev_base}_odd"] = p
                    matches.append(m)

                # If processing succeeds, store the data
                responses_data[alias].append({
                    "tournament_name": comp_name,
                    "tournament_id": int(comp_id),
                    "matches": matches
                })
                success = True
                break  # Exit retry loop on success

            except Exception as e:
                print(f"⚠️ Attempt {attempt + 1}/{MAX_RETRIES} failed for '{comp_name}' ({comp_id}). Error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_S)
                else:
                    print(f"🚫 Skipping competition '{comp_name}' ({comp_id}) after all retries failed.")

        # This block MUST run regardless of success or failure to prevent freezing.
        processed_requests += 1
        region_processed_counts[alias] += 1
        print(
            f"Processed {processed_requests}/{expected_requests} for {alias} "
            f"({region_processed_counts[alias]}/{region_expected_counts[alias]})"
        )

        # Export per‐region file when done
        if region_processed_counts[alias] >= region_expected_counts[alias]:
            export_country_data(alias)

        # Close WebSocket when all are complete
        if processed_requests >= expected_requests:
            print("All countries completed!")
            ws.close()


def on_error(ws, error):
    print("❌ WebSocket error:", error)


def on_close(ws, code, msg):
    print(f"🔴 Connection closed (code={code}, msg={msg})")


def prepare_session_ids():
    """Regenerate SESSION_RID and GET_RID and patch the command dicts."""
    global SESSION_RID, GET_RID, request_session_cmd, request_counts_cmd
    SESSION_RID = make_rid()
    GET_RID     = make_rid()
    request_session_cmd["rid"] = SESSION_RID
    request_counts_cmd["rid"]  = GET_RID



new_session_each_cycle = False  # Set to False to reuse session RIDs across cycles

if LOOP:
    n = 0
    if __name__ == "__main__":
        n = 0
        while True:
            # If we want a fresh session each cycle, regenerate the RIDs now.
            if new_session_each_cycle or n == 0:
                prepare_session_ids()

            websocket.enableTrace(False)
            ws = websocket.WebSocketApp(
                "wss://eu-swarm-newm.betconstruct.com/",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            t = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 20})
            t.daemon = True
            t.start()

            try:
                while t.is_alive():
                    time.sleep(0.5)
            except KeyboardInterrupt:
                print("Interrupted, closing…")
                ws.close()
                t.join()
                break

            n += 1
            print(f"Cycle number {n} completed")
            sleep(CYCLE_DELAY)
else:
    if __name__ == "__main__":
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp(
            "wss://eu-swarm-newm.betconstruct.com/",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        t = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 20})
        t.daemon = True
        t.start()
        try:
            while t.is_alive(): time.sleep(0.5)
        except KeyboardInterrupt:
            print("Interrupted, closing…")
            ws.close()
            t.join()
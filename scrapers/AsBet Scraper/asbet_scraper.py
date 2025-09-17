import os
import sys
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
MODE = "prematch"  # Choose the mode "live" or "prematch"
SPORT = "football"  # Choose the sport "football" or ...
LOOP = False  # Choose if the code will loop or one time scrape
CYCLE_DELAY = 0.5  # Choose the delay in seconds between each cycle if loop is activated
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
MODE = args.mode  # Choose the mode "live" or "prematch"
SPORT = args.sport  # Choose the sport "football" or ...
LOOP = args.loop  # Choose if the code will loop or one time scrape
CYCLE_DELAY = args.delay  # Choose the delay in seconds between each cycle if loop is activated
# ----------------------------------------------------------------------------------------

# chdir into the folder that holds this script
os.chdir(os.path.dirname(os.path.abspath(__file__)))


def load_sport_id(sport):
    """Loads the sport_id from settings/<sport>/sport_id.json"""
    path = os.path.join("settings", sport, "sport_id.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"Loaded sport_id {data['sport_id']} from {path}")
            return data["sport_id"]
    except Exception as e:
        print(f"WARNING: Could not load sport_id from {path}: {e}")
        return None


# ----------------Load odds parsing configuration --------------------------------------------------
def load_odds_config(sport):
    """Load odds configuration from JSON file"""
    config_path = f"settings/{sport}/parsing.json"
    # Load existing config
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✅ Loaded parsing configuration from {config_path}")
        return config
    except Exception as e:
        print(f"❌ Error loading parsing configuration: {e}")
        print("Please put the configuration file into the settings folder")
        sys.exit(1)


# Load configuration
SPORT_ID = load_sport_id(SPORT)
ODDS_CONFIG = load_odds_config(SPORT)
MARKET_TYPES = ODDS_CONFIG.get("market_types", [])
ODDS_MAPPING = ODDS_CONFIG.get("odds_mapping", {})

print(f"📊 Loaded {len(MARKET_TYPES)} market types: {', '.join(MARKET_TYPES)}")


# ----------------------------------------------------------------------------------------

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


# ----------------Odds parsing function --------------------------------------------------
def parse_odds_for_match(match_dict, game_data):
    """Parse odds for a match using the configurable odds mapping"""
    try:
        for mkt in game_data.get("market", {}).values():
            mtype = mkt.get("type")
            base = mkt.get("base")

            # Skip if market type is not in our configuration
            if mtype not in ODDS_MAPPING:
                continue

            market_config = ODDS_MAPPING[mtype]
            events_config = market_config.get("events", {})

            for ev in mkt.get("event", {}).values():
                event_type = ev.get("type_1")
                price = ev.get("price")

                # Skip if event type is not in our configuration
                if event_type not in events_config:
                    continue

                # Get the output field name template
                output_field = events_config[event_type]

                # Handle markets that use base values
                if market_config.get("use_base") and base is not None:
                    try:
                        base_value = float(base)
                        # Apply base filter if specified
                        if market_config.get("base_filter") == "half_integers_only":
                            # Only process if base * 2 equals an integer (i.e., half integers)
                            if base_value * 2 == int(base_value * 2):
                                field_name = output_field.format(base=base_value)
                                match_dict[field_name] = price
                        else:
                            field_name = output_field.format(base=base_value)
                            match_dict[field_name] = price
                    except (ValueError, TypeError):
                        pass  # Skip if base is not a valid number

                # Handle markets that use event base values
                elif market_config.get("use_event_base"):
                    ev_base = ev.get("base")
                    if ev_base is not None:
                        field_name = output_field.format(base=ev_base)
                        match_dict[field_name] = price

                # Handle simple markets without base values
                else:
                    match_dict[output_field] = price

    except Exception as e:
        print(f"⚠️ Error parsing odds for match: {e}")


# ----------------------------------------------------------------------------------------

# Fixed RIDs for session and initial "count" request
SESSION_RID = make_rid()
GET_RID = make_rid()

# Directory to save scraped JSON files
if MODE == "live":
    OUTPUT_DIR = f"scraped_live_matches/{SPORT}"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
if MODE == "prematch":
    OUTPUT_DIR = f"scraped_prematch_matches/{SPORT}"
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
    mode_num = 1
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
                "sport": {"alias": SPORT_ID},
                "game": {"@and": [{"is_live": 1}, {"is_blocked": 0}]}
            }
        },
        "rid": GET_RID
    }
if MODE == "prematch":
    mode_num = 0
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
                "sport": {"alias": SPORT_ID},
                "game": {
                    "@and": [
                        {"is_blocked": 0},
                        {"is_live": 0},
                        {
                            "@or": [
                                {"visible_in_prematch": 1},
                                {"type": {"@in": [0, 2]}}
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
                "sport": {"alias": SPORT_ID},
                "competition": {"id": comp_id},
                "game": {"@and": [{"is_live": mode_num}, {"is_blocked": 0}]},
                "market": {"type": {"@in": MARKET_TYPES}}
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

# Tunisia timezone (UTC+1) - defined once for efficiency
TUNISIA_TZ = datetime.timezone(datetime.timedelta(hours=1))

def export_country_data(alias):
    """Export data for a specific country immediately"""
    if alias in responses_data:
        current_time = datetime.datetime.now(TUNISIA_TZ)
        last_updated = current_time.isoformat()

        # Prepare the data with last_updated at the top
        export_data = [
            {"last_updated": last_updated}
        ]
        export_data.extend(responses_data[alias])

        fn = os.path.join(OUTPUT_DIR, f"{alias}.json")
        with open(fn, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=4, ensure_ascii=False)
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
        competition_requests.clear()  # drop last cycle's queued RIDs
        processed_requests = 0  # reset how many requests we've handled
        expected_requests = 0  # reset the total‐to‐process count
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
                        sport_alias = game.get("sport_alias", SPORT_ID)

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

                    # Use the new configurable odds parsing function
                    parse_odds_for_match(m, game)

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
    GET_RID = make_rid()
    request_session_cmd["rid"] = SESSION_RID
    request_counts_cmd["rid"] = GET_RID


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
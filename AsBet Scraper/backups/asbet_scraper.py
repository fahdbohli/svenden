import os
import json
import uuid
import time
import threading
import datetime
import websocket

# Directory to save scraped JSON files
OUTPUT_DIR = "scraped_matches"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# Generate a unique request ID
def make_rid():
    return uuid.uuid4().hex


# Fixed RIDs for session and initial "count" request
SESSION_RID = make_rid()
GET_RID = make_rid()

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
        "release_date": "04/07/2025-19:23",
        "afec": "2KWqQHPpEm4ZSsgFmyqxfjoJOJM3fbFZKWHg"
    },
    "rid": SESSION_RID
}

# Live and Prematch indicators
LIVE = 0
PREMATCH = 1

# Choose the mode
mode = PREMATCH

# Command: get counts of games per competition
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
            "game": {"@or": [{"visible_in_prematch": mode}, {"type": {"@in": [0, 2]}}]}
        }
    },
    "rid": GET_RID
}


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
                "sport": {"alias": "Soccer"},
                "competition": {"id": comp_id},
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
        sport_payload = data.get("data", {}).get("data", {}).get("sport", {})
        _, sport_node = next(iter(sport_payload.items()), (None, {}))
        regions = sport_node.get("region", {})

        # Collect current countries and clean up obsolete files
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

        # Clean up obsolete country files
        cleanup_obsolete_files(current_countries)

        expected_requests = len(competition_requests)
        print(f"⚙️ Scheduled {expected_requests} competition requests")
        for rid_new, (_, _, comp_id, _) in competition_requests.items():
            cmd = make_match_request(rid_new, int(comp_id))
            ws.send(json.dumps(cmd))

    # Detailed match response
    elif rid in competition_requests:
        alias, region_id, comp_id, comp_name = competition_requests[rid]
        payload = data.get("data", {}).get("data", {})
        sport_node = next(iter(payload.get("sport", {}).values()), {})
        region_node = sport_node.get("region", {}).get(str(region_id), {})
        comp_node = region_node.get("competition", {}).get(str(comp_id), {})
        games = comp_node.get("game", {})

        matches = []
        for game in games.values():
            ts = game.get("start_ts")
            dt_utc = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
            tz = datetime.timezone(datetime.timedelta(hours=1))
            dt = dt_utc.astimezone(tz)
            date_str = dt.strftime("%d/%m/%Y")
            time_str = dt.strftime("%H:%M")
            m = {
                "match_id": game.get("id"),
                "date": date_str,
                "time": time_str,
                "home_team": game.get("team1_name"),
                "away_team": game.get("team2_name")
            }
            # Extract odds by market type
            for mkt in game.get("market", {}).values():
                mtype = mkt.get("type")
                base = mkt.get("base")
                for ev in mkt.get("event", {}).values():
                    t = ev.get("type_1")
                    p = ev.get("price")
                    # Mainline 1X2
                    if mtype == "P1XP2":
                        if t == "W1":
                            m["1_odd"] = p
                        elif t == "X":
                            m["draw_odd"] = p
                        elif t == "W2":
                            m["2_odd"] = p
                    # Double Chance
                    elif mtype == "1X12X2":
                        if t == "1X":
                            m["1X_odd"] = p
                        elif t == "12":
                            m["12_odd"] = p
                        elif t == "X2":
                            m["X2_odd"] = p
                    # Both Teams To Score
                    elif mtype == "BothTeamsToScore":
                        if t == "Yes":
                            m["both_score_odd"] = p
                        elif t == "No":
                            m["both_noscore_odd"] = p
                    # Over/Under: capture only half and whole lines
                    elif mtype == "OverUnder":
                        try:
                            b = float(base)
                            if b * 2 == int(b * 2):
                                if t == "Under":
                                    m[f"under_{b}_odd"] = p
                                elif t == "Over":
                                    m[f"over_{b}_odd"] = p
                        except:
                            pass
                            # Asian Handicap / Total Handicap
                    elif mtype == "AsianHandicap":
                        # Use the event's own base value (not the market's) to label correctly
                        ev_base = ev.get("base")
                        if t == "Home":
                            m[f"home_handicap_{ev_base}_odd"] = p
                        elif t == "Away":
                            m[f"away_handicap_{ev_base}_odd"] = p
            matches.append(m)

        responses_data[alias].append({
            "tournament_name": comp_name,
            "tournament_id": comp_id,
            "matches": matches
        })

        # Update counters
        processed_requests += 1
        region_processed_counts[alias] += 1

        print(
            f"Processed {processed_requests}/{expected_requests} for {alias} ({region_processed_counts[alias]}/{region_expected_counts[alias]})")

        # Check if this country is complete and export immediately
        if region_processed_counts[alias] >= region_expected_counts[alias]:
            export_country_data(alias)

        # If all done, close connection
        if processed_requests >= expected_requests:
            print("All countries completed!")
            ws.close()


def on_error(ws, error):
    print("❌ WebSocket error:", error)


def on_close(ws, code, msg):
    print(f"🔴 Connection closed (code={code}, msg={msg})")


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
        while t.is_alive(): time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted, closing…")
        ws.close()
        t.join()
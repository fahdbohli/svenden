# ev_calculator.py

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from file_utils import load_json_from_file, save_json_to_file

# --- Global placeholders (populated by main.py) ---
METHOD: str = "ONE_SHARPING"
SHARP_SOURCE: str = ""
SHARPING_GROUP: List[str] = []
EV_SOURCE: str = ""
ODDS_INTERVAL: List[float] = [1.0, 10.0]
MIN_OVERPRICE: float = 0.0
MARKET_SETS: Dict[str, List[str]] = {}
URL_TEMPLATES: Dict[str, str] = {}
SPORT_NAME: str = ""
MODE_NAME: str = ""

# --- New placeholders for Overprice Source Logging ---
OVERPRICE_SOURCE_LOGGING: bool = False
APPEARANCE_INVESTIGATION: bool = False
DOUBLE_CHECK: bool = False
INVESTIGATION_TIMEOUT_MINUTES: int = 5 # Timeout for pending investigations
# ----------------------------------------------------


def build_source_url(source_name: str, match_data: Dict[str, Any]) -> str:
    """
    Builds a specific match URL for a given source using URL_TEMPLATES.
    This version handles case-insensitivity and the new config structure
    with 'template' and 'mappings' keys.
    """
    # Create a case-insensitive lookup dictionary on the fly.
    url_templates_lower = {k.lower(): v for k, v in URL_TEMPLATES.items()}
    template_config = url_templates_lower.get(source_name.lower())

    if match_data.get("match_url"):
        return match_data["match_url"]

    if not template_config:
        if source_name not in getattr(build_source_url, "warned_sources", set()):
            print(f"[URL_BUILDER_WARN] No URL template found for source: '{source_name}' in url_builder.json")
            if not hasattr(build_source_url, "warned_sources"):
                build_source_url.warned_sources = set()
            build_source_url.warned_sources.add(source_name)
        return ""

    template = template_config.get("template")
    mappings = template_config.get("mappings", {})

    if not template:
        print(f"[URL_BUILDER_WARN] Template config for '{source_name}' is missing the 'template' string.")
        return ""

    try:
        format_data = {
            "sport": SPORT_NAME,
            "mode": MODE_NAME,
            **match_data
        }

        if 'mode' in mappings and MODE_NAME in mappings.get('mode', {}):
            format_data['mode'] = mappings['mode'][MODE_NAME]
        if 'sport' in mappings and SPORT_NAME in mappings.get('sport', {}):
            format_data['sport'] = mappings['sport'][SPORT_NAME]

        required_keys = [k.split('}')[0] for k in template.split('{')[1:]]
        missing_keys = [key for key in required_keys if key not in format_data or not format_data[key]]
        if missing_keys:
            return ""

        return template.format(**format_data)

    except (KeyError, TypeError) as e:
        print(f"[URL_BUILDER_ERROR] Failed to format URL for {source_name} (match_id: {match_data.get('match_id')}). "
              f"Error: {e}. Check template placeholders and mappings.")
        return ""


def get_fair_odds_one_sharp(market_set: List[str], sharp_match: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """Calculates fair odds by removing the vig from a single sharp source."""
    odds_values = [sharp_match.get(odd_name) for odd_name in market_set]
    if not all(isinstance(o, (int, float)) and o > 0 for o in odds_values):
        return None

    vig_sum = sum(1.0 / o for o in odds_values)
    if vig_sum <= 0:
        return None

    fair_odds = {
        market_set[i]: round(val * vig_sum, 4)
        for i, val in enumerate(odds_values)
    }
    return fair_odds


def get_fair_odds_multiple_sharp(market_set: List[str], matches_by_src: Dict[str, Dict[str, Any]]) -> Optional[
    Dict[str, float]]:
    """Calculates fair odds based on the average odds from a group of sharp sources."""
    avg_odds_calculator = {odd_name: {'sum': 0.0, 'count': 0} for odd_name in market_set}

    for src in SHARPING_GROUP:
        if src in matches_by_src:
            match = matches_by_src[src]
            for odd_name in market_set:
                odd_value = match.get(odd_name)
                if isinstance(odd_value, (int, float)) and odd_value > 0:
                    avg_odds_calculator[odd_name]['sum'] += odd_value
                    avg_odds_calculator[odd_name]['count'] += 1

    avg_odds = {}
    for odd_name, data in avg_odds_calculator.items():
        if data['count'] > 0:
            avg_odds[odd_name] = data['sum'] / data['count']
        else:
            return None

    odds_values = list(avg_odds.values())
    vig_sum = sum(1.0 / o for o in odds_values)
    if vig_sum <= 0:
        return None

    fair_odds = {
        name: round(val * vig_sum, 4)
        for name, val in avg_odds.items()
    }
    return fair_odds


def analyze_ev_opportunities(group: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Analyzes a group of matched matches to find Positive Expected Value (+EV) opportunities.
    """
    if not group:
        return None

    matches_by_src = {m['source']: m for m in group}

    if EV_SOURCE not in matches_by_src:
        return None

    if METHOD == "ONE_SHARPING" and SHARP_SOURCE not in matches_by_src:
        return None
    if METHOD == "MULTIPLE_SHARPING" and not any(s in matches_by_src for s in SHARPING_GROUP):
        return None

    found_opportunities = []
    ev_source_match = matches_by_src[EV_SOURCE]

    for market_name, market_odds_list in MARKET_SETS.items():
        fair_odds = None

        if METHOD == "ONE_SHARPING":
            if SHARP_SOURCE not in matches_by_src: continue
            sharp_match = matches_by_src[SHARP_SOURCE]
            fair_odds = get_fair_odds_one_sharp(market_odds_list, sharp_match)
        elif METHOD == "MULTIPLE_SHARPING":
            fair_odds = get_fair_odds_multiple_sharp(market_odds_list, matches_by_src)

        if not fair_odds:
            continue

        for odd_name, fair_value in fair_odds.items():
            if not (ODDS_INTERVAL[0] <= fair_value <= ODDS_INTERVAL[1]):
                continue

            ev_source_odd = ev_source_match.get(odd_name)
            if not isinstance(ev_source_odd, (int, float)) or ev_source_odd <= 0:
                continue

            if ev_source_odd > fair_value:
                overprice = (ev_source_odd / fair_value) - 1.0

                if overprice >= MIN_OVERPRICE:
                    ev_opp = {
                        "source": EV_SOURCE,
                        "odd_name": odd_name,
                        "overpriced_odd_value": ev_source_odd,
                        "fair_odd_value": round(fair_value, 4),
                        "overprice": round(overprice, 4),
                        "unique_id": f"{ev_source_match.get('match_id')}-{odd_name}",
                        f"{EV_SOURCE}_country_name": ev_source_match.get("country_name", ""),
                        f"tournament_{EV_SOURCE}": ev_source_match.get("tournament_name", ""),
                        f"{EV_SOURCE}_match_id": ev_source_match.get("match_id", ""),
                        f"{EV_SOURCE}_tournament_id": ev_source_match.get("tournament_id", ""),
                        f"{EV_SOURCE}_match_url": build_source_url(EV_SOURCE, ev_source_match),
                    }
                    found_opportunities.append(ev_opp)

    if found_opportunities:
        base_match = ev_source_match
        country_canonical = base_match.get('country')
        if not country_canonical and group:
            country_canonical = group[0].get('country_name')

        ev_group_object = {
            "group_id": base_match.get("matching_group_id"),
            "home_team": base_match.get("home_team"),
            "away_team": base_match.get("away_team"),
            "date": base_match.get("date"),
            "time": base_match.get("time"),
            "country": country_canonical,
            "all_sources": sorted(list(matches_by_src.keys())),
            "opportunities": found_opportunities
        }
        return ev_group_object

    return None

def _write_ev_log(log_entry: Dict[str, Any], log_output_root: str, investigation_type: str):
    """Saves a single log entry to the correct file, using the new directory structure."""
    overprice_source_folder = log_entry["overprice_source"]
    today_str = datetime.now().strftime("%d-%m-%Y")
    odd_name_sanitized = log_entry['odd_name'].replace('/', '_')
    group_id = log_entry['group_id']

    log_dir = os.path.join(
        log_output_root, MODE_NAME, EV_SOURCE, SPORT_NAME, today_str,
        overprice_source_folder, group_id, investigation_type
    )
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, f"{odd_name_sanitized}.json")

    try:
        if os.path.exists(log_file_path):
            with open(log_file_path, "r", encoding="utf-8") as f:
                logs = json.load(f)
        else:
            logs = []
    except (json.JSONDecodeError, IOError):
        logs = []
    logs.append(log_entry)
    with open(log_file_path, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)

# --- NEW FUNCTION TO WRITE APPEARANCE LOG ---
def write_appearance_log_immediately(log_entry: Dict[str, Any], log_output_root: str):
    """
    Takes a completed appearance log entry and writes it to the file system immediately.
    """
    if not log_entry or not log_output_root:
        return
    _write_ev_log(log_entry, log_output_root, "appearance_investigations")


def analyze_ev_appearance(
    current_match_group: List[Dict],
    previous_match_group: List[Dict],
    current_opportunity: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Analyzes the appearance of a new EV opportunity by comparing current odds
    with the odds from the previous cycle.
    """
    odd_name = current_opportunity['odd_name']
    market_set = next((ms for ms in MARKET_SETS.values() if odd_name in ms), None)
    if not market_set: return None

    matches_by_src_current = {m['source']: m for m in current_match_group}
    ev_match_current = matches_by_src_current.get(EV_SOURCE)
    sharp_match_current = matches_by_src_current.get(SHARP_SOURCE)

    matches_by_src_previous = {m['source']: m for m in previous_match_group}
    ev_match_previous = matches_by_src_previous.get(EV_SOURCE)
    sharp_match_previous = matches_by_src_previous.get(SHARP_SOURCE)

    if not all([ev_match_current, sharp_match_current, ev_match_previous, sharp_match_previous]): return None
    if not all(k in v for k in [odd_name] for v in [ev_match_current, sharp_match_current, ev_match_previous, sharp_match_previous]):
        return None

    new_fair_odds = get_fair_odds_one_sharp(market_set, sharp_match_current)
    old_fair_odds = get_fair_odds_one_sharp(market_set, sharp_match_previous)
    if not new_fair_odds or not old_fair_odds: return None
    new_fair_odd = new_fair_odds.get(odd_name)
    old_fair_odd = old_fair_odds.get(odd_name)
    new_ev_odd = ev_match_current.get(odd_name)
    old_ev_odd = ev_match_previous.get(odd_name)

    if not all([new_fair_odd, old_fair_odd, new_ev_odd, old_ev_odd]): return None

    fair_odd_changed = (new_fair_odd != old_fair_odd)
    ev_odd_changed = (new_ev_odd != old_ev_odd)

    overprice_source = None
    if fair_odd_changed and ev_odd_changed:
        return None

    if fair_odd_changed and not ev_odd_changed:
        if new_fair_odd < old_fair_odd: overprice_source = EV_SOURCE
    elif ev_odd_changed and not fair_odd_changed:
        if new_ev_odd > old_ev_odd: overprice_source = "fair_source"

    if overprice_source:
        log_entry = {
            "overprice": current_opportunity['overprice'],
            "overprice_source": overprice_source,
            "odd_name": odd_name,
            "old_fair_odd": old_fair_odd,
            f"old_{EV_SOURCE}_odd": old_ev_odd,
            "new_fair_odd": new_fair_odd,
            f"new_{EV_SOURCE}_odd": new_ev_odd,
            "group_id": current_opportunity.get("group_id"),
            "home_team": current_opportunity.get("home_team"),
            "away_team": current_opportunity.get("away_team"),
            "appeared_at": datetime.now().isoformat(),
        }
        return log_entry
    return None


def _resolve_disappearance(
    last_known_opp: Dict[str, Any],
    all_match_groups_by_id: Dict[str, List[Dict[str, Any]]],
    activity_data: Dict[str, Any],
    log_output_root: str
) -> (bool, Optional[Dict[str, Any]]):
    """
    Tries to find new odds for a disappeared opportunity. Returns (True, log_entry) if resolved,
    or (False, None) if pending. Special case is (True, None) for false/pre-investigated disappearances.
    """
    group_id = last_known_opp.get("group_id")
    unique_id = last_known_opp.get('unique_id')
    activity_entry = activity_data.get(unique_id)
    if activity_entry and isinstance(activity_entry, dict) and activity_entry.get("appearance_log"):
        if not DOUBLE_CHECK:
            # finalize the original appearance investigation
            final_log = activity_entry["appearance_log"]
            final_log["opportunity_duration"] = last_known_opp.get("activity_duration", "unknown")
            _write_ev_log(final_log, log_output_root, "appearance_investigations")
            print(f"[EV_LOG] Finalized appearance investigation for {unique_id}.")
            # now remove it so we don't keep finalizing it on every subsequent run
            activity_data.pop(unique_id, None)
            return True, None

    if not group_id or group_id not in all_match_groups_by_id: return False, None
    match_group = all_match_groups_by_id[group_id]
    matches_by_src = {m['source']: m for m in match_group}
    odd_name = last_known_opp["odd_name"]
    market_set = next((ms for ms in MARKET_SETS.values() if odd_name in ms), None)
    if not market_set: return False, None

    new_fair_odd_val = None
    if METHOD == "ONE_SHARPING" and SHARP_SOURCE in matches_by_src:
        fair_odds_obj = get_fair_odds_one_sharp(market_set, matches_by_src[SHARP_SOURCE])
        if fair_odds_obj: new_fair_odd_val = fair_odds_obj.get(odd_name)
    elif METHOD == "MULTIPLE_SHARPING":
        fair_odds_obj = get_fair_odds_multiple_sharp(market_set, matches_by_src)
        if fair_odds_obj: new_fair_odd_val = fair_odds_obj.get(odd_name)

    new_ev_odd_val = matches_by_src.get(EV_SOURCE, {}).get(odd_name)
    if new_fair_odd_val is None or new_ev_odd_val is None: return False, None

    if new_ev_odd_val > new_fair_odd_val:
        new_overprice = (new_ev_odd_val / new_fair_odd_val) - 1.0
        if new_overprice >= MIN_OVERPRICE:
            print(f"[EV_LOG] Cancelling investigation for {unique_id}. Opportunity is still active with new odds.")
            return True, None

    old_fair_odd = last_known_opp["fair_odd_value"]
    old_ev_odd = last_known_opp["overpriced_odd_value"]
    if old_fair_odd <= 0 or old_ev_odd <= 0: return False, None

    fair_change_pct = abs((new_fair_odd_val - old_fair_odd) / old_fair_odd) if old_fair_odd > 0 else float('inf')
    ev_change_pct = abs((new_ev_odd_val - old_ev_odd) / old_ev_odd) if old_ev_odd > 0 else float('inf')
    overprice_source = EV_SOURCE if ev_change_pct > fair_change_pct else "fair_source"

    log_entry = {
        "overprice": last_known_opp["overprice"],
        "overprice_source": overprice_source,
        "odd_name": odd_name,
        "old_fair_odd": old_fair_odd,
        f"old_{EV_SOURCE}_odd": old_ev_odd,
        "new_fair_odd": round(new_fair_odd_val, 4),
        f"new_{EV_SOURCE}_odd": new_ev_odd_val,
        "opportunity_duration": last_known_opp.get("activity_duration", "unknown"),
        "group_id": group_id,
        "home_team": last_known_opp.get("home_team"),
        "away_team": last_known_opp.get("away_team"),
        "disappeared_at": datetime.now().isoformat(),
    }
    _write_ev_log(log_entry, log_output_root, "disappearance_investigations")
    return True, log_entry


def handle_opportunity_lifecycle(
    all_match_groups_by_id: Dict[str, List[Dict]],
    pending_investigations: Dict[str, Any],
    log_output_root: str,
    activity_data: Dict[str, Any]
) -> Dict[str, Any]:
    if not OVERPRICE_SOURCE_LOGGING: return {}
    now = datetime.now()
    updated_pending = {}
    for uid, pending_data in pending_investigations.items():
        disappeared_time = datetime.fromisoformat(pending_data["disappeared_at"])
        if (now - disappeared_time) > timedelta(minutes=INVESTIGATION_TIMEOUT_MINUTES):
            print(f"[EV_LOG] Investigation for {uid} timed out after {INVESTIGATION_TIMEOUT_MINUTES} minutes.")
            continue
        resolved, log_entry = _resolve_disappearance(
            pending_data["last_known_opp"], all_match_groups_by_id, activity_data, log_output_root
        )
        if resolved:
            if log_entry:
                print(f"[EV_LOG] Resolved and logged disappearance for {uid}.")
        else:
            updated_pending[uid] = pending_data
    return updated_pending


def manage_ev_lifecycle(
    current_opportunities_cache: Dict[str, Any],
    all_match_groups_by_id: Dict[str, List[Dict]],
    output_dir: str,
    log_output_root: str,
    activity_data: Dict[str, Any]
):
    """
    Manages the EV opportunity lifecycle, now accepting activity_data to pass to sub-functions.
    """
    if not OVERPRICE_SOURCE_LOGGING:
        return

    cache_dir = os.path.join(output_dir, "_cache")
    os.makedirs(cache_dir, exist_ok=True)
    EV_OPP_CACHE_PATH = os.path.join(cache_dir, "ev_opportunity_cache.json")
    PURGATORY_CACHE_PATH = os.path.join(cache_dir, "purgatory_cache.json")
    PENDING_INVESTIGATIONS_PATH = os.path.join(cache_dir, "pending_investigations.json")

    previous_opp_cache = load_json_from_file(EV_OPP_CACHE_PATH)
    purgatory_cache = load_json_from_file(PURGATORY_CACHE_PATH)
    pending_investigations = load_json_from_file(PENDING_INVESTIGATIONS_PATH)
    print(f"[EV_LOG] Loaded {len(previous_opp_cache)} cached opps, {len(purgatory_cache)} in purgatory, {len(pending_investigations)} pending.")

    items_to_investigate_now = {}
    for uid, last_known_opp in purgatory_cache.items():
        if uid not in current_opportunities_cache:
            items_to_investigate_now[uid] = {
                "disappeared_at": datetime.now().isoformat(),
                "last_known_opp": last_known_opp
            }
    if items_to_investigate_now:
        print(f"[EV_LOG] {len(items_to_investigate_now)} opps from purgatory confirmed disappeared; queuing for investigation.")

    next_run_purgatory_cache = {}
    disappeared_this_cycle_ids = set(previous_opp_cache.keys()) - set(current_opportunities_cache.keys())
    for uid in disappeared_this_cycle_ids:
        next_run_purgatory_cache[uid] = previous_opp_cache[uid]
    if next_run_purgatory_cache:
        print(f"[EV_LOG] {len(next_run_purgatory_cache)} new opps disappeared; moved to purgatory for next cycle.")

    all_items_to_process = {**pending_investigations, **items_to_investigate_now}
    if all_items_to_process:
        updated_pending = handle_opportunity_lifecycle(
            all_match_groups_by_id,
            all_items_to_process,
            log_output_root,
            activity_data
        )
    else:
        updated_pending = {}

    save_json_to_file(EV_OPP_CACHE_PATH, current_opportunities_cache)
    save_json_to_file(PURGATORY_CACHE_PATH, next_run_purgatory_cache)
    save_json_to_file(PENDING_INVESTIGATIONS_PATH, updated_pending)
    print(f"[EV_LOG] Saved: {len(current_opportunities_cache)} active, {len(next_run_purgatory_cache)} to purgatory, {len(updated_pending)} pending.")
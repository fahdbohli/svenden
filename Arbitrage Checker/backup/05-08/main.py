# main.py

import os
import json
import shutil
import time
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo



# ----- Choose default configuration ---------------------------------------
MODE = "live"  # "prematch" or "live"
SPORT = "football"  # e.g., "football", "basketball", "tennis" ...
CHECKING_MODE = "ev"  # "arb" for arbitrage or "ev" for expected value
LOOP = True  # choose the default loop state
DELAY = 1  # choose the default delay between checking cycles
SHOW_ONLY_CONFIRMED = False  # default for the new confirmation logic
# --------------------------------------------------------------------------

# ----- Parse command-line arguments --------------------------------
parser = argparse.ArgumentParser(description="Process sport arbitrage/EV opportunities.")
parser.add_argument(
    "--mode", choices=["prematch", "live"], default=MODE,
    help=f"Choose mode: prematch or live (default : {MODE})"
)
parser.add_argument(
    "--sport", default=SPORT,
    help=f"Choose sport (e.g., football, basketball) (default : {SPORT})"
)
parser.add_argument(
    "--loop", action="store_true", default=LOOP,
    help=f"If set, the script will loop continuously (default : {LOOP})"
)
parser.add_argument(
    "--check", choices=["arb", "ev"], dest="checking_mode",
    default=CHECKING_MODE,
    help=f"Choose checking mode: arb or ev (default : {CHECKING_MODE})"
)
parser.add_argument(
    '--delay', type=float, default=DELAY,
    help=f'Delay in seconds between each checking cycle if loop is activated (default: {DELAY})'
)
# --- NEW ARGUMENT ---
parser.add_argument(
    "--show-only-confirmed", action="store_true", default=SHOW_ONLY_CONFIRMED,
    help="If set, only show arbitrage opportunities after all involved sources have been updated."
)

args = parser.parse_args()

# Override defaults with parsed arguments
MODE = args.mode
SPORT = args.sport
CHECKING_MODE = args.checking_mode
LOOP = args.loop
DELAY = args.delay
# --- NEW SETTING ---
SHOW_ONLY_CONFIRMED = args.show_only_confirmed
# --------------------------------------------------------------------


# ----- Load and Override Synonyms in matcher -----
import matcher

SYN_PATH = os.path.join("settings", SPORT, "synonyms.json")
with open(SYN_PATH, encoding="utf-8") as syn_file:
    syn_conf = json.load(syn_file)
matcher.SYN_GROUPS = syn_conf.get("synonyms", [])
matcher.SYN_PRIMARY = {
    syn: group[0]
    for group in matcher.SYN_GROUPS
    for syn in group
}

# ----- Load Common Settings for This Sport/Mode -----
SETTINGS_PATH = os.path.join("settings", SPORT, "settings.json")
with open(SETTINGS_PATH, encoding="utf-8") as sf:
    all_settings = json.load(sf)

if SPORT not in all_settings:
    raise ValueError(f"Sport '{SPORT}' not found in settings.")
if MODE not in all_settings[SPORT]:
    raise ValueError(f"Mode '{MODE}' not found under sport '{SPORT}'.")

selected_settings = all_settings[SPORT][MODE]
SOURCE_DIRECTORIES = [
    (entry["name"], entry["path"])
    for entry in selected_settings["source_directories"]
]
matcher.STRONG_THRESHOLD = selected_settings["strong_threshold"]
matcher.MODERATE_THRESHOLD = selected_settings["moderate_threshold"]
matcher.TIME_DIFF_TOLERANCE = selected_settings["time_diff_tolerance"]
matcher.GATEKEEPER_THRESHOLD = selected_settings["gatekeeper_threshold"]
matcher.DAY_DIFF_TOLERANCE = selected_settings["day_diff_tolerance"]

# ----- Load Team-Matching Constants into matcher -----
TEAM_CONF_PATH = os.path.join("settings", SPORT, "matching_helper.json")
with open(TEAM_CONF_PATH, encoding="utf-8") as tf:
    team_conf = json.load(tf)

matcher.IMPORTANT_TERM_GROUPS = team_conf["important_terms"]
matcher.COMMON_TEAM_WORDS = set(team_conf["common_team_words"])
matcher.LOCATION_IDENTIFIERS = set(team_conf["location_identifiers"])
matcher.TEAM_SYNONYMS = [set(group) for group in team_conf["team_synonyms"]]



# ----- Import Calculators and Other Modules -----
import arb_calculator
import ev_calculator
from file_utils import (
    get_all_canonical_countries,
    get_country_file_paths,
    load_matches,
    cleanup_old_files,
    load_activity_data,
    save_activity_data,
    load_json_from_file,
    save_json_to_file
)
from matcher import find_all_matching_matches
from arb_calculator import analyze_optimal_arbitrage
from ev_calculator import analyze_ev_opportunities, manage_ev_lifecycle
from utils import dedupe_all_country_files, test_team_matching


# ----- Conditionally Load Mode-Specific Settings -----

if CHECKING_MODE == "arb":
    # Load settings for Arbitrage mode
    print("Running in Arbitrage (arb) mode.")
    OUTPUT_DIR = selected_settings["output_dir"]
    OUTPUT_DIR = os.path.join(OUTPUT_DIR, MODE, SPORT)
    ARB_SETTINGS_PATH = os.path.join("settings", SPORT, "arb.json")
    if os.path.exists(ARB_SETTINGS_PATH):
        with open(ARB_SETTINGS_PATH, encoding="utf-8") as arbf:
            arb_settings = json.load(arbf)
        arb_calculator.MISVALUE_DETECTION_METHOD = arb_settings.get("MISVALUE_DETECTION_METHOD", "comparaison")
        arb_calculator.APPEARANCE_INVESTIGATION_LOGGING = arb_settings.get("APPEARANCE_INVESTIGATION_LOGGING", False)
        # Define the root path for the new logs
        arb_output_base = os.path.dirname(os.path.dirname(OUTPUT_DIR)) # Gets C:\...\arbitrage-viewer\public\arb_output
        arb_calculator.LOG_OUTPUT_ROOT = os.path.join(arb_output_base, "misvalue_source_log")
    else:
        # Fallback if arb.json doesn't exist to prevent crashes
        arb_calculator.MISVALUE_DETECTION_METHOD = "comparaison"
        arb_calculator.APPEARANCE_INVESTIGATION_LOGGING = False
        arb_calculator.LOG_OUTPUT_ROOT = ""
    with open(os.path.join("settings", SPORT, "markets.json"), encoding="utf-8") as mfile:
        markets_root = json.load(mfile)
        arb_calculator.MARKET_SETS = markets_root["market_sets"]
    arb_calculator.build_market_categories()
    URL_BUILDER_PATH = os.path.join("settings", SPORT, "url_builder.json")
    with open(URL_BUILDER_PATH, encoding="utf-8") as url_file:
        url_conf = json.load(url_file)
        arb_calculator.URL_TEMPLATES = url_conf.get("url_templates", {})
        arb_calculator.SPORT_NAME = SPORT
        arb_calculator.MODE_NAME = MODE
    analyzer_function = lambda grp, prev_data, act_data: analyze_optimal_arbitrage(
        matching_group=grp,
        previous_match_data=prev_data,
        activity_data=act_data
    )

elif CHECKING_MODE == "ev":
    # Load settings for EV mode
    print("Running in Positive EV (ev) mode.")
    EV_SETTINGS_PATH = os.path.join("settings", SPORT, "ev.json")
    with open(EV_SETTINGS_PATH, encoding="utf-8") as evf:
        ev_settings = json.load(evf)
    ev_source_name = ev_settings["EV_SOURCE"]
    output_directory = ev_settings["OUTPUT_DIRECTORY"]
    OUTPUT_DIR = os.path.join(output_directory, "ev_opportunities", MODE, ev_source_name, SPORT)
    ev_calculator.METHOD = ev_settings["METHOD"]
    ev_calculator.SHARP_SOURCE = ev_settings["SHARP_SOURCE"]
    ev_calculator.SHARPING_GROUP = ev_settings["SHARPING_GROUP"]
    ev_calculator.EV_SOURCE = ev_source_name
    ev_calculator.ODDS_INTERVAL = ev_settings["ODDS_INTERVAL"]
    ev_calculator.MIN_OVERPRICE = ev_settings["MIN_OVERPRICE"]
    ev_calculator.OVERPRICE_SOURCE_LOGGING = ev_settings.get("OVERPRICE_SOURCE_LOGGING", False)
    ev_calculator.APPEARANCE_INVESTIGATION = ev_settings.get("APPEARANCE_INVESTIGATION", False)
    ev_calculator.DOUBLE_CHECK = ev_settings.get("DOUBLE_CHECK", False)
    with open(os.path.join("settings", SPORT, "markets.json"), encoding="utf-8") as mfile:
        markets_root = json.load(mfile)
        ev_calculator.MARKET_SETS = markets_root["market_sets"]
    URL_BUILDER_PATH = os.path.join("settings", SPORT,"url_builder.json")
    with open(URL_BUILDER_PATH, encoding="utf-8") as url_file:
        url_conf = json.load(url_file)
        ev_calculator.URL_TEMPLATES = url_conf.get("url_templates", {})
        ev_calculator.SPORT_NAME = SPORT
        ev_calculator.MODE_NAME = MODE
    analyzer_function = lambda grp, prev_data, act_data: analyze_ev_opportunities(
        group=grp,
        previous_match_data=prev_data,
        activity_data=act_data
    )

else:
    raise ValueError(f"Invalid CHECKING_MODE: '{CHECKING_MODE}'. Must be 'arb' or 'ev'.")


def format_duration(total_seconds: float) -> str:
    """Formats a duration in seconds into a human-readable string."""
    if total_seconds < 60: return f"{round(total_seconds)} seconds"
    minutes = round(total_seconds / 60)
    if minutes < 60: return f"{minutes} minute" if minutes == 1 else f"{minutes} minutes"
    hours = round(minutes / 60)
    return f"{hours} hour" if hours == 1 else f"{hours} hours"


# ----- Main Processing Function -----
def process_files_optimal():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output will be written to: {OUTPUT_DIR}")
    if SHOW_ONLY_CONFIRMED:
        print("Confirmation mode is ON. Only showing arbs after all sources update.")

    total_matching_groups, total_opp_groups, total_opps = 0, 0, 0
    all_margins, results_by_country = [], {}
    ACTIVITY_TRACKER_PATH = os.path.join(OUTPUT_DIR, "activity_tracker.json")
    activity_data = load_activity_data(ACTIVITY_TRACKER_PATH)

    UNCONFIRMED_OPPS_PATH = os.path.join(OUTPUT_DIR, "unconfirmed_opportunities.json")
    unconfirmed_opps_cache = load_json_from_file(UNCONFIRMED_OPPS_PATH)
    current_unconfirmed_opps = {}

    cache_dir = os.path.join(OUTPUT_DIR, "_cache")
    PREV_MATCH_DATA_PATH = os.path.join(cache_dir, "previous_match_data_cache.json")
    previous_match_data = load_json_from_file(PREV_MATCH_DATA_PATH)

    current_run_unique_ids, generated_files, processed_countries = set(), set(), set()
    current_opportunities_cache, all_match_groups_by_id = {}, {}
    log_output_root = ""

    last_updated_times = {}

    if CHECKING_MODE == "ev" and ev_calculator.OVERPRICE_SOURCE_LOGGING:
        ev_settings_for_path = json.load(open(os.path.join("settings", SPORT, "ev.json"), encoding="utf-8"))
        base_output_dir = ev_settings_for_path["OUTPUT_DIRECTORY"]
        log_output_root = os.path.join(base_output_dir, "ev_source_log")

    all_countries = get_all_canonical_countries(SOURCE_DIRECTORIES)
    for country_name in sorted(all_countries):
        if country_name in processed_countries: continue
        processed_countries.add(country_name)
        paths = get_country_file_paths(country_name, SOURCE_DIRECTORIES)
        if len(paths) < 2: continue
        matches_by_source = {}
        for src_name, file_list in paths.items():
            entries = []
            latest_update_for_source = None
            for path in file_list:
                new_matches, updated_at = load_matches(path)
                entries.extend(new_matches)
                if updated_at:
                    if not latest_update_for_source or updated_at > latest_update_for_source:
                        latest_update_for_source = updated_at

            if latest_update_for_source:
                last_updated_times[src_name] = latest_update_for_source

            for m in entries: m["source"] = src_name
            matches_by_source[src_name] = entries

        matching_groups = find_all_matching_matches(matches_by_source)
        total_matching_groups += len(matching_groups)

        for group in matching_groups:
            # --- Cache the full group data if needed for the next run ---
            # This is used by both EV and the new Arb 'appearance' method.
            if group:
                group_id = group[0].get("matching_group_id")
                if group_id:
                    all_match_groups_by_id[group_id] = group

            group_object = analyzer_function(
                grp=group,
                prev_data=previous_match_data,
                act_data=activity_data
            )
            if group_object:
                confirmed_opportunities = []
                opps_list = group_object.get('opportunities', [])
                now_utc = datetime.now(ZoneInfo("Etc/GMT-1"))

                for opp in opps_list:
                    unique_id = opp.get("unique_id")
                    if not unique_id: continue

                    is_confirmed = True
                    birth_time_dt = now_utc

                    if SHOW_ONLY_CONFIRMED:
                        is_confirmed = False
                        birth_time_str = None

                        # 1. Check if we are already tracking this opportunity
                        if unique_id in unconfirmed_opps_cache:
                            # It's an existing unconfirmed opportunity. Use its recorded birth time.
                            birth_time_str = unconfirmed_opps_cache[unique_id]["birth_time"]
                        elif unique_id in activity_data and "first_seen" in activity_data[unique_id]:
                            # It's a previously confirmed opportunity that we are tracking.
                            birth_time_str = activity_data[unique_id]["first_seen"]

                        # 2. Determine the birth_time datetime object
                        if birth_time_str:
                            # Load the existing timestamp
                            birth_time_dt = datetime.fromisoformat(birth_time_str)
                            if birth_time_dt.tzinfo is None:
                                birth_time_dt = birth_time_dt.replace(tzinfo=ZoneInfo("Etc/GMT-1"))
                        else:
                            # It's a brand new, never-before-seen opportunity.
                            # Set birth_time to the latest update timestamp from the involved sources.

                            # Get involved sources based on the mode
                            if CHECKING_MODE == "arb":
                                involved_sources = opp.get("arbitrage_sources", "").split(", ")
                            else:  # ev mode
                                involved_sources = opp.get("ev_sources", [])

                            source_timestamps = []
                            for match_in_group in group:
                                if match_in_group.get(
                                        'source') in involved_sources and 'file_last_updated' in match_in_group:
                                    source_timestamps.append(match_in_group['file_last_updated'])

                            if source_timestamps:
                                birth_time_dt = max(source_timestamps)
                            else:
                                # Fallback to now_utc ONLY if source timestamps are missing (unlikely)
                                birth_time_dt = now_utc

                            birth_time_str = birth_time_dt.isoformat()

                        # 3. Check if all sources have been updated since the opportunity was born
                        all_sources_updated = True

                        # Get involved sources based on the mode
                        if CHECKING_MODE == "arb":
                            involved_sources = opp.get("arbitrage_sources", "").split(", ")
                        else:  # ev mode
                            involved_sources = opp.get("ev_sources", [])

                        for src in involved_sources:
                            # To be confirmed, a source's last update must be >= the opportunity's birth time.
                            # So, if a source's update is < birth time, it's not confirmed yet.
                            if src not in last_updated_times or last_updated_times[src] < birth_time_dt:
                                all_sources_updated = False
                                break

                        if all_sources_updated:
                            is_confirmed = True
                        else:
                            # Still waiting for confirmation, save it to the cache for the next run
                            current_unconfirmed_opps[unique_id] = {
                                "birth_time": birth_time_str,
                                "opportunity_data": opp
                            }

                            # Ensure the source info is preserved in activity_data even for unconfirmed opportunities
                            if CHECKING_MODE == "arb" and 'misvalue_source' in opp:
                                activity_data.setdefault(unique_id, {})['misvalue_source'] = opp['misvalue_source']
                            elif CHECKING_MODE == "ev" and 'overprice_source' in opp:
                                activity_data.setdefault(unique_id, {})['overprice_source'] = opp['overprice_source']

                    if is_confirmed:
                        # This check was slightly misplaced, it should only increment once per group
                        # total_opp_groups += 1
                        country_key = group_object['country']

                        opp.update({
                            'group_id': group_object.get('group_id'),
                            'home_team': group_object.get('home_team'),
                            'away_team': group_object.get('away_team')
                        })

                        current_run_unique_ids.add(unique_id)

                        if unique_id in activity_data and "first_seen" in activity_data[unique_id]:
                            # This is an existing, tracked opportunity.
                            first_seen_str = activity_data[unique_id]["first_seen"]
                            first_seen_dt = datetime.fromisoformat(first_seen_str)
                            if first_seen_dt.tzinfo is None:
                                first_seen_dt = first_seen_dt.replace(tzinfo=ZoneInfo("Etc/GMT-1"))
                        else:
                            # This is a brand new opportunity.
                            first_seen_dt = birth_time_dt
                            # The calculator may have already added info to a placeholder dict.
                            # We must UPDATE the dict, not replace it.
                            activity_data.setdefault(unique_id, {}).update({
                                "first_seen": first_seen_dt.isoformat()
                            })

                        opp["activity_duration"] = format_duration((now_utc - first_seen_dt).total_seconds())

                        confirmed_opportunities.append(opp)

                        if CHECKING_MODE == "ev":
                            all_margins.append(opp["overprice"] * 100)
                        else:
                            all_margins.append((1.0 - opp["arbitrage_percentage"]) * 100)

                if confirmed_opportunities:
                    total_opp_groups += 1  # Increment group count here, once per group with confirmed opps
                    group_object['opportunities'] = confirmed_opportunities
                    total_opps += len(confirmed_opportunities)
                    country_key = group_object['country']
                    results_by_country.setdefault(country_key, []).append(group_object)

    # --- Post-processing after all files are checked ---

    if SHOW_ONLY_CONFIRMED:
        save_json_to_file(UNCONFIRMED_OPPS_PATH, current_unconfirmed_opps)
        unconfirmed_count = len(current_unconfirmed_opps)
        if unconfirmed_count > 0:
            print(f"\n{unconfirmed_count} opportunities are waiting for source confirmation.")


    # EV Lifecycle management (remains unchanged, it has its own logic)
    if CHECKING_MODE == "ev" and ev_calculator.OVERPRICE_SOURCE_LOGGING:
        manage_ev_lifecycle(
            current_opportunities_cache=current_opportunities_cache,
            all_match_groups_by_id=all_match_groups_by_id,
            output_dir=OUTPUT_DIR,
            log_output_root=log_output_root,
            activity_data=activity_data
        )

    # ADD THIS HELPER FUNCTION RIGHT BEFORE THE LOOP
    def json_serializer(obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    # Write results to files
    for country, list_of_groups in sorted(results_by_country.items()):
        if list_of_groups:
            filename = f"{country}.json"
            out_path = os.path.join(OUTPUT_DIR, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(list_of_groups, f, ensure_ascii=False, indent=2, default=json_serializer)
            generated_files.add(filename)

    # --- Cache previous match data if needed by either mode ---
    # EV mode needs it for its appearance investigation.
    # Arb mode needs it if the detection method is 'appearance'.
    should_cache_for_ev = (CHECKING_MODE == "ev" and ev_calculator.APPEARANCE_INVESTIGATION)
    should_cache_for_arb = (CHECKING_MODE == "arb" and arb_calculator.MISVALUE_DETECTION_METHOD == "appearance")

    if should_cache_for_ev or should_cache_for_arb:
        os.makedirs(cache_dir, exist_ok=True)
        save_json_to_file(PREV_MATCH_DATA_PATH, all_match_groups_by_id)
        if should_cache_for_arb:
            print("[ARB_CACHE] Saved match data for next cycle's 'appearance' investigation.")

    dedupe_all_country_files(OUTPUT_DIR)
    cleanup_old_files(OUTPUT_DIR, generated_files)

    # Prune activity tracker
    unconfirmed_unique_ids = set(current_unconfirmed_opps.keys()) if SHOW_ONLY_CONFIRMED else set()

    if CHECKING_MODE == "arb":
        # For arbitrage mode, an opportunity is only tracked if it is currently
        # active (in current_run_unique_ids) or waiting for confirmation.
        # Once it disappears, it is removed from the tracker.
        pruned_activity_data = {
            uid: data
            for uid, data in activity_data.items()
            if (uid in current_run_unique_ids or
                uid in unconfirmed_unique_ids)
        }
    else:  # This covers "ev" mode
        # MODIFIED: Inactive EV opportunities are now removed from the tracker.
        # The necessary state for disappearance investigations will be passed
        # to the lifecycle manager and stored in the purgatory cache instead.
        pruned_activity_data = {
            uid: data
            for uid, data in activity_data.items()
            if (uid in current_run_unique_ids or
                uid in unconfirmed_unique_ids)
        }

    save_activity_data(ACTIVITY_TRACKER_PATH, pruned_activity_data)
    print(f"\nUpdated activity tracker. Tracking {len(pruned_activity_data)} active opportunities.")
    print(f"Total matching groups: {total_matching_groups}")

    if CHECKING_MODE == "ev":
        print(f"Total +EV matches: {total_opp_groups}")
        print(f"Total +EV opportunities: {total_opps}")
        if all_margins:
            print(f"Overprice: Avg {sum(all_margins) / len(all_margins):.2f}%, Max {max(all_margins):.2f}%")
    else:  # arb
        print(f"Total arbitrage groups: {total_opp_groups}")
        print(f"Total arbitrage opportunities: {total_opps}")
        if all_margins:
            print(f"Profit margin: Avg {sum(all_margins) / len(all_margins):.2f}%, Max {max(all_margins):.2f}%")



# ----- Entry Point -----
if LOOP:
    while True:
        if __name__ == "__main__":
            process_files_optimal()
        print(f"\n--- Cycle complete. Waiting for {DELAY} seconds. ---\n")
        time.sleep(DELAY)
else:
    if __name__ == "__main__":
        process_files_optimal()
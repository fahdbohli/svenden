# main.py

import os
import json
import shutil
import time
import argparse
from datetime import datetime

# ----- Choose default configuration ---------------------------------------
MODE = "prematch"       # "prematch" or "live"
SPORT = "football"      # e.g., "football", "basketball", "tennis" ...
CHECKING_MODE = "arb"   # "arb" for arbitrage or "ev" for expected value
LOOP = False            # choose the default loop state
DELAY = 1               # choose the default delay between chacking cycles
# --------------------------------------------------------------------

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
    help=f'Delay in seconds between each checking cycle if loop is activated (default: {DELAY})')

args = parser.parse_args()

# Override defaults with parsed arguments
MODE = args.mode
SPORT = args.sport
CHECKING_MODE = args.checking_mode
LOOP = args.loop
DELAY = args.delay
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
    with open(os.path.join("settings", SPORT, "markets.json"), encoding="utf-8") as mfile:
        markets_root = json.load(mfile)
        arb_calculator.MARKET_SETS = markets_root["market_sets"]
    arb_calculator.build_market_categories()
    URL_BUILDER_PATH = os.path.join("settings", "url_builder.json")
    with open(URL_BUILDER_PATH, encoding="utf-8") as url_file:
        url_conf = json.load(url_file)
        arb_calculator.URL_TEMPLATES = url_conf.get("url_templates", {})
        arb_calculator.SPORT_NAME = SPORT
        arb_calculator.MODE_NAME = MODE
    analyzer_function = analyze_optimal_arbitrage

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
    URL_BUILDER_PATH = os.path.join("settings", "url_builder.json")
    with open(URL_BUILDER_PATH, encoding="utf-8") as url_file:
        url_conf = json.load(url_file)
        ev_calculator.URL_TEMPLATES = url_conf.get("url_templates", {})
        ev_calculator.SPORT_NAME = SPORT
        ev_calculator.MODE_NAME = MODE
    analyzer_function = analyze_ev_opportunities

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

    total_matching_groups, total_opp_groups, total_opps = 0, 0, 0
    all_margins, results_by_country = [], {}
    ACTIVITY_TRACKER_PATH = os.path.join(OUTPUT_DIR, "activity_tracker.json")
    activity_data = load_activity_data(ACTIVITY_TRACKER_PATH)

    # Cache for previous match data for appearance investigation
    cache_dir = os.path.join(OUTPUT_DIR, "_cache")
    PREV_MATCH_DATA_PATH = os.path.join(cache_dir, "previous_match_data_cache.json")
    previous_match_data = load_json_from_file(PREV_MATCH_DATA_PATH)

    current_run_unique_ids, generated_files, processed_countries = set(), set(), set()
    current_opportunities_cache, all_match_groups_by_id = {}, {}
    log_output_root = ""

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
            for path in file_list: entries.extend(load_matches(path))
            for m in entries: m["source"] = src_name
            matches_by_source[src_name] = entries
        matching_groups = find_all_matching_matches(matches_by_source)
        total_matching_groups += len(matching_groups)

        for group in matching_groups:
            if CHECKING_MODE == "ev" and ev_calculator.OVERPRICE_SOURCE_LOGGING:
                if group:
                    group_id = group[0].get("matching_group_id")
                    if group_id:
                        all_match_groups_by_id[group_id] = group

            group_object = analyzer_function(group)
            if group_object:
                total_opp_groups += 1
                country_key = group_object['country']
                opps_list = group_object.get('opportunities', [])
                now = datetime.now()
                for opp in opps_list:
                    unique_id = opp.get("unique_id")
                    if not unique_id: continue
                    current_run_unique_ids.add(unique_id)

                    opp.update({
                        'group_id': group_object.get('group_id'),
                        'home_team': group_object.get('home_team'),
                        'away_team': group_object.get('away_team')
                    })

                    # Step 1: Handle duration and activity tracking for all opportunities (new and existing)
                    if unique_id in activity_data:
                        activity_entry = activity_data[unique_id]
                        if isinstance(activity_entry, str):  # Migrate old format
                            activity_entry = {"first_seen": activity_entry, "appearance_log": None}
                            activity_data[unique_id] = activity_entry

                        first_seen_dt = datetime.fromisoformat(activity_entry["first_seen"])
                        opp["activity_duration"] = format_duration((now - first_seen_dt).total_seconds())

                        # For existing opportunities, check if they have a stored appearance log
                        if activity_entry.get("appearance_log"):
                            opp["overprice_source"] = activity_entry["appearance_log"]["overprice_source"]

                    else:
                        # This is a NEW opportunity
                        opp["activity_duration"] = "0 seconds"
                        new_activity_entry = {"first_seen": now.isoformat(), "appearance_log": None}
                        activity_data[unique_id] = new_activity_entry

                        # Trigger Appearance Investigation ONLY for new opportunities
                        if CHECKING_MODE == "ev" and ev_calculator.APPEARANCE_INVESTIGATION:
                            group_id = group_object.get("group_id")
                            previous_group = previous_match_data.get(group_id)
                            if previous_group:
                                appearance_log = ev_calculator.analyze_ev_appearance(group, previous_group, opp)
                                if appearance_log:
                                    print(f"[EV_LOG] Appearance investigation succeeded for {unique_id}.")
                                    # Store the log for future runs
                                    activity_data[unique_id]["appearance_log"] = appearance_log
                                    # Inject the source for the current run
                                    opp["overprice_source"] = appearance_log["overprice_source"]
                                    # Write the log file immediately
                                    ev_calculator.write_appearance_log_immediately(
                                        appearance_log,
                                        log_output_root
                                    )

                    if CHECKING_MODE == "ev":
                        all_margins.append(opp["overprice"] * 100)
                        if ev_calculator.OVERPRICE_SOURCE_LOGGING:
                            current_opportunities_cache[unique_id] = opp
                    else:  # arb
                        all_margins.append((1.0 - opp["arbitrage_percentage"]) * 100)

                total_opps += len(opps_list)
                results_by_country.setdefault(country_key, []).append(group_object)

    if CHECKING_MODE == "ev" and ev_calculator.OVERPRICE_SOURCE_LOGGING:
        manage_ev_lifecycle(
            current_opportunities_cache=current_opportunities_cache,
            all_match_groups_by_id=all_match_groups_by_id,
            output_dir=OUTPUT_DIR,
            log_output_root=log_output_root,
            activity_data=activity_data  # Pass the new activity data
        )

    for country, list_of_groups in sorted(results_by_country.items()):
        if list_of_groups:
            filename = f"{country}.json"
            out_path = os.path.join(OUTPUT_DIR, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(list_of_groups, f, ensure_ascii=False, indent=2)
            generated_files.add(filename)

    # Save the match data for the next cycle's appearance investigation
    if CHECKING_MODE == "ev" and ev_calculator.APPEARANCE_INVESTIGATION:
        os.makedirs(cache_dir, exist_ok=True)
        save_json_to_file(PREV_MATCH_DATA_PATH, all_match_groups_by_id)

    dedupe_all_country_files(OUTPUT_DIR)
    cleanup_old_files(OUTPUT_DIR, generated_files)
    pruned_activity_data = {
        uid: data
        for uid, data in activity_data.items()
        # Keep it if it's still active **or** if it has an appearance_log
        if uid in current_run_unique_ids or (isinstance(data, dict) and data.get("appearance_log") is not None)
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
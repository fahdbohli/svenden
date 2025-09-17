# main.py

import os
import json
import shutil
import time
import argparse
from datetime import datetime

# ----- Choose default configuration ---------------------------------------
MODE = "prematch"       # "prematch" or "live"
SPORT = "football"      # e.g., "football", "basketball"
CHECKING_MODE = "ev"   # "arb" for arbitrage or "ev" for expected value
LOOP = True            # choose the default loop state
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
    save_activity_data
)
from matcher import find_all_matching_matches
from arb_calculator import analyze_optimal_arbitrage
from ev_calculator import analyze_ev_opportunities
from utils import dedupe_all_country_files, test_team_matching

# ----- Conditionally Load Mode-Specific Settings -----

if CHECKING_MODE == "arb":
    # Load settings for Arbitrage mode
    print("Running in Arbitrage (arb) mode.")
    OUTPUT_DIR = selected_settings["output_dir"]

    # Construct the new dynamic output directory path
    OUTPUT_DIR = os.path.join(OUTPUT_DIR,MODE, SPORT)

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

    # Get the EV_SOURCE from the settings first
    ev_source_name = ev_settings["EV_SOURCE"]
    output_directory = ev_settings["OUTPUT_DIRECTORY"]

    # Construct the new dynamic output directory path
    OUTPUT_DIR = os.path.join(output_directory, "ev_output", MODE, ev_source_name, SPORT)

    # The rest of the settings are loaded as before
    ev_calculator.METHOD = ev_settings["METHOD"]
    ev_calculator.SHARP_SOURCE = ev_settings["SHARP_SOURCE"]
    ev_calculator.SHARPING_GROUP = ev_settings["SHARPING_GROUP"]
    ev_calculator.EV_SOURCE = ev_source_name  # Use the variable we just created
    ev_calculator.ODDS_INTERVAL = ev_settings["ODDS_INTERVAL"]
    ev_calculator.MIN_OVERPRICE = ev_settings["MIN_OVERPRICE"]

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
    if total_seconds < 60:
        return f"{round(total_seconds)} seconds"
    minutes = round(total_seconds / 60)
    if minutes < 60:
        return f"{minutes} minute" if minutes == 1 else f"{minutes} minutes"
    hours = round(minutes / 60)
    return f"{hours} hour" if hours == 1 else f"{hours} hours"


# ----- Main Processing Function -----
def process_files_optimal():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output will be written to: {OUTPUT_DIR}")

    total_matching_groups = 0
    total_opp_groups = 0
    total_opps = 0
    all_margins = []  # Used for both arb % and EV overprice %

    ACTIVITY_TRACKER_PATH = os.path.join(OUTPUT_DIR, "activity_tracker.json")
    activity_data = load_activity_data(ACTIVITY_TRACKER_PATH)
    current_run_unique_ids = set()
    generated_files = set()
    results_by_country = {}
    processed_countries = set()

    all_countries = get_all_canonical_countries(SOURCE_DIRECTORIES)

    for country_name in sorted(all_countries):
        if country_name in processed_countries:
            continue
        processed_countries.add(country_name)

        paths = get_country_file_paths(country_name, SOURCE_DIRECTORIES)

        # In EV mode, we might need more than one source (e.g., sharp + EV source)
        min_sources = 2 if CHECKING_MODE == "ev" else 2
        if len(paths) < min_sources:
            continue

        matches_by_source = {}
        for src_name, file_list in paths.items():
            entries = []
            for path in file_list:
                entries.extend(load_matches(path))
            for m in entries:
                m["source"] = src_name
            matches_by_source[src_name] = entries

        matching_groups = find_all_matching_matches(matches_by_source)
        total_matching_groups += len(matching_groups)
        if matching_groups:
            print(f"Country {country_name}: Found {len(matching_groups)} matching groups across sources.")

        country_opp_count = 0
        for group in matching_groups:
            # Call the appropriate analyzer function based on CHECKING_MODE
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

                    if unique_id in activity_data:
                        first_seen_dt = datetime.fromisoformat(activity_data[unique_id])
                        duration_seconds = (now - first_seen_dt).total_seconds()
                        opp["activity_duration"] = format_duration(duration_seconds)
                    else:
                        opp["activity_duration"] = "0 seconds"
                        activity_data[unique_id] = now.isoformat()

                    # Store relevant metric (arb or EV) for summary
                    if CHECKING_MODE == "arb":
                        margin = (1.0 - opp["arbitrage_percentage"]) * 100
                        all_margins.append(margin)
                    elif CHECKING_MODE == "ev":
                        margin = opp["overprice"] * 100
                        all_margins.append(margin)

                country_opp_count += len(opps_list)
                total_opps += len(opps_list)
                results_by_country.setdefault(country_key, []).append(group_object)

        if country_opp_count > 0:
            opp_type = "arbitrage opportunities" if CHECKING_MODE == "arb" else "+EV opportunities"
            print(
                f"Country {country_name}: Found {country_opp_count} {opp_type} in {len(results_by_country.get(country_name, []))} groups.")

    # Write out JSON results by country
    for country, list_of_groups in sorted(results_by_country.items()):
        if list_of_groups:
            filename = f"{country}.json"
            out_path = os.path.join(OUTPUT_DIR, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(list_of_groups, f, ensure_ascii=False, indent=2)
            generated_files.add(filename)

    # Common cleanup and reporting
    dedupe_all_country_files(OUTPUT_DIR)
    cleanup_old_files(OUTPUT_DIR, generated_files)

    pruned_activity_data = {
        uid: ts for uid, ts in activity_data.items() if uid in current_run_unique_ids
    }
    save_activity_data(ACTIVITY_TRACKER_PATH, pruned_activity_data)
    print(f"\nUpdated activity tracker. Tracking {len(pruned_activity_data)} active opportunities.")

    print(f"Total matching groups: {total_matching_groups}")

    # Mode-specific summary
    if CHECKING_MODE == "arb":
        print(f"Total arbitrage groups: {total_opp_groups}")
        print(f"Total arbitrage opportunities: {total_opps}")
        if all_margins:
            avg_margin = sum(all_margins) / len(all_margins)
            max_margin = max(all_margins)
            print(f"Profit margin: Avg {avg_margin:.2f}%, Max {max_margin:.2f}%")
        else:
            print("Profit margin: No arbitrage opportunities found.")

    elif CHECKING_MODE == "ev":
        print(f"Total +EV matches: {total_opp_groups}")
        print(f"Total +EV opportunities: {total_opps}")
        if all_margins:
            avg_overprice = sum(all_margins) / len(all_margins)
            max_overprice = max(all_margins)
            print(f"Overprice: Avg {avg_overprice:.2f}%, Max {max_overprice:.2f}%")
        else:
            print("Overprice: No +EV opportunities found.")


# ----- Entry Point -----
if LOOP:
    while True:
        if __name__ == "__main__":
            # test_team_matching() # Optional: disable for faster loops
            process_files_optimal()
        time.sleep(DELAY)
else:
    if __name__ == "__main__":
        # test_team_matching()
        process_files_optimal()
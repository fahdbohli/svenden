# main.py

import os
import json
import shutil
from datetime import datetime

# ----- Choose Sport and Mode -----
MODE = "prematch"  # "prematch" or "live"
SPORT = "football"  # e.g., "football", "basketball"

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

# ----- Load Settings for This Sport/Mode -----
SETTINGS_PATH = os.path.join("settings", SPORT, "settings.json")
with open(SETTINGS_PATH, encoding="utf-8") as sf:
    all_settings = json.load(sf)

if SPORT not in all_settings:
    raise ValueError(f"Sport '{SPORT}' not found in settings.")
if MODE not in all_settings[SPORT]:
    raise ValueError(f"Mode '{MODE}' not found under sport '{SPORT}'.")

selected_settings = all_settings[SPORT][MODE]
OUTPUT_DIR = selected_settings["output_dir"]
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

# Load the new grouped structure for important terms
matcher.IMPORTANT_TERM_GROUPS = team_conf["important_terms"]
matcher.COMMON_TEAM_WORDS = set(team_conf["common_team_words"])
matcher.LOCATION_IDENTIFIERS = set(team_conf["location_identifiers"])
matcher.TEAM_SYNONYMS = [set(group) for group in team_conf["team_synonyms"]]

# ----- Load Market Sets from markets.json and assign into arb_calculator -----
import arb_calculator

with open(os.path.join("settings", SPORT, "markets.json"), encoding="utf-8") as mfile:
    markets_root = json.load(mfile)
    arb_calculator.MARKET_SETS = markets_root["market_sets"]

# ----- START OF MODIFICATION -----
# Load URL Builder Config into arb_calculator
URL_BUILDER_PATH = os.path.join("settings", "url_builder.json")
with open(URL_BUILDER_PATH, encoding="utf-8") as url_file:
    url_conf = json.load(url_file)
    arb_calculator.URL_TEMPLATES = url_conf.get("url_templates", {})
    # Pass SPORT and MODE to arb_calculator for template processing
    arb_calculator.SPORT_NAME = SPORT
    arb_calculator.MODE_NAME = MODE
# ----- END OF MODIFICATION -----

# ----- Import Other Modules -----
from file_utils import (
    get_all_canonical_countries,
    get_country_file_paths,
    load_matches,
    cleanup_old_files  # Import the new cleanup function
)
from matcher import (
    canonical,
    normalize_team_name,
    simplify_team_name,
    extract_significant_words,
    check_team_synonyms,
    teams_match,
    parse_date,
    find_all_matching_matches,
)
from arb_calculator import analyze_optimal_arbitrage
from utils import dedupe_all_country_files, test_team_matching


# ----- Main Processing Function -----
def process_files_optimal():
    # Instead of deleting the directory, just ensure it exists.
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output will be written to: {OUTPUT_DIR}")

    total_matching_groups = 0
    total_arb = 0
    all_margins = []

    # This set will keep track of all files generated in this run.
    generated_files = set()

    arbitrage_by_country = {}
    processed_countries = set()

    # 1) Find all canonical country names across sources
    all_countries = get_all_canonical_countries(SOURCE_DIRECTORIES)

    # 2) For each country, gather file paths and load matches
    for country_name in sorted(all_countries):
        if country_name in processed_countries:
            continue
        processed_countries.add(country_name)

        # 2a) Find JSON paths for this country in each source
        paths = get_country_file_paths(country_name, SOURCE_DIRECTORIES)
        if len(paths) < 2:
            continue

        # 2b) Load matches from each source
        matches_by_source = {}
        for src_name, file_list in paths.items():
            entries = []
            for path in file_list:
                entries.extend(load_matches(path))
            for m in entries:
                m["source"] = src_name
            matches_by_source[src_name] = entries

        # 2c) Group fixtures across sources
        matching_groups = find_all_matching_matches(matches_by_source)
        total_matching_groups += len(matching_groups)
        print(f"Country {country_name}: Found {len(matching_groups)} matching groups across sources.")

        # 2d) For each group, find optimal arbitrage
        country_arb_count = 0
        for group in matching_groups:
            opportunities = analyze_optimal_arbitrage(group)
            if opportunities:
                country_arb_count += len(opportunities)
                total_arb += len(opportunities)
                for opp in opportunities:
                    arb_decimal = opp["arbitrage_percentage"]
                    margin_pct = (1.0 - arb_decimal) * 100
                    all_margins.append(margin_pct)

                    # Get the country from the opportunity object itself. This is the fix.
                    country_key = opp['match_info']['country']
                    # Add this single opportunity to the list for that specific country key.
                    arbitrage_by_country.setdefault(country_key, []).append(opp)

        if country_arb_count > 0:
            print(f"Country {country_name}: Found {country_arb_count} arbitrage opportunities.")

    # 3) Write out JSON results by country (overwriting existing files)
    for country, opps in sorted(arbitrage_by_country.items()):
        if opps:
            filename = f"{country}.json"
            out_path = os.path.join(OUTPUT_DIR, filename)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(opps, f, ensure_ascii=False, indent=2)
            # Add the filename to our set of generated files for this run
            generated_files.add(filename)

    # 4) Deduplicate all country files
    removed_total = dedupe_all_country_files(OUTPUT_DIR)
    total_arb -= removed_total

    # 5) Clean up old files that were not generated in this run
    cleanup_old_files(OUTPUT_DIR, generated_files)

    print(f"Total matching groups: {total_matching_groups}")
    print(f"Total arbitrage opportunities: {total_arb}")
    if all_margins:
        avg_margin = sum(all_margins) / len(all_margins)
        max_margin = max(all_margins)
        print(f"Profit margin: Avg {avg_margin:.2f}%, Max {max_margin:.2f}%")
    else:
        print("Profit margin: No arbitrage opportunities found.")


# ----- Entry Point -----
if __name__ == "__main__":
    test_team_matching()
    process_files_optimal()
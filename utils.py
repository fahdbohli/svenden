# utils.py

import os
import json
from collections import defaultdict

from matcher import (
    normalize_team_name,
    simplify_team_name,
    extract_significant_words,
    check_team_synonyms,
    teams_match
)


def dedupe_country_file(country_fn: str) -> int:
    """
    Reads a country file (list of group objects), flattens all opportunities,
    removes duplicates based on the opportunity type (arb or ev), rebuilds
    the grouped structure, and overwrites the file.
    Returns the number of removed opportunities.
    """
    try:
        with open(country_fn, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0

    if not isinstance(data, list):
        print(f"[ERROR] {os.path.basename(country_fn)} has unexpected format. Skipping deduplication.")
        return 0

    # --- 1. Flatten all opportunities and tag with group_id ---
    all_opps_tagged = []
    for group_obj in data:
        group_id = group_obj.get("group_id")
        if not group_id: continue
        for opp in group_obj.get("opportunities", []):
            opp['temp_group_id'] = group_id
            all_opps_tagged.append(opp)

    if not all_opps_tagged:
        return 0

    # --- 2. Determine mode and keys from the data itself ---
    first_opp = all_opps_tagged[0]
    if 'arbitrage_percentage' in first_opp:
        mode = 'arb'
        # Keys that define a unique arbitrage opportunity
        dedupe_keys = ('arbitrage_percentage', 'complementary_set', 'arbitrage_sources')
        # Key to sort by (lower is better)
        sort_key = 'arbitrage_percentage'
        sort_reverse = False
    elif 'overprice' in first_opp:
        mode = 'ev'
        # Keys that define a unique EV opportunity
        dedupe_keys = ('source', 'odd_name', 'overpriced_odd_value', 'fair_odd_value')
        # Key to sort by (higher is better)
        sort_key = 'overprice'
        sort_reverse = True
    else:
        # Unknown format, clean up temp tags and exit
        for opp in all_opps_tagged:
            opp.pop('temp_group_id', None)
        print(f"[WARN] {os.path.basename(country_fn)}: Could not determine opportunity type. Skipping.")
        return 0

    # --- 3. Perform efficient deduplication using a set of signatures ---
    unique_opps_flat = []
    seen_signatures = set()
    for opp in all_opps_tagged:
        # Create a unique signature from the relevant keys
        signature = tuple(opp.get(key) for key in dedupe_keys)
        if signature not in seen_signatures:
            unique_opps_flat.append(opp)
            seen_signatures.add(signature)

    removed = len(all_opps_tagged) - len(unique_opps_flat)

    if removed > 0:
        # --- 4. Rebuild the original grouped structure ---
        rebuilt_groups = {}
        for group_obj in data:
            gid = group_obj.get("group_id")
            if gid:
                rebuilt_groups[gid] = group_obj.copy()
                rebuilt_groups[gid]['opportunities'] = []

        for unique_opp in unique_opps_flat:
            gid = unique_opp.pop('temp_group_id')
            if gid in rebuilt_groups:
                rebuilt_groups[gid]['opportunities'].append(unique_opp)

        final_list_to_save = [group for group in rebuilt_groups.values() if group['opportunities']]

        # Sort opportunities within each group using the correct key and order
        for group in final_list_to_save:
            group['opportunities'].sort(key=lambda o: o[sort_key], reverse=sort_reverse)

        with open(country_fn, "w", encoding="utf-8") as f:
            json.dump(final_list_to_save, f, indent=2, ensure_ascii=False)
        print(f"[INFO] {os.path.basename(country_fn)}: removed {removed} duplicate opportunities")
    else:
        # If nothing was removed, we still need to remove the temp tag
        for opp in all_opps_tagged:
            opp.pop('temp_group_id', None)

    return removed


def dedupe_all_country_files(json_dir: str) -> int:
    """
    Run dedupe_country_file on every .json file in json_dir.
    Return the total number of removed entries across all files.
    """
    total_removed = 0
    if not os.path.isdir(json_dir):
        print(f"[WARN] Directory not found: {json_dir}. Skipping deduplication.")
        return 0

    for fn in os.listdir(json_dir):
        if not fn.lower().endswith(".json"):
            continue
        full_path = os.path.join(json_dir, fn)
        total_removed += dedupe_country_file(full_path)
    return total_removed


def test_team_matching():
    """
    Test function to validate team name matching logic.
    """
    # This function remains unchanged.
    test_cases = [
        ("Al Hilal SFC", "Al Hilal Riyadh", True),
        ("Al-Shabab FC (SA)", "Al Shabab Riyadh", True),
        ("Manchester United", "Man Utd", True),
        ("Real Madrid", "Real Madrid CF", True),
        ("Paris Saint-Germain", "PSG", True),
        ("Inter Milan", "Internazionale", True),
        ("Bayern Munich", "FC Bayern München", True),
        ("Liverpool FC", "Liverpool", True),
        ("Barcelona", "FC Barcelona", True),
        ("Juventus", "Juventus Turin", True),
        ("AC Milan", "Milan", True),
        ("Chelsea FC", "Chelsea London", True),
        ("Al Nassr", "Al-Nassr FC", True),
        ("Al Ittihad", "Al-Ittihad Club", True),
        ("Yanbian Longding", "Yanbian Longding", True),
        ("Dalian Kun City", "Dalian K'un City", True),
        ("Czech Republic U23 (Women)", "Czech Republic (Youth) (Wom)", True),
        ("Sarmiento II", "CA Sarmiento Junin (Reserves)", True),
    ]

    passed = 0
    failed = []

    for team1, team2, expected in test_cases:
        result = teams_match(team1, team2)
        if result == expected:
            passed += 1
        else:
            failed.append((team1, team2, expected, result))

    print(f"Team matching tests: {passed}/{len(test_cases)} passed")

    if failed:
        print("Failed tests:")
        for team1, team2, expected, result in failed:
            print(f"  - '{team1}' vs '{team2}': Expected {expected}, got {result}")
            print(f"    - Normalized: '{normalize_team_name(team1)}' vs '{normalize_team_name(team2)}'")
            print(f"    - Simplified: '{simplify_team_name(team1)}' vs '{simplify_team_name(team2)}'")
            print(
                f"    - Words: {extract_significant_words(team1)} vs {extract_significant_words(team2)}"
            )
            print(f"    - Synonyms: {check_team_synonyms(team1, team2)}")
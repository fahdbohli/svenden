# utils.py

import os
import json

from matcher import (
    normalize_team_name,
    simplify_team_name,
    extract_significant_words,
    check_team_synonyms,
    teams_match
)


def dedupe_country_file(country_fn: str) -> int:
    """
    Read country_fn, drop duplicates based on a multi-step, fuzzy-matching
    logic, overwrite country_fn, and return how many entries were removed.
    """
    try:
        with open(country_fn, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0  # Cannot process empty or non-existent files

    opps = data if isinstance(data, list) else list(data.values())
    if not opps:
        return 0

    unique_opps = []

    for current_opp in opps:
        is_duplicate = False
        # Compare the current opportunity against all the unique ones we've found so far
        for unique_opp in unique_opps:

            # --- STEP 1: Check if the core non-team keys are identical ---
            # Use .get() for safety in case a key is missing
            m_info_current = current_opp.get("match_info", {})
            m_info_unique = unique_opp.get("match_info", {})

            keys_are_identical = (
                    current_opp.get("arbitrage_percentage") == unique_opp.get("arbitrage_percentage") and
                    current_opp.get("complementary_set") == unique_opp.get("complementary_set") and
                    m_info_current.get("date") == m_info_unique.get("date") and
                    m_info_current.get("time") == m_info_unique.get("time")
            )

            # If the base keys don't match, this cannot be a duplicate. Move to the next unique_opp.
            if not keys_are_identical:
                continue

            # --- If we reach here, the base keys are the same. Now, check teams. ---
            h1, a1 = m_info_current.get("home_team", ""), m_info_current.get("away_team", "")
            h2, a2 = m_info_unique.get("home_team", ""), m_info_unique.get("away_team", "")

            # --- STEP 2: Check for an exact team name match ---
            # This covers cases where at least one team name is identical.
            exact_team_match = (h1 == h2 or a1 == a2)
            if exact_team_match:
                is_duplicate = True
                break  # Found a duplicate, no need to check further

            # --- STEP 3: If no exact match, perform a fuzzy team name check ---
            # This is the most powerful check, using your teams_match function.
            # It correctly handles both normal and swapped team orders.

            # Check for A vs B == A' vs B'
            fuzzy_normal_match = teams_match(h1, h2) and teams_match(a1, a2)

            # Check for A vs B == B' vs A' (swapped teams)
            fuzzy_swapped_match = teams_match(h1, a2) and teams_match(a1, h2)

            if fuzzy_normal_match or fuzzy_swapped_match:
                is_duplicate = True
                break  # Found a duplicate, no need to check further

        # After comparing against all unique_opps, if it's not a duplicate, add it.
        if not is_duplicate:
            unique_opps.append(current_opp)

    removed = len(opps) - len(unique_opps)
    if removed > 0:
        with open(country_fn, "w", encoding="utf-8") as f:
            json.dump(unique_opps, f, indent=2, ensure_ascii=False)
        print(f"[INFO] {os.path.basename(country_fn)}: removed {removed} duplicates")

    return removed


def dedupe_all_country_files(json_dir: str) -> int:
    """
    Run dedupe_country_file on every .json file in json_dir.
    Return the total number of removed entries across all files.
    """
    total_removed = 0
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
    test_cases = [
        ("Al Hilal SFC", "Al Hilal Riyadh", True),
        ("Al-Shabab FC (SA)", "Al Shabab Riyadh", True),
        ("Manchester United", "Man Utd", True),
        ("Real Madrid", "Real Madrid CF", True),
        ("Paris Saint-Germain", "PSG", True),  # Add to synonyms if needed
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

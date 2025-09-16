# matcher.py

import re
import unicodedata
import difflib
from typing import Dict, List, Set, Any, Optional

# --- Configuration Placeholders (Filled in for the test below) ---
IMPORTANT_TERM_GROUPS: List[List[str]] = []
COMMON_TEAM_WORDS: Set[str] = set()
LOCATION_IDENTIFIERS: Set[str] = set()


# --- Helper Functions (using the final correct versions) ---

def remove_accents(text: str) -> str:
    if not text: return ""
    return "".join(c for c in unicodedata.normalize("NFD", text) if not unicodedata.combining(c))


def normalize_team_name(name: str) -> str:
    if not name: return ""
    n = remove_accents(name.lower())
    n = re.sub(r"\([^)]*\)", "", n)
    n = re.sub(r"[^\w\s]", " ", n)
    n = re.sub(r"\s+", " ", n)
    return n.strip()


def simplify_team_name(name: str) -> str:
    if not name:
        return ""
    n = normalize_team_name(name)
    roman_numeral_set = {
        "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
        "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX", "XX"
    }
    sorted_romans = sorted(list(roman_numeral_set), key=len, reverse=True)
    roman_pattern = re.compile(r'\b(' + '|'.join(sorted_romans) + r')\b', re.IGNORECASE)
    n = roman_pattern.sub("", n)
    n = re.sub(r'\s+', ' ', n).strip()
    words = n.split()
    filtered_words = [
        w for w in words
        if w not in COMMON_TEAM_WORDS and w not in LOCATION_IDENTIFIERS
    ]
    result = " ".join(filtered_words)
    result = re.sub(r"(ienne|ien|aise|ais|oise|ois|ine|in|é)$", "", result)
    return result.strip()


def get_core_name(name: str) -> str:
    if not name: return ""
    simplified = simplify_team_name(name)
    all_important_terms = [term for group in IMPORTANT_TERM_GROUPS for term in group]
    core_name = simplified
    for term in all_important_terms:
        # We need to be careful with terms in parentheses
        # The logic in the main script checks the original string for these terms
        # but strips them for core name comparison. Here we simulate the stripping part.
        term_to_strip = term.lower().replace("(", "").replace(")", "")
        pattern = re.compile(r'\b' + re.escape(term_to_strip) + r'\b', flags=re.IGNORECASE)
        core_name = pattern.sub("", core_name)
    return re.sub(r"\s+", " ", core_name).strip()


# ===================================================================
# ---               DIAGNOSTIC TEST RUNNER                      ---
# ===================================================================
if __name__ == "__main__":
    print("--- Running Diagnostic Test for Women's Teams ---")

    # --- Setup a minimal environment ---
    # This simulates the data loaded from your JSON files for this specific case.
    COMMON_TEAM_WORDS = {"united", "city", "fc", "sc", "afc", "sfc", "ec", "aguia", "uniao", "sd",
    "club", "clube", "de", "sporting", "racing", "fotbol", "if", "fk",
    "team", "association", "sport", "academy", "society", "school", "atletico"}
    # This group is CRITICAL for this test case
    IMPORTANT_TERM_GROUPS = [
        ["Wom", "(W)", "(F)", "(Women)"]
    ]

    # The two matches in question
    h1, a1 = "Club Atletico Gimnasia y Esgrima", "Club Atletico Talleres Remedios de Escalada"
    h2, a2 = "Patronato", "Atletico Guemes"

    # Simulate the check for important terms from the main loop
    # This check happens on the *original* names
    text1 = (h1 + " " + a1).lower()
    text2 = (h2 + " " + a2).lower()
    mismatch = False
    for term_group in IMPORTANT_TERM_GROUPS:
        found1 = any(term.lower() in text1 for term in term_group)
        found2 = any(term.lower() in text2 for term in term_group)
        if found1 ^ found2:
            mismatch = True
            break

    print("\n1. IMPORTANT TERM CHECK (pre-computation)")
    print("-" * 50)
    print(f"  - Text 1 contains a women's term? {found1}")
    print(f"  - Text 2 contains a women's term? {found2}")
    print(f"  - Is there a mismatch? {'YES, will not proceed' if mismatch else 'NO, can proceed'}")
    if mismatch:
        exit()

    print("\n2. CORE NAME COMPUTATION")
    print("-" * 50)
    core_h1 = get_core_name(h1)
    core_a1 = get_core_name(a1)
    core_h2 = get_core_name(h2)
    core_a2 = get_core_name(a2)
    print(f"  - Match 1: '{h1}' vs '{a1}'")
    print(f"    -> Core: '{core_h1}' vs '{core_a1}'")
    print(f"  - Match 2: '{h2}' vs '{a2}'")
    print(f"    -> Core: '{core_h2}' vs '{core_a2}'")

    print("\n3. FINAL COMPARISON & SCORES")
    print("-" * 50)
    home_score = difflib.SequenceMatcher(None, core_h1, core_h2).ratio()
    away_score = difflib.SequenceMatcher(None, core_a1, core_a2).ratio()
    print(f"  - Home Score ('{core_h1}' vs '{core_h2}'): {home_score:.4f}")
    print(f"  - Away Score ('{core_a1}' vs '{core_a2}'): {away_score:.4f}")

    print("\n4. THRESHOLD ANALYSIS (Simulating Prematch Mode)")
    print("-" * 50)
    strong_thresholds = [0.7, 0.9, 0.45, 1.0]
    moderate_thresholds = [0.35, 0.2, 0.45, 0.1]
    passed = False

    gatekeeper_threshold = 0.1
    print(
        f"  - Gatekeeper check: min({home_score:.2f}, {away_score:.2f}) >= {gatekeeper_threshold}? {'PASS' if min(home_score, away_score) >= gatekeeper_threshold else 'FAIL'}")

    if min(home_score, away_score) >= gatekeeper_threshold:
        for s_thresh, m_thresh in zip(strong_thresholds, moderate_thresholds):
            print(f"\n  - Testing against (Strong={s_thresh}, Moderate={m_thresh})")
            # Condition 1: Home is Strong, Away is Moderate
            cond1 = (home_score >= s_thresh and away_score >= m_thresh)
            print(f"    - Home Strong? ({home_score:.2f} >= {s_thresh}) -> {home_score >= s_thresh}")
            print(f"    - Away Moderate? ({away_score:.2f} >= {m_thresh}) -> {away_score >= m_thresh}")
            print(f"    --> Result: {'PASS' if cond1 else 'FAIL'}")

            # Condition 2: Away is Strong, Home is Moderate
            cond2 = (away_score >= s_thresh and home_score >= m_thresh)
            print(f"    - Away Strong? ({away_score:.2f} >= {s_thresh}) -> {away_score >= s_thresh}")
            print(f"    - Home Moderate? ({home_score:.2f} >= {m_thresh}) -> {home_score >= m_thresh}")
            print(f"    --> Result: {'PASS' if cond2 else 'FAIL'}")

            if cond1 or cond2:
                passed = True
                break  # Stop at the first passing threshold
    else:
        print("\n  - Failed Gatekeeper, skipping threshold checks.")

    print("\n--- FINAL OVERALL RESULT ---")
    print(f"This match is considered: {'A MATCH' if passed else 'NOT A MATCH'}")
    print("----------------------------")
import os
import json
import shutil
import re
import unicodedata
import difflib
from datetime import datetime
from itertools import combinations
from typing import Dict, List, Tuple, Set, Any, Optional

# --- load our filename‑synonyms config ---
CONFIG_PATH = "config.json"
with open(CONFIG_PATH, encoding="utf-8") as cfg:
    config = json.load(cfg)
# preserve the order you specified in config.json
SYN_GROUPS = config.get("synonyms", [])

# one‐time build of synonym → “canonical” (primary) name
SYN_PRIMARY = {
    syn: group[0]
    for group in SYN_GROUPS
    for syn in group
}

# Output directory for arbitrage opportunities
OUTPUT_DIR = "arbitrage_opportunities"

# Configure source directories - easily add more sources here
SOURCE_DIRECTORIES = [
    # Format: (source_name, directory_path)
    ("1xbet", r"C:\Users\HP\Desktop\Arbitrage Betting\Football\1xbet Scraper\scraped_prematch_matches"),
    ("Clubx2",r"C:\Users\HP\Desktop\Arbitrage Betting\Football\Clubx2 scraper\scraped_prematch_matches" ),
    ("Tounesbet", r"C:\Users\HP\Desktop\Arbitrage Betting\Football\TounesBet Scraper\scraped_prematch_matches"),
    ("Asbet", r"C:\Users\HP\Desktop\Arbitrage Betting\Football\AsBet Scraper\scraped_prematch_matches"),
    # ("Africa1x2", r"C:\Users\foura\PyCharmMiscProject\Projects\Arbitrage Betting\Africa1x2 scraper\scraped_matches"),
    # Add more sources as needed:
    # ("NewSource1", r"path\to\new\source1"),
    # ("NewSource2", r"path\to\new\source2"),
]

# Enhanced common words and location-specific terms to ignore
COMMON_TEAM_WORDS = {
    'united', 'city', 'fc', 'sc', 'afc', 'sfc', 'club', 'de', 'real', 'sporting', 'athletic', 'racing',
    'team', 'association', 'sport', 'academy', 'society', 'school',
}

LOCATION_IDENTIFIERS = {
    'riyadh', 'sa', 'saudi', 'arabia', 'jeddah', 'london', 'madrid', 'milan', 'rome', 'paris', 'moscow',
    'berlin', 'munich', 'manchester', 'liverpool', 'barcelona', 'lisbon', 'porto'
}

# Team name synonyms for teams that are commonly written differently
TEAM_SYNONYMS = [
    {"psg", "paris saint germain", "paris-saint-germain", "paris saint-germain"},
    {"al hilal", "alhilal", "al-hilal"},
    {"al shabab", "alshabab", "al-shabab"},
    {"al ahli", "alahli", "al-ahli"},
    {"al nassr", "alnassr", "al-nassr"},
    {"al ittihad", "alittihad", "al-ittihad"},
    {"dynamo", "dinamo"},
    {"olympique", "olympic"},
    {"real", "royal"},
    {"inter", "internazionale"},
    {"atletico", "athletic", "atlético"},
    {"manchester utd", "manchester united", "man utd", "man united"},
    {"manchester city", "man city"},
    {"dalian kun", "dalian kun city", "dalian k'un", "dalian k'un city"},
]

FUZZY_THRESHOLD = 0.5
FUZZY_THRESHOLD_ARABIC = 0.55  # Lower threshold for Arabic team names which have more variations

# how many minutes of clock‐error we’ll tolerate (0 means exact‐time only)
TIME_DIFF_TOLERANCE = 15

# --- Helpers ---
def reset_output():
    """Reset the output directory"""
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def parse_date(date_str):
    """Parse date string to datetime object"""
    if not date_str:
        return None

    try:
        d, m, y = date_str.strip().split('/')
        return datetime(int(y), int(m), int(d)).date()
    except:
        try:
            # Try alternative format
            formats = ["%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%d.%m.%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt).date()
                except:
                    continue
        except:
            return None


def remove_accents(text):
    """Remove accents from text"""
    if not text:
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text)
                   if not unicodedata.combining(c))


def load_matches(filename):
    """Load matches from a JSON file"""
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    matches = []
    if isinstance(data, dict):
        matches = data.get("matches", [data])
        # Extract country name from the filename
        base_filename = os.path.basename(filename)
        country_name = os.path.splitext(base_filename)[0]

        # Store country name in each match
        for match in matches:
            match["country"] = country_name

    elif isinstance(data, list):
        for element in data:
            if isinstance(element, dict) and "matches" in element:
                for match in element["matches"]:
                    match["country"] = os.path.splitext(os.path.basename(filename))[0]
                matches.extend(element["matches"])
            else:
                element["country"] = os.path.splitext(os.path.basename(filename))[0]
                matches.append(element)
    return matches


def fuzzy_match(a, b, threshold=None):
    """Check if two strings match using fuzzy logic"""
    if not a or not b:
        return False

    # Convert to lowercase for comparison
    a_lower = a.lower()
    b_lower = b.lower()

    # Detect if we're dealing with Arabic team names (often have "Al" prefix)
    is_arabic = any(name.lower().startswith(("al ", "al-")) for name in [a, b])

    # Use provided threshold or default based on name type
    if threshold is None:
        threshold = FUZZY_THRESHOLD_ARABIC if is_arabic else FUZZY_THRESHOLD

    return difflib.SequenceMatcher(None, a_lower, b_lower).ratio() >= threshold

def normalize_team_name(name):
    """Normalize team name for better matching"""
    if not name:
        return ""

    # Convert to lowercase and remove accents
    n = remove_accents(name.lower())

    # Remove parentheses content like "(SA)" or "(Saudi Arabia)"
    n = re.sub(r'\([^)]*\)', '', n)

    # Replace special characters with spaces
    n = re.sub(r'[^\w\s]', ' ', n)

    # Replace multiple spaces with a single space
    n = re.sub(r'\s+', ' ', n)

    return n.strip()

def get_canonical_name(name):
    """Get canonical form of a name (removing all non-alphanumeric characters)"""
    if not name:
        return ""
    # Remove all non-alphanumeric characters
    return re.sub(r'[^a-z0-9]', '', normalize_team_name(name))

def canonical(base_name):
    """
    Map any file‐base (with or without “.json”) to its primary synonym.
    If the base *contains* a known synonym, we’ll use that group’s primary.
    """
    # 1) strip “.json”
    if base_name.lower().endswith('.json'):
        base = base_name[:-5]
    else:
        base = base_name

    # 2) exact match to a synonym key
    if base in SYN_PRIMARY:
        return SYN_PRIMARY[base]

    # 3) substring match: if any synonym appears inside this base
    for group in SYN_GROUPS:
        primary = group[0]
        for syn in group:
            if syn.lower() in base.lower():
                return primary

    # 4) fallback to itself
    return base

def get_phonetic_representation(name):
    """
    Get a phonetic representation of a name to match similar sounding names
    This handles cases like 'K'un' vs 'Kun', etc.
    """
    if not name:
        return ""

    # Normalize first
    n = normalize_team_name(name)

    # Apply phonetic substitutions
    phonetic_subs = [
        (r'k[\'`\-\s]*un', 'kun'),  # K'un, K-un, K un -> kun
        (r'j[\'`\-\s]*in', 'jin'),  # J'in, J-in, J in -> jin
        (r'zh[\'`\-\s]*ou', 'zhou'),  # Zh'ou, Zh-ou -> zhou
        (r'([aeiou])[\'`]', r'\1'),  # Remove apostrophes after vowels
        (r'saint', 'st'),  # Saint -> St
        (r'fc', ''),  # Remove FC
        (r'[\s\-]+', '')  # Remove spaces and hyphens
    ]

    result = n
    for pattern, replacement in phonetic_subs:
        result = re.sub(pattern, replacement, result)

    return result

def simplify_team_name(name):
    """Simplify team name for better matching by removing common words"""
    if not name:
        return ""

    n = normalize_team_name(name)

    # Remove common team words and location identifiers
    words = n.split()
    filtered_words = [w for w in words if w not in COMMON_TEAM_WORDS and w not in LOCATION_IDENTIFIERS]

    # Remove suffix patterns like "ienne", "ais", etc.
    result = ' '.join(filtered_words)
    result = re.sub(r'(ienne|ien|aise|ais|oise|ois|ine|in|é)$', '', result)

    return result.strip()

def extract_significant_words(name):
    """Extract significant words from team name"""
    if not name:
        return set()

    # Normalize and get words
    normalized = normalize_team_name(name)
    words = normalized.split()

    # Filter out short words and common words
    return {w for w in words if len(w) > 2 and
            w not in COMMON_TEAM_WORDS and
            w not in LOCATION_IDENTIFIERS}


def check_team_synonyms(t1, t2):
    """Check if teams are known synonyms"""
    # Normalize names
    n1 = normalize_team_name(t1)
    n2 = normalize_team_name(t2)

    # Check in synonym groups
    for synonym_group in TEAM_SYNONYMS:
        t1_found = any(syn in n1 for syn in synonym_group)
        t2_found = any(syn in n2 for syn in synonym_group)
        if t1_found and t2_found:
            return True

    return False


def teams_match(t1, t2):
    """Check if two team names match using multiple methods"""
    if not t1 or not t2:
        return False

    # 1) Normalize
    n1 = normalize_team_name(t1)
    n2 = normalize_team_name(t2)
    if n1 == n2:
        return True

    # 2) Canonical (alphanumeric only)
    c1 = get_canonical_name(t1)
    c2 = get_canonical_name(t2)
    if c1 and c1 == c2:
        return True

    # 3) Phonetic
    p1 = get_phonetic_representation(t1)
    p2 = get_phonetic_representation(t2)
    if p1 and p1 == p2:
        return True

    # 4) Fuzzy on normalized
    if fuzzy_match(n1, n2):
        return True

    # 5) Fuzzy on phonetic (for longer names)
    if len(p1) > 5 and len(p2) > 5 and fuzzy_match(p1, p2):
        return True

    # ─── NEW: single‑vs‑multi‑word fuzzy match ───
    w1 = n1.split()
    w2 = n2.split()
    # If one side is a single word...
    if len(w1) == 1 and len(w2) > 1:
        # fuzzy against each word of the other
        if any(fuzzy_match(w1[0], other) or
               fuzzy_match(get_phonetic_representation(w1[0]), get_phonetic_representation(other))
               for other in w2):
            return True
    if len(w2) == 1 and len(w1) > 1:
        if any(fuzzy_match(w2[0], other) or
               fuzzy_match(get_phonetic_representation(w2[0]), get_phonetic_representation(other))
               for other in w1):
            return True
    # ──────────────────────────────────────────────

    # ─── NEW: very short names get a looser full‑name fuzzy ───
    if len(n1) <= 5 or len(n2) <= 5:
        if fuzzy_match(n1, n2, threshold=0.5):
            return True
    # ────────────────────────────────────────────────────────

    # 6) Known synonyms from config.json
    if check_team_synonyms(t1, t2):
        return True

    # 7) Simplified names (drop common words) + fuzzy
    s1, s2 = simplify_team_name(t1), simplify_team_name(t2)
    if s1 and s2 and (s1 == s2 or fuzzy_match(s1, s2)):
        return True

    # 8) Significant‑word overlap
    sig1, sig2 = extract_significant_words(t1), extract_significant_words(t2)
    if sig1 and sig2:
        # If both single‑word sets, compare them directly
        if len(sig1) == len(sig2) == 1:
            w1_clean = re.sub(r'[^a-z0-9]', '', next(iter(sig1)))
            w2_clean = re.sub(r'[^a-z0-9]', '', next(iter(sig2)))
            if w1_clean == w2_clean or fuzzy_match(w1_clean, w2_clean, threshold=0.8):
                return True
            if get_phonetic_representation(w1_clean) == get_phonetic_representation(w2_clean):
                return True
        # Otherwise require >50% overlap (normal or phonetic)
        norm1 = {re.sub(r'[^a-z0-9]', '', w) for w in sig1}
        norm2 = {re.sub(r'[^a-z0-9]', '', w) for w in sig2}
        if norm1 & norm2 and len(norm1 & norm2) / min(len(norm1), len(norm2)) > 0.5:
            return True
        ph1 = {get_phonetic_representation(w) for w in sig1}
        ph2 = {get_phonetic_representation(w) for w in sig2}
        if ph1 & ph2 and len(ph1 & ph2) / min(len(ph1), len(ph2)) > 0.5:
            return True

    return False



def pick_best_odds(matches, key):
    """
    Pick the best odd across all matches for a specific market
    Returns the best odd value and its source
    """
    best_value = 0
    best_source = None

    for match in matches:
        try:
            value_raw = match.get(key, "0")

            # Handle both string and numeric types
            if isinstance(value_raw, (int, float)):
                value = float(value_raw)
            elif isinstance(value_raw, str) and value_raw.strip():
                value = float(value_raw)
            else:
                continue  # Skip if empty or invalid

            if value > best_value:
                best_value = value
                best_source = match.get("source")
        except (ValueError, TypeError) as e:
            # Log the error for debugging
            print(f"Error parsing odd {key} from match {match.get('home_team')} vs {match.get('away_team')}: {e}")
            continue

    return best_value, best_source


def check_arbitrage(odds):
    """Check if there's an arbitrage opportunity"""
    # Check if all odds are valid
    if any(v <= 0 for v, _ in odds.values()):
        return None

    # Check if odds come from at least 2 different sources
    sources = {src for _, src in odds.values()}
    if len(sources) < 2:
        return None

    # Calculate arbitrage percentage (convert to decimal odds)
    total = sum(1 / float(v) for v, _ in odds.values())

    # Return the arbitrage percentage if it's profitable
    return total if total < 1 else None


def find_actual_filename(base, dir_path):
    """Find the actual filename for a country (or any synonym) in dir_path"""
    # 1) exact primary.json
    primary = canonical(base)
    want = primary + ".json"
    p = os.path.join(dir_path, want)
    if os.path.exists(p):
        return want

    # 2) fallback: try every synonym in that primary’s group, in config order
    group = next((g for g in SYN_GROUPS if primary in g), None)
    if group:
        for syn in group:
            candidate = syn + ".json"
            if os.path.exists(os.path.join(dir_path, candidate)):
                return candidate

    # 3) last resort: any file containing base substring
    for fn in os.listdir(dir_path):
        if fn.lower().endswith('.json') and base.lower() in fn.lower():
            return fn

    return None



def find_all_matching_matches(matches_by_source):
    """
    Find all possible match combinations across all sources
    Returns a list of groups, where each group contains matching matches from all possible sources
    """
    # Sort sources for deterministic processing order
    sources = sorted(matches_by_source.keys())

    # Step 1: Create match signature index
    # This will map a match signature to all its occurrences across all sources
    match_index = {}

    # First pass: Index all matches by exact signature
    for source in sources:
        for match in matches_by_source[source]:
            # ── SKIP any “combination” entry ──
            home = match.get("home_team", "")
            away = match.get("away_team", "")
            if "/" in home and "/" in away:
                continue
            # Create match key based on normalized team names
            home_norm = normalize_team_name(match.get('home_team', ''))
            away_norm = normalize_team_name(match.get('away_team', ''))
            date_str = match.get('date', '')
            time_str = match.get('time', '').strip()

            # Create a match signature
            match_sig = f"{home_norm}|{away_norm}|{date_str}|{time_str}"

            if match_sig not in match_index:
                match_index[match_sig] = {}

            # Store the match indexed by source
            match["source"] = source  # Ensure source is set
            match_index[match_sig][source] = match

    # Step 2: Find fuzzy matches
    # For each match that doesn't have matches in all sources, try to find fuzzy matches
    fuzzy_matches = []

    # Track which matches have been processed in fuzzy matching
    processed_matches = {src: set() for src in sources}

    # For each source
    for source1 in sources:
        for match1 in matches_by_source[source1]:
            # ── SKIP if match1 is a combination entry ──
            h1 = match1.get("home_team", "")
            a1 = match1.get("away_team", "")
            if "/" in h1 and "/" in a1:
                continue
            # Skip if already found an exact match with all sources
            match1_sig = f"{normalize_team_name(match1.get('home_team', ''))}|{normalize_team_name(match1.get('away_team', ''))}|{match1.get('date', '')}|{match1.get('time', '').strip()}"
            if match1_sig in match_index and len(match_index[match1_sig]) == len(sources):
                continue

            # Skip if this match was already processed in fuzzy matching
            match1_id = f"{match1.get('home_team', '')}|{match1.get('away_team', '')}|{match1.get('date', '')}|{match1.get('time', '')}"
            if match1_id in processed_matches[source1]:
                continue

            # This will track all matches that match with match1 across all sources
            fuzzy_group = {source1: match1}
            processed_matches[source1].add(match1_id)

            # For each other source
            for source2 in sources:
                if source2 == source1:
                    continue

                best_match = None
                best_score = 0

                # Try to find best matching match from source2
                for match2 in matches_by_source[source2]:
                    # ── SKIP if match2 is a combination entry ──
                    h2 = match2.get("home_team", "")
                    a2 = match2.get("away_team", "")
                    if "/" in h2 and "/" in a2:
                        continue
                    # Skip if already used in this fuzzy match group or exact match
                    match2_id = f"{match2.get('home_team', '')}|{match2.get('away_team', '')}|{match2.get('date', '')}|{match2.get('time', '')}"
                    if match2_id in processed_matches[source2]:
                        continue

                    # Check date match (allow one‐day drift just like before)
                    d1 = parse_date(match1.get("date", ""))
                    d2 = parse_date(match2.get("date", ""))
                    date_match = (d1 and d2 and abs((d1 - d2).days) <= 1) \
                                 or match1.get("date", "") == match2.get("date", "")

                    # Check time match, with optional tolerance
                    t1 = match1.get("time", "").strip()
                    t2 = match2.get("time", "").strip()

                    if TIME_DIFF_TOLERANCE <= 0:
                        # exact as before
                        time_match = (t1 == t2)
                    else:
                        try:
                            # assume both use same format, e.g. "HH:MM"
                            fmt = "%H:%M"
                            dt1 = datetime.strptime(t1, fmt)
                            dt2 = datetime.strptime(t2, fmt)
                            diff_min = abs((dt1 - dt2).total_seconds()) / 60
                            time_match = (diff_min <= TIME_DIFF_TOLERANCE)
                        except ValueError:
                            # fallback if parsing fails
                            time_match = (t1 == t2)

                    if date_match and time_match:
                        # Check team names match
                        home_match = teams_match(match1.get("home_team", ""), match2.get("home_team", ""))
                        away_match = teams_match(match1.get("away_team", ""), match2.get("away_team", ""))

                        if home_match and away_match:
                            # Calculate match score
                            h1 = simplify_team_name(match1.get("home_team", ""))
                            a1 = simplify_team_name(match1.get("away_team", ""))
                            h2 = simplify_team_name(match2.get("home_team", ""))
                            a2 = simplify_team_name(match2.get("away_team", ""))

                            home_score = difflib.SequenceMatcher(None, h1, h2).ratio()
                            away_score = difflib.SequenceMatcher(None, a1, a2).ratio()
                            score = (home_score + away_score) / 2

                            if score > best_score:
                                best_score = score
                                best_match = match2

                # If we found a match, add it to the group
                if best_match:
                    best_match["source"] = source2  # Ensure source is set
                    fuzzy_group[source2] = best_match
                    best_match_id = f"{best_match.get('home_team', '')}|{best_match.get('away_team', '')}|{best_match.get('date', '')}|{best_match.get('time', '')}"
                    processed_matches[source2].add(best_match_id)

            # Only add groups that have matches from at least 2 sources
            if len(fuzzy_group) >= 2:
                fuzzy_matches.append(list(fuzzy_group.values()))

    # Step 3: Combine exact matches from match_index
    exact_matches = []
    for sig, matches_by_src in match_index.items():
        if len(matches_by_src) >= 2:  # Only include if found in at least 2 sources
            exact_matches.append(list(matches_by_src.values()))

    # Combine both exact and fuzzy matches
    all_matches = exact_matches + fuzzy_matches
    return all_matches


def analyze_optimal_arbitrage(matching_group):
    """
    Find the optimal arbitrage opportunity across all sources for a match
    This function ensures we check all possible combinations of odds
    """
    # Only process if we have matches from at least 2 sources
    if len(matching_group) < 2:
        return None

    # Extract sources and create match info
    sources = [match.get("source") for match in matching_group]

    # Create match info from the first match
    first_match = matching_group[0]
    country = first_match.get("country", "unknown")

    # Check which home/away team name to use (use the most detailed one)
    home_teams = [m.get("home_team", "") for m in matching_group if m.get("home_team")]
    away_teams = [m.get("away_team", "") for m in matching_group if m.get("away_team")]

    best_home = max(home_teams, key=len) if home_teams else ""
    best_away = max(away_teams, key=len) if away_teams else ""

    info = {
        "home_team": best_home,
        "away_team": best_away,
        "date": first_match.get("date"),
        "time": first_match.get("time"),
        "sources": sources,
        "country": country
    }

    # Match signature for debugging
    match_signature = f"{best_home} vs {best_away} on {first_match.get('date')} at {first_match.get('time')}"

    # Add tournament info for each source
    for match in matching_group:
        src = match.get("source")
        if src:
            info[f"tournament_{src}"] = match.get("tournament_name")

    # Define market sets to check
    market_sets = {
        "three_way": ["1_odd", "draw_odd", "2_odd"],
        "one_vs_x2": ["1_odd", "X2_odd"],
        "two_vs_1x": ["2_odd", "1X_odd"],
        "x_vs_12": ["draw_odd", "12_odd"],
        "both_score": ["both_score_odd", "both_noscore_odd"],
    }

    # Add over/under and Asian Handicap for every line from 0.5 to 8.5 (step 0.5)
    total_lines = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5]
    for n in total_lines:
        market_sets[f"under_{n}_vs_over_{n}"] = [
            f"under_{n:.1f}_odd",
            f"over_{n:.1f}_odd"
        ]

    # With this corrected version:
    handicap_lines = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5,
                      -0.5, -1.0, -1.5, -2.0, -2.5, -3.0, -3.5, -4.0, -4.5, -5.0, -5.5, -6.0, -6.5, -7.0, -7.5, -8.0,
                      -8.5]
    for n in handicap_lines:
        if n == 0.0:
            # Special case for 0.0 handicap - both sides use 0.0
            market_sets[f"ah_{n}_home_vs_away"] = [
                "home_handicap_0.0_odd",
                "away_handicap_0.0_odd"
            ]
        else:
            market_sets[f"ah_{n}_home_vs_away"] = [
                f"home_handicap_{n:.1f}_odd",
                f"away_handicap_{-n:.1f}_odd"
            ]
    # Find the best arbitrage opportunity across all market sets
    best_opportunity = None
    best_arb_percentage = 1.0  # Initialize with 1.0 (no profit)

    for name, keys in market_sets.items():
        # Skip if any key is missing from all matches
        should_skip = False
        for k in keys:
            if all(not match.get(k) or str(match.get(k)).strip() == "" for match in matching_group):
                should_skip = True
                break

        if should_skip:
            continue

        # For each key, find the best odd across all sources
        best_odds = {}
        for k in keys:
            best_odds[k] = pick_best_odds(matching_group, k)

        # Check if we have a valid arbitrage opportunity
        arb = check_arbitrage(best_odds)
        if arb is not None and arb < best_arb_percentage:
            # Format odds for better readability
            formatted_odds = {}
            for k, (v, s) in best_odds.items():
                formatted_odds[k] = {"value": v, "source": s}

            # Create opportunity object
            opportunity = {
                "match_info": info,
                "complementary_set": name,
                "best_odds": formatted_odds,
                "arbitrage_percentage": round(arb, 4)
            }

            best_opportunity = opportunity
            best_arb_percentage = arb

            # Debug info
            print(f"Arbitrage found ({name}): {match_signature}, {arb:.4f}")

    # Return the best opportunity (or None if no arbitrage found)
    return [best_opportunity] if best_opportunity else None


def dedupe_country_file(country_fn: str,
                        key_fields: tuple = ("match_info", "home_team",
                                             "arbitrage_percentage",
                                             "complementary_set",
                                             "match_info", "date",
                                             "match_info", "time")) -> int:
    """
    Read country_fn, drop duplicates based on key_fields,
    overwrite country_fn, and return how many entries were removed.
    """
    # load
    with open(country_fn, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # if it's a dict, convert to list
    opps = data if isinstance(data, list) else list(data.values())

    seen = set()
    unique = []
    for opp in opps:
        # build the key (including nested paths)
        key = (
            opp["match_info"]["home_team"],
            opp["arbitrage_percentage"],
            opp["complementary_set"],
            opp["match_info"]["date"],
            opp["match_info"]["time"],
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(opp)

    removed = len(opps) - len(unique)
    if removed > 0:
        # write back
        with open(country_fn, 'w', encoding='utf-8') as f:
            json.dump(unique, f, indent=2, ensure_ascii=False)
        print(f"[INFO] {os.path.basename(country_fn)}: removed {removed} duplicates")

    return removed



def dedupe_all_country_files(json_dir: str) -> int:
    total_removed = 0
    for fn in os.listdir(json_dir):
        if not fn.lower().endswith('.json'):
            continue
        full_path = os.path.join(json_dir, fn)
        total_removed += dedupe_country_file(full_path)
    return total_removed


def process_files_optimal():
    """Process all files to find optimal arbitrage opportunities across all sources"""
    reset_output()
    total_matching_groups = 0
    total_arb = 0
    all_margins = []  # <-- collect every arbitrage “margin (%)” here

    # Dictionary to store arbitrage opportunities by country
    arbitrage_by_country = {}

    # Track which country files we've already processed
    processed_countries = set()

    # Use the configured source directories
    sources = SOURCE_DIRECTORIES

    # Process each country file once, collapsed to its canonical (primary) name
    all_countries = set()
    for _, source_dir in sources:
        if not os.path.isdir(source_dir):
            continue
        for fname in os.listdir(source_dir):
            if not fname.lower().endswith('.json'):
                continue
            raw = os.path.splitext(fname)[0]
            canon = canonical(raw)
            all_countries.add(canon)

    # Process each country
    for country_name in sorted(all_countries):
        # Skip if we've already processed this country
        if country_name in processed_countries:
            continue

        processed_countries.add(country_name)
        base = country_name

        # Find corresponding files in all sources
        # Find **all** synonym files in each source
        paths = {}
        for src_name, src_dir in sources:
            if not os.path.isdir(src_dir):
                continue

            files = []
            for fname in os.listdir(src_dir):
                if not fname.lower().endswith('.json'):
                    continue
                raw = os.path.splitext(fname)[0]
                if canonical(raw) == base:
                    files.append(os.path.join(src_dir, fname))

            if files:
                paths[src_name] = files

        # Need at least two sources with data
        if len(paths) < 2:
            continue

        # Load matches from each source (all synonym files)
        data = {}
        for src_name, file_list in paths.items():
            entries = []
            for path in file_list:
                entries.extend(load_matches(path))
            for m in entries:
                m["source"] = src_name
            data[src_name] = entries

        # Find all possible matching matches across all sources
        matching_groups = find_all_matching_matches(data)
        total_matching_groups += len(matching_groups)

        print(f"Country {country_name}: Found {len(matching_groups)} matching groups across sources.")

        # Analyze each group for optimal arbitrage opportunities
        country_arb_count = 0
        for group in matching_groups:
            opportunities = analyze_optimal_arbitrage(group)
            if opportunities:
                country_arb_count += len(opportunities)
                total_arb += len(opportunities)

                # convert each arb decimal into a percentage margin ──
                for opp in opportunities:
                    arb_decimal = opp["arbitrage_percentage"]
                    margin_pct = (1.0 - arb_decimal) * 100
                    all_margins.append(margin_pct)

                # Group opportunities by country
                country = group[0].get("country", country_name)
                if country not in arbitrage_by_country:
                    arbitrage_by_country[country] = []
                arbitrage_by_country[country].extend(opportunities)

        if country_arb_count > 0:
            print(f"Country {country_name}: Found {country_arb_count} arbitrage opportunities.")

    # Save arbitrage opportunities by country
    for country, opps in sorted(arbitrage_by_country.items()):
        if opps:
            out_path = os.path.join(OUTPUT_DIR, f"{country}.json")
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(opps, f, ensure_ascii=False, indent=2)
    # Remove duplicates from the json files
    removed_total = dedupe_all_country_files("arbitrage_opportunities")
    total_arb = total_arb - removed_total
    print(f"Total matching groups: {total_matching_groups}")
    print(f"Total arbitrage opportunities: {total_arb}")
    # ── NEW: compute & print average and max arbitrage‐margin ──
    if all_margins:
        avg_margin = sum(all_margins) / len(all_margins)
        max_margin = max(all_margins)
        print(f"Profit margin: Avg {avg_margin:.2f}%, Max {max_margin:.2f}%")
    else:
        print("Profit margin: No arbitrage opportunities found.")


def test_team_matching():
    """Test function to validate team name matching logic"""
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
            # Debug info
            print(f"    - Normalized: '{normalize_team_name(team1)}' vs '{normalize_team_name(team2)}'")
            print(f"    - Simplified: '{simplify_team_name(team1)}' vs '{simplify_team_name(team2)}'")
            print(f"    - Words: {extract_significant_words(team1)} vs {extract_significant_words(team2)}")
            print(f"    - Synonyms: {check_team_synonyms(team1, team2)}")





if __name__ == '__main__':
    # Test team name matching
    test_team_matching()

    # Process all files to find arbitrage opportunities
    process_files_optimal()

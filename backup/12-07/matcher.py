# matcher.py - Performance Optimized (Logic Preserved)

import re
import unicodedata
import difflib
from datetime import datetime
from typing import Dict, List, Set, Any, Optional
from functools import lru_cache

# ----- DEBUGGING Configuartions -----
# Set DEBUG to True to see detailed logs for a specific match.
# The log will trace its comparison process against all other potential matches.
DEBUG = False  # True or False
DEBUG_SOURCE_NAME = "Clubx2"  # The 'source' of the match to debug
DEBUG_MATCH_ID = "13259998"  # The 'match_id' of the match to debug
# ---------------------

# --- Important terms placeholder (populated in main.py) ---
IMPORTANT_TERM_GROUPS: List[List[str]] = []
SYN_PRIMARY: Set[str] = set()
SYN_GROUPS: Set[str] = set()
COMMON_TEAM_WORDS: Set[str] = set()
LOCATION_IDENTIFIERS: Set[str] = set()
TEAM_SYNONYMS: Set[str] = set()
STRONG_THRESHOLD: List[float] = []
MODERATE_THRESHOLD: List[float] = []
TIME_DIFF_TOLERANCE: Set[int] = set()
GATEKEEPER_THRESHOLD: Set[int] = set()
DAY_DIFF_TOLERANCE: Set[int] = set()

# Pre-compiled regex patterns for performance only
_PARENTHETICAL_PATTERN = re.compile(r"\([^)]*\)")
_NON_WORD_PATTERN = re.compile(r"[^\w\s]")
_WHITESPACE_PATTERN = re.compile(r"\s+")
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]")
_ROMAN_NUMERALS = [
    "XVIII", "XVII", "XVI", "XIII", "XIV", "XII", "XIX", "XV",
    "VIII", "VII", "III", "XII", "XIV", "XVI", "XVII", "XIX",
    "IV", "IX", "VI", "XI", "XX", "II"
]
_ROMAN_PATTERN = re.compile(r'\b(' + '|'.join(_ROMAN_NUMERALS) + r')\b', re.IGNORECASE)
_SUFFIX_PATTERN = re.compile(r"(ienne|ien|aise|ais|oise|ois|ine|in|é)$")


def _log_debug(is_target: bool, *args):
    """Helper function to print debug messages only when DEBUG is on and it's the target match."""
    if DEBUG and is_target:
        print("[DEBUG]", *args)


def _debug_match_details(m1: Dict, m2: Dict, home_score: float, away_score: float):
    """Prints a detailed breakdown of the name comparison for debugging."""
    print(f"    [TEAMS] '{m1['home_team']}' vs '{m2['home_team']}'")
    print(f"    [CORE]  '{get_core_name(m1['home_team'])}' vs '{get_core_name(m2['home_team'])}'")
    print(f"    [SCORE] Home Score: {home_score:.3f}")
    print(f"    --------------------")
    print(f"    [TEAMS] '{m1['away_team']}' vs '{m2['away_team']}'")
    print(f"    [CORE]  '{get_core_name(m1['away_team'])}' vs '{get_core_name(m2['away_team'])}'")
    print(f"    [SCORE] Away Score: {away_score:.3f}")


# Cache only for pure performance - no logic changes
@lru_cache(maxsize=10000)
def remove_accents(text: str) -> str:
    """
    Remove accents (diacritics) from a Unicode string.
    """
    if not text:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if not unicodedata.combining(c)
    )


@lru_cache(maxsize=10000)
def normalize_team_name(name: str) -> str:
    """
    Lowercase, strip accents, remove parenthetical content, replace non-alphanumeric
    with spaces, collapse whitespace, and trim.
    """
    if not name:
        return ""
    n = remove_accents(name.lower())
    # Remove anything in parentheses, e.g. "(SA)"
    n = _PARENTHETICAL_PATTERN.sub("", n)
    # Replace non-word characters with spaces
    n = _NON_WORD_PATTERN.sub(" ", n)
    # Collapse multiple spaces
    n = _WHITESPACE_PATTERN.sub(" ", n)
    return n.strip()


@lru_cache(maxsize=10000)
def get_canonical_name(name: str) -> str:
    """
    Return a fully alphanumeric-only representation of the normalized team name.
    E.g. "Al-Hilal FC (SA)" -> "alhilalsa"
    """
    if not name:
        return ""
    norm = normalize_team_name(name)
    return _NON_ALNUM_PATTERN.sub("", norm)


@lru_cache(maxsize=5000)
def canonical(base_name: str) -> str:
    """
    Map any base name (e.g. "france" or "us.open") to its primary synonym,
    using SYN_GROUPS and SYN_PRIMARY that the main script provides.
    Strips ".json" if present.
    """
    if base_name.lower().endswith(".json"):
        base = base_name[:-5]
    else:
        base = base_name

    # 1) exact match to a synonym key
    if base in SYN_PRIMARY:
        return SYN_PRIMARY[base]

    # 2) substring match: if any synonym appears inside this base
    for group in SYN_GROUPS:
        primary = group[0]
        for syn in group:
            if syn.lower() in base.lower():
                return primary

    # 3) fallback to itself
    return base


@lru_cache(maxsize=5000)
def get_phonetic_representation(name: str) -> str:
    """
    Apply simple regex substitutions to convert certain patterns into
    a phonetic-like form (e.g. "K'un" -> "kun").
    """
    if not name:
        return ""
    n = normalize_team_name(name)

    phonetic_subs = [
        (r"k[\'`\-\s]*un", "kun"),
        (r"j[\'`\-\s]*in", "jin"),
        (r"zh[\'`\-\s]*ou", "zhou"),
        (r"([aeiou])[\'`]", r"\1"),
        (r"saint", "st"),
        (r"fc", ""),
        (r"[\s\-]+", ""),
    ]

    result = n
    for pattern, replacement in phonetic_subs:
        result = re.sub(pattern, replacement, result)

    return result


@lru_cache(maxsize=5000)
def simplify_team_name(name: str) -> str:
    """
    Remove common team words, location identifiers, Roman numerals, and then
    strip typical suffixes.
    """
    if not name:
        return ""
    n = normalize_team_name(name)

    # CORRECTED ROMAN NUMERAL LOGIC - using pre-compiled pattern
    n = _ROMAN_PATTERN.sub("", n)

    # Clean up extra spaces that may result from the substitution
    n = _WHITESPACE_PATTERN.sub(" ", n).strip()

    words = n.split()
    filtered_words = [
        w for w in words
        if w not in COMMON_TEAM_WORDS and w not in LOCATION_IDENTIFIERS
    ]
    result = " ".join(filtered_words)
    result = _SUFFIX_PATTERN.sub("", result)
    return result.strip()


@lru_cache(maxsize=5000)
def get_core_name(name: str) -> str:
    """
    Strips common words AND important terms to get the core identifier of a team.
    e.g., "America Mineiro U20" -> "america mineiro"
    """
    if not name:
        return ""
    # Start with the simplified name (removes common words like 'fc', 'ec', etc.)
    simplified = simplify_team_name(name)

    # Flatten the important term groups into a single list for easier processing
    all_important_terms = [term for group in IMPORTANT_TERM_GROUPS for term in group]

    # Remove all important terms, ignoring case
    core_name = simplified
    for term in all_important_terms:
        # Use regex to remove the term as a whole word, with flexible spacing
        # This prevents "reserve" from removing the "rese" in "Varese"
        pattern = re.compile(r'\b' + re.escape(term.lower()) + r'\b', flags=re.IGNORECASE)
        core_name = pattern.sub("", core_name)
        # remove all standalone numbers
        core_name = re.sub(r'\b\d+\b', '', core_name)
    # Clean up extra whitespace that may result from substitutions
    core_name = _WHITESPACE_PATTERN.sub(" ", core_name).strip()
    return core_name


@lru_cache(maxsize=5000)
def extract_significant_words(name: str) -> Set[str]:
    """
    From a normalized team name, return the set of words longer than 2 characters
    that are not common_team_words or location_identifiers.
    """
    if not name:
        return set()
    normalized = normalize_team_name(name)
    words = normalized.split()
    return {
        w for w in words
        if len(w) > 2 and w not in COMMON_TEAM_WORDS and w not in LOCATION_IDENTIFIERS
    }


@lru_cache(maxsize=10000)
def check_team_synonyms(t1: str, t2: str) -> bool:
    """
    Return True if both t1 and t2 contain any synonym from the same synonym group.
    """
    n1 = normalize_team_name(t1)
    n2 = normalize_team_name(t2)
    for synonym_group in TEAM_SYNONYMS:
        found1 = any(syn in n1 for syn in synonym_group)
        found2 = any(syn in n2 for syn in synonym_group)
        if found1 and found2:
            return True
    return False


@lru_cache(maxsize=10000)
def calculate_jaccard_score(name1: str, name2: str) -> float:
    """
    Calculates a robust similarity score between two team names.
    It combines Jaccard similarity on core words with a fuzzy
    SequenceMatcher ratio on the full core names to handle minor
    variations (e.g., plurals, typos).
    """
    # Use the existing get_core_name function to preprocess the names
    core1 = get_core_name(name1)
    core2 = get_core_name(name2)

    if not core1 or not core2:
        return 0.0

    # 1. Calculate the Jaccard score (original logic)
    set1 = set(core1.split())
    set2 = set(core2.split())

    jaccard_score = 0.0
    if set1 or set2:  # Avoid division by zero if both are empty
        intersection = set1.intersection(set2)
        union = set1.union(set2)
        if union:
            jaccard_score = len(intersection) / len(union)

    # 2. Calculate a fuzzy ratio on the complete core names
    # This is excellent at catching minor differences like 'kristianstad' vs 'kristianstads'
    fuzzy_score = difflib.SequenceMatcher(None, core1, core2).ratio()

    # 3. Return the higher of the two scores
    # This preserves the strength of the Jaccard method for word order
    # while adding a fallback for minor string differences.
    return max(jaccard_score, fuzzy_score)


@lru_cache(maxsize=10000)
def fuzzy_match(a: str, b: str, threshold: Optional[float] = None) -> bool:
    """
    Return True if SequenceMatcher.ratio() >= threshold. Uses a lower threshold
    for Arabic names (starting with "al " or "al-").
    """
    if not a or not b:
        return False

    a_lower = a.lower()
    b_lower = b.lower()
    if threshold is None:
        threshold = 0.5

    return difflib.SequenceMatcher(None, a_lower, b_lower).ratio() >= threshold


def teams_match(t1: str, t2: str) -> bool:
    # EXACT SAME LOGIC - just using cached helper functions
    if not t1 or not t2:
        return False
    t1_lower = t1.lower()
    t2_lower = t2.lower()

    # 1) ENHANCED IMPORTANT-TERM PRESENCE CHECK (CORRECTED)
    def check_presence(source_lower: str, target_lower: str) -> bool:
        # Find all terms present as whole words in the source string
        present_terms = {
            term.lower() for group in IMPORTANT_TERM_GROUPS for term in group
            if re.search(r'\b' + re.escape(term.lower()) + r'\b', source_lower)
        }

        if not present_terms:
            return True  # No important terms in source, so no restriction on target.

        # Gather all synonym groups for the terms we found.
        relevant_groups = [
            group for group in IMPORTANT_TERM_GROUPS
            if any(term.lower() in present_terms for term in group)
        ]
        # Combine all synonyms from those relevant groups into one set.
        combined_terms = {term.lower() for group in relevant_groups for term in group}

        # Ensure at least one of the synonyms appears as a whole word in the target.
        return any(re.search(r'\b' + re.escape(term) + r'\b', target_lower) for term in combined_terms)

    if not (check_presence(t1_lower, t2_lower) and check_presence(t2_lower, t1_lower)):
        return False

    # 2) STRIP IMPORTANT TERMS FOR COMPARISON ONLY (CORRECTED)
    comp1, comp2 = t1, t2
    all_important_terms = [term for group in IMPORTANT_TERM_GROUPS for term in group]
    for term in all_important_terms:
        # Use word boundaries (\b) to ensure only whole words are removed
        pattern = re.compile(r'\b' + re.escape(term) + r'\b', flags=re.IGNORECASE)
        comp1 = pattern.sub("", comp1)
        comp2 = pattern.sub("", comp2)

    # 3) NORMALIZE AND COMPARE (No changes from here onwards in this function)
    n1 = normalize_team_name(comp1)
    n2 = normalize_team_name(comp2)
    if n1 == n2:
        return True

    # ... (rest of the function remains identical)
    c1 = get_canonical_name(comp1)
    c2 = get_canonical_name(comp2)
    if c1 and c1 == c2:
        return True

    p1 = get_phonetic_representation(comp1)
    p2 = get_phonetic_representation(comp2)
    if p1 and p1 == p2:
        return True

    if fuzzy_match(n1, n2):
        return True
    if len(p1) > 5 and len(p2) > 5 and fuzzy_match(p1, p2):
        return True

    w1, w2 = n1.split(), n2.split()
    if len(w1) == 1 and len(w2) > 1:
        if any(fuzzy_match(w1[0], other) or
               fuzzy_match(get_phonetic_representation(w1[0]), get_phonetic_representation(other))
               for other in w2):
            return True
    if len(w2) == 1 and len(w1) > 1:
        if any(fuzzy_match(w2[0], other) or
               fuzzy_match(get_phonetic_representation(w2[0]), get_phonetic_representation(other))
               for other in w1):
            return True

    if len(n1) <= 5 or len(n2) <= 5:
        if fuzzy_match(n1, n2, threshold=0.5):
            return True

    if check_team_synonyms(t1, t2):
        return True

    s1 = simplify_team_name(comp1)
    s2 = simplify_team_name(comp2)
    if s1 and s2 and (s1 == s2 or fuzzy_match(s1, s2)):
        return True

    sig1 = extract_significant_words(comp1)
    sig2 = extract_significant_words(comp2)
    if sig1 and sig2:
        if len(sig1) == len(sig2) == 1:
            w1_clean = _NON_ALNUM_PATTERN.sub("", next(iter(sig1)))
            w2_clean = _NON_ALNUM_PATTERN.sub("", next(iter(sig2)))
            if w1_clean == w2_clean or fuzzy_match(w1_clean, w2_clean, threshold=0.8):
                return True
            if get_phonetic_representation(w1_clean) == get_phonetic_representation(w2_clean):
                return True
        norm1 = {_NON_ALNUM_PATTERN.sub("", w) for w in sig1}
        norm2 = {_NON_ALNUM_PATTERN.sub("", w) for w in sig2}
        if norm1 & norm2 and len(norm1 & norm2) / min(len(norm1), len(norm2)) > 0.5:
            return True
        ph1 = {get_phonetic_representation(w) for w in sig1}
        ph2 = {get_phonetic_representation(w) for w in sig2}
        if ph1 & ph2 and len(ph1 & ph2) / min(len(ph1), len(ph2)) > 0.5:
            return True

    return False


@lru_cache(maxsize=1000)
def parse_date(date_str: str) -> Optional[datetime.date]:
    """
    Parse a date string into a datetime.date object. Supports multiple formats.
    Returns None if parsing fails.
    """
    if not date_str:
        return None
    try:
        d, m, y = date_str.strip().split("/")
        return datetime(int(y), int(m), int(d)).date()
    except ValueError:
        formats = ["%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%d.%m.%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
    return None


def find_all_matching_matches(
        matches_by_source: Dict[str, List[Dict[str, Any]]]
) -> List[List[Dict[str, Any]]]:
    """
    Find matching groups across sources, merging during the fuzzy phase,
    and annotate each match dict with a `matching_group_id` composed of
    the source-specific match_ids sorted by length descending.

    Added: symmetric best‐match verification to avoid one‐way “stealing” of matches.
    """
    sources = sorted(matches_by_source.keys())

    def sig_key(m: Dict[str, Any]) -> tuple:
        return (
            normalize_team_name(m.get("home_team", "")),
            normalize_team_name(m.get("away_team", "")),
            m.get("date", ""),
            m.get("time", "").strip()
        )

    processed: Dict[str, Set[str]] = {src: set() for src in sources}
    groups: List[List[Dict[str, Any]]] = []

    # STEP 1: Exact signature grouping
    for src in sources:
        exact_index: Dict[tuple, List[Dict[str, Any]]] = {}
        for match in matches_by_source[src]:
            match.setdefault("source", src)
            key = sig_key(match)
            exact_index.setdefault(key, []).append(match)
        for bucket in exact_index.values():
            if len(bucket) > 1:
                for m in bucket:
                    processed[m['source']].add(str(m['match_id']))
                groups.append(bucket)

    # STEP 2: Fuzzy matching with symmetric best‐match check
    for src1 in sources:
        for m1 in matches_by_source[src1]:
            m1.setdefault("source", src1)
            mid1 = str(m1['match_id'])
            if mid1 in processed[src1]:
                continue

            is_debug = (
                DEBUG and
                m1.get('source') == DEBUG_SOURCE_NAME and
                mid1 == str(DEBUG_MATCH_ID)
            )

            if is_debug:
                print(f"[DEBUG] Starting fuzzy match for {m1['home_team']} vs {m1['away_team']}")

            group = [m1]
            processed[src1].add(mid1)

            for src2 in sources:
                if src2 == src1:
                    continue

                best_match = None
                best_score = 0.0

                # 1 - Forward search: find best candidate in src2 for m1
                for m2 in matches_by_source[src2]:
                    m2.setdefault("source", src2)
                    mid2 = str(m2['match_id'])
                    if mid2 in processed[src2]:
                        continue

                    # Date guard
                    d1 = parse_date(m1.get('date', ''))
                    d2 = parse_date(m2.get('date', ''))
                    if not d1 or not d2 or abs((d1 - d2).days) > DAY_DIFF_TOLERANCE:
                        continue

                    # Time guard
                    t1, t2 = m1.get('time','').strip(), m2.get('time','').strip()
                    try:
                        dt1 = datetime.strptime(t1, "%H:%M")
                        dt2 = datetime.strptime(t2, "%H:%M")
                        if abs((dt1 - dt2).total_seconds())/60 > TIME_DIFF_TOLERANCE:
                            continue
                    except ValueError:
                        if t1 != t2:
                            continue

                    # Important-term guard
                    text1 = (m1['home_team'] + " " + m1['away_team']).lower()
                    text2 = (m2['home_team'] + " " + m2['away_team']).lower()
                    for grp in IMPORTANT_TERM_GROUPS:
                        in1 = any(re.search(r'\b'+re.escape(term.lower())+r'\b', text1) for term in grp)
                        in2 = any(re.search(r'\b'+re.escape(term.lower())+r'\b', text2) for term in grp)
                        if in1 != in2:
                            break
                    else:
                        # compute similarity scores
                        home_score = (
                            1.0 if check_team_synonyms(m1['home_team'], m2['home_team'])
                            else calculate_jaccard_score(m1['home_team'], m2['home_team'])
                        )
                        away_score = (
                            1.0 if check_team_synonyms(m1['away_team'], m2['away_team'])
                            else calculate_jaccard_score(m1['away_team'], m2['away_team'])
                        )
                        if min(home_score, away_score) < GATEKEEPER_THRESHOLD:
                            continue
                        if not any(
                            (home_score >= s and away_score >= m) or
                            (away_score >= s and home_score >= m)
                            for s, m in zip(STRONG_THRESHOLD, MODERATE_THRESHOLD)
                        ):
                            continue

                        avg_score = (home_score + away_score) / 2
                        if avg_score > best_score:
                            best_score = avg_score
                            best_match = m2

                if not best_match:
                    continue

                # 2 - Reverse search: verify best_match also prefers m1 over alternatives
                reverse_best = None
                reverse_score = 0.0
                for m1b in matches_by_source[src1]:
                    m1b.setdefault("source", src1)
                    # allow even processed ones: looking for true preference
                    # Date guard
                    d1b = parse_date(m1b.get('date',''))
                    d2b = parse_date(best_match.get('date',''))
                    if not d1b or not d2b or abs((d1b - d2b).days) > DAY_DIFF_TOLERANCE:
                        continue
                    # Time guard
                    tb1, tb2 = m1b.get('time','').strip(), best_match.get('time','').strip()
                    try:
                        dtb1 = datetime.strptime(tb1, "%H:%M")
                        dtb2 = datetime.strptime(tb2, "%H:%M")
                        if abs((dtb1 - dtb2).total_seconds())/60 > TIME_DIFF_TOLERANCE:
                            continue
                    except ValueError:
                        if tb1 != tb2:
                            continue
                    # Important-term guard
                    text1b = (m1b['home_team'] + " " + m1b['away_team']).lower()
                    text2b = (best_match['home_team'] + " " + best_match['away_team']).lower()
                    for grp in IMPORTANT_TERM_GROUPS:
                        in1b = any(re.search(r'\b'+re.escape(term.lower())+r'\b', text1b) for term in grp)
                        in2b = any(re.search(r'\b'+re.escape(term.lower())+r'\b', text2b) for term in grp)
                        if in1b != in2b:
                            break
                    else:
                        home_score_rev = (
                            1.0 if check_team_synonyms(m1b['home_team'], best_match['home_team'])
                            else calculate_jaccard_score(m1b['home_team'], best_match['home_team'])
                        )
                        away_score_rev = (
                            1.0 if check_team_synonyms(m1b['away_team'], best_match['away_team'])
                            else calculate_jaccard_score(m1b['away_team'], best_match['away_team'])
                        )
                        if min(home_score_rev, away_score_rev) < GATEKEEPER_THRESHOLD:
                            continue
                        if not any(
                            (home_score_rev >= s and away_score_rev >= m) or
                            (away_score_rev >= s and home_score_rev >= m)
                            for s, m in zip(STRONG_THRESHOLD, MODERATE_THRESHOLD)
                        ):
                            continue

                        avg_rev = (home_score_rev + away_score_rev) / 2
                        if avg_rev > reverse_score:
                            reverse_score = avg_rev
                            reverse_best = m1b

                # 3 - Accept only if mutual best or our forward link is stronger
                if reverse_best is not m1 and reverse_score > best_score:
                    # skip this pair
                    if is_debug:
                        print(f"[DEBUG] Skipping {m1['match_id']}↔{best_match['match_id']} (reverse {reverse_score:.3f} > forward {best_score:.3f})")
                    continue

                # 4 - Link them
                group.append(best_match)
                processed[best_match['source']].add(str(best_match['match_id']))

            if len(group) > 1:
                groups.append(group)

    # STEP 3: Annotate groups with a stable ID
    for group in groups:
        ids = [str(m['match_id']) for m in group]
        sorted_ids = sorted(ids, key=lambda x: (-len(x), x))
        gid = "-".join(sorted_ids)
        for m in group:
            m['matching_group_id'] = gid

    return groups
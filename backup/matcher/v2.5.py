# matcher.py

import re
import unicodedata
import difflib
from datetime import datetime
from typing import Dict, List, Set, Any, Optional

# --- Important terms placeholder (populated in main.py) ---
# This is now a list of lists, e.g., [["(W)", "Wom"], ["Reserve", "II"]]
IMPORTANT_TERM_GROUPS: List[List[str]] = []
SYN_PRIMARY: Set[str] = set()
SYN_GROUPS: Set[str] = set()
COMMON_TEAM_WORDS: Set[str] = set()
LOCATION_IDENTIFIERS: Set[str] = set()
TEAM_SYNONYMS: Set[str] = set()
STRONG_THRESHOLD: Set[float] = set()
MODERATE_THRESHOLD: Set[float] = set()
TIME_DIFF_TOLERANCE: Set[int] = set()
GATEKEEPER_THRESHOLD: Set[int] = set()
DAY_DIFF_TOLERANCE: Set[int] = set()



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


def normalize_team_name(name: str) -> str:
    """
    Lowercase, strip accents, remove parenthetical content, replace non-alphanumeric
    with spaces, collapse whitespace, and trim.
    """
    if not name:
        return ""
    n = remove_accents(name.lower())
    # Remove anything in parentheses, e.g. "(SA)"
    n = re.sub(r"\([^)]*\)", "", n)
    # Replace non-word characters with spaces
    n = re.sub(r"[^\w\s]", " ", n)
    # Collapse multiple spaces
    n = re.sub(r"\s+", " ", n)
    return n.strip()


def get_canonical_name(name: str) -> str:
    """
    Return a fully alphanumeric-only representation of the normalized team name.
    E.g. "Al-Hilal FC (SA)" -> "alhilalsa"
    """
    if not name:
        return ""
    norm = normalize_team_name(name)
    return re.sub(r"[^a-z0-9]", "", norm)


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


def simplify_team_name(name: str) -> str:
    """
    Remove common team words, location identifiers, Roman numerals, and then
    strip typical suffixes.
    """
    if not name:
        return ""
    n = normalize_team_name(name)

    # CORRECTED ROMAN NUMERAL LOGIC
    # List of Roman numerals, sorted by length (longest first) to prevent partial matching.
    # Excludes single-letter numerals to avoid false positives on words like "Vitesse" or "Inter".
    roman_numerals = [
        "XVIII", "XVII", "XVI", "XIII", "XIV", "XII", "XIX", "XV",
        "VIII", "VII", "III", "XII", "XIV", "XVI", "XVII", "XIX",
        "IV", "IX", "VI", "XI", "XX", "II"
    ]
    # Create the regex pattern with word boundaries
    roman_pattern = re.compile(r'\b(' + '|'.join(roman_numerals) + r')\b', re.IGNORECASE)
    n = roman_pattern.sub("", n)

    # Clean up extra spaces that may result from the substitution
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

    # Clean up extra whitespace that may result from substitutions
    core_name = re.sub(r"\s+", " ", core_name).strip()
    return core_name


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
    """
    Decide if two team-name strings refer to the same team.
    """
    if not t1 or not t2:
        return False

    # 1) IMPORTANT‑TERM PRESENCE CHECK (UPDATED LOGIC)
    # For each group of synonyms (e.g., ["Reserve", "II"]), ensure that if one
    # name contains a term from the group, the other one must as well.
    t1_lower, t2_lower = t1.lower(), t2.lower()
    for group in IMPORTANT_TERM_GROUPS:
        has1 = any(term.lower() in t1_lower for term in group)
        has2 = any(term.lower() in t2_lower for term in group)
        if has1 ^ has2:
            return False

    # 2) STRIP IMPORTANT TERMS FOR COMPARISON ONLY
    comp1, comp2 = t1, t2
    # Flatten the groups to get all terms for stripping
    all_important_terms = [term for group in IMPORTANT_TERM_GROUPS for term in group]
    for term in all_important_terms:
        pattern = re.compile(re.escape(term), flags=re.IGNORECASE)
        comp1 = pattern.sub("", comp1)
        comp2 = pattern.sub("", comp2)

    # 3) NORMALIZE AND COMPARE
    n1 = normalize_team_name(comp1)
    n2 = normalize_team_name(comp2)
    if n1 == n2:
        return True

    # 4) CANONICAL (alphanumeric-only) COMPARISON
    c1 = get_canonical_name(comp1)
    c2 = get_canonical_name(comp2)
    if c1 and c1 == c2:
        return True

    # ... (rest of the function is unchanged) ...
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
               fuzzy_match(get_phonetic_representation(w1[0]),
                           get_phonetic_representation(other))
               for other in w2):
            return True
    if len(w2) == 1 and len(w1) > 1:
        if any(fuzzy_match(w2[0], other) or
               fuzzy_match(get_phonetic_representation(w2[0]),
                           get_phonetic_representation(other))
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
            w1_clean = re.sub(r"[^a-z0-9]", "", next(iter(sig1)))
            w2_clean = re.sub(r"[^a-z0-9]", "", next(iter(sig2)))
            if w1_clean == w2_clean or fuzzy_match(w1_clean, w2_clean, threshold=0.8):
                return True
            if get_phonetic_representation(w1_clean) == get_phonetic_representation(w2_clean):
                return True
        norm1 = {re.sub(r"[^a-z0-9]", "", w) for w in sig1}
        norm2 = {re.sub(r"[^a-z0-9]", "", w) for w in sig2}
        if norm1 & norm2 and len(norm1 & norm2) / min(len(norm1), len(norm2)) > 0.5:
            return True
        ph1 = {get_phonetic_representation(w) for w in sig1}
        ph2 = {get_phonetic_representation(w) for w in sig2}
        if ph1 & ph2 and len(ph1 & ph2) / min(len(ph1), len(ph2)) > 0.5:
            return True
    return False


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
    sources = sorted(matches_by_source.keys())
    match_index: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # STEP 1: Exact-signature indexing (unchanged)
    for source in sources:
        for match in matches_by_source[source]:
            home = match.get("home_team", "")
            away = match.get("away_team", "")
            if "/" in home and "/" in away: continue
            sig = (f"{normalize_team_name(home)}|{normalize_team_name(away)}|"
                   f"{match.get('date', '')}|{match.get('time', '').strip()}")
            match.setdefault("source", source)
            match_index.setdefault(sig, {})[source] = match

    # STEP 2: Fuzzy matching for incomplete groups
    fuzzy_matches: List[List[Dict[str, Any]]] = []
    processed: Dict[str, Set[str]] = {src: set() for src in sources}

    for src1 in sources:
        for m1 in matches_by_source[src1]:
            h1, a1 = m1.get("home_team", ""), m1.get("away_team", "")
            if "/" in h1 and "/" in a1: continue
            id1 = f"{h1}|{a1}|{m1.get('date', '')}|{m1.get('time', '')}"
            if id1 in processed[src1]: continue
            processed[src1].add(id1)
            group = {src1: m1}

            for src2 in sources:
                if src2 == src1: continue
                best_match = None
                best_score = 0.0

                for m2 in matches_by_source[src2]:
                    h2, a2 = m2.get("home_team", ""), m2.get("away_team", "")
                    id2 = f"{h2}|{a2}|{m2.get('date', '')}|{m2.get('time', '')}"
                    if "/" in h2 and "/" in a2 or id2 in processed.get(src2, set()): continue

                    # Date and Time matching
                    d1, d2 = parse_date(m1.get("date", "")), parse_date(m2.get("date", ""))

                    # OLD LINE:
                    # if not (d1 and d2 and d1 == d2): continue

                    # NEW LOGIC WITH DAY TOLERANCE:
                    if not (d1 and d2): continue  # Skip if either date is invalid

                    day_diff = abs((d1 - d2).days)
                    if day_diff > DAY_DIFF_TOLERANCE:
                        continue

                    t1, t2 = m1.get("time", "").strip(), m2.get("time", "").strip()
                    try:
                        dt1 = datetime.strptime(t1, "%H:%M")
                        dt2 = datetime.strptime(t2, "%H:%M")
                        time_diff = abs((dt1 - dt2).total_seconds()) / 60
                        if time_diff > TIME_DIFF_TOLERANCE: continue
                    except (ValueError, TypeError):
                        if t1 != t2: continue

                    # IMPORTANT-TERM GUARD
                    text1 = (h1 + " " + a1).lower()
                    text2 = (h2 + " " + a2).lower()
                    if any((any(term.lower() in text1 for term in term_group)) ^
                           (any(term.lower() in text2 for term in term_group))
                           for term_group in IMPORTANT_TERM_GROUPS):
                        continue

                    # ===================== REVISED SCORING LOGIC =====================
                    # Use synonym check to force a perfect score
                    home_is_synonym = check_team_synonyms(h1, h2)
                    away_is_synonym = check_team_synonyms(a1, a2)

                    home_score = 1.0 if home_is_synonym else difflib.SequenceMatcher(None, get_core_name(h1),
                                                                                     get_core_name(h2)).ratio()
                    away_score = 1.0 if away_is_synonym else difflib.SequenceMatcher(None, get_core_name(a1),
                                                                                     get_core_name(a2)).ratio()
                    # =================================================================

                    # Gatekeeper check
                    if min(home_score, away_score) < GATEKEEPER_THRESHOLD:
                        continue

                    passed = False
                    # Iterate through thresholds in the defined order
                    for s_thresh, m_thresh in zip(STRONG_THRESHOLD, MODERATE_THRESHOLD):
                        if ((home_score >= s_thresh and away_score >= m_thresh) or
                                (away_score >= s_thresh and home_score >= m_thresh)):
                            passed = True
                            break

                    if not passed: continue

                    avg_score = (home_score + away_score) / 2
                    if avg_score > best_score:
                        best_score = avg_score
                        best_match = m2

                if best_match:
                    best_match.setdefault("source", src2)
                    group[src2] = best_match
                    processed[src2].add(f"{best_match['home_team']}|{best_match['away_team']}|"
                                        f"{best_match.get('date', '')}|{best_match.get('time', '')}")

            if len(group) >= 2:
                fuzzy_matches.append(list(group.values()))

    # STEP 3: Combine and return
    exact = [list(g.values()) for g in match_index.values() if len(g) >= 2]
    return exact + fuzzy_matches
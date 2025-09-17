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
TIME_DIFF_TOLERANCE: Set[float] = set()


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


def get_consonants(text: str) -> str:
    """Extracts only the consonant characters from a string."""
    if not text:
        return ""
    # Assuming English/Romance language consonants
    return "".join([c for c in text.lower() if c in 'bcdfghjklmnpqrstvwxyz'])

def calculate_team_similarity(name1: str, name2: str) -> float:
    """
    Calculates a similarity score between 0.0 and 1.0 for two team names,
    using a cascade of checks.
    """
    if not name1 or not name2:
        return 0.0

    # First, strip important terms like 'U20' so they don't interfere
    # with the core logic, but keep the rest of the name for context.
    comp1, comp2 = name1, name2
    all_important_terms = [term for group in IMPORTANT_TERM_GROUPS for term in group]
    for term in all_important_terms:
        pattern = re.compile(r'\b' + re.escape(term) + r'\b', flags=re.IGNORECASE)
        comp1 = pattern.sub("", comp1)
        comp2 = pattern.sub("", comp2)
    comp1, comp2 = comp1.strip(), comp2.strip()

    # --- High-Confidence Checks ---
    if check_team_synonyms(comp1, comp2):
        return 0.95
    if check_abbreviation_match(comp1, comp2):
        return 0.90

    # --- Fallback to Core Name Fuzzy Matching ---
    core1 = get_core_name(name1)
    core2 = get_core_name(name2)

    if not core1 or not core2:
        return 0.0

    # ====================================================================
    # <<< NEW: SPECIAL HANDLING FOR "Al" PREFIXES >>>
    # If both names start with "al", the prefix is not very distinctive.
    # We calculate the ratio on the part *after* the "al".
    # This prevents "Al Sinaah" vs "Al-Hussein" from getting a high score.
    # We use the core_name for this comparison.
    is_al1 = core1.lower().startswith(('al ', 'al-'))
    is_al2 = core2.lower().startswith(('al ', 'al-'))

    if is_al1 and is_al2:
        # Strip the prefix from both and compare the rest of the string.
        remainder1 = re.sub(r'^al[\s-]', '', core1, flags=re.IGNORECASE).strip()
        remainder2 = re.sub(r'^al[\s-]', '', core2, flags=re.IGNORECASE).strip()
        # Only compare if there's something left after stripping.
        if remainder1 and remainder2:
            return difflib.SequenceMatcher(None, remainder1, remainder2).ratio()
    # ====================================================================

    # Original fallback for all other cases
    return difflib.SequenceMatcher(None, core1, core2).ratio()


def check_abbreviation_match(name1: str, name2: str) -> bool:
    """
    Checks if one name contains a likely abbreviation for the other, handling
    two cases:
    1. All-consonant abbreviation: "PR" for "Paranaense" (checks first consonants).
    2. Mixed vowel/consonant: "CA" for "Cascavel" (checks start of word).
    """
    # Use normalized names but don't strip common words yet, as the
    # abbreviation itself might be a common word (e.g., "ca" for "clube atletico").
    words1 = normalize_team_name(name1).split()
    words2 = normalize_team_name(name2).split()

    # This logic is complex, so we wrap it in a helper to run it both ways
    def _check_one_way(short_words: List[str], long_words: List[str]) -> bool:
        for abbr_word in short_words:
            # We are looking for 2- or 3-letter potential abbreviations
            if not (2 <= len(abbr_word) <= 3):
                continue

            # Don't treat purely numeric strings as abbreviations (e.g., '14' from a date)
            if abbr_word.isnumeric():
                continue

            # Case 1: All-consonant abbreviation (e.g., "pr", "cr", "sc")
            # We check if the abbreviation consists of non-vowel characters.
            if all(c not in 'aeiou' for c in abbr_word):
                for full_word in long_words:
                    # The full word should be longer than the abbreviation itself
                    if len(full_word) > len(abbr_word):
                        first_consonants = get_consonants(full_word)
                        if first_consonants.startswith(abbr_word):
                            return True
            # Case 2: Abbreviation contains vowels (e.g., "ca", "pa", "bo")
            else:
                for full_word in long_words:
                    if full_word.startswith(abbr_word):
                        return True
        return False

    # Run the check in both directions (e.g., t1 has abbr for t2, or t2 has abbr for t1)
    return _check_one_way(words1, words2) or _check_one_way(words2, words1)


def simplify_team_name(name: str) -> str:
    """
    Remove common team words and location identifiers, then strip
    typical suffixes like "ienne", "ais", "aise", etc.
    """
    if not name:
        return ""
    n = normalize_team_name(name)
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

    # 1) IMPORTANT‑TERM PRESENCE CHECK (logic is correct)
    t1_lower, t2_lower = t1.lower(), t2.lower()
    for group in IMPORTANT_TERM_GROUPS:
        has1 = any(term.lower() in t1_lower for term in group)
        has2 = any(term.lower() in t2_lower for term in group)
        if has1 ^ has2:
            return False

    # 2) STRIP IMPORTANT TERMS FOR COMPARISON
    comp1, comp2 = t1, t2
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

    # 5) PHONETIC COMPARISON
    p1 = get_phonetic_representation(comp1)
    p2 = get_phonetic_representation(comp2)
    if p1 and p1 == p2:
        return True

    # ======================================================================
    # <<< NEW: 6) ABBREVIATION CHECK >>>
    # This is the perfect place for your new logic. It will catch things
    # like "Athletico PR" vs "CA Paranaense" before falling back to fuzzy logic.
    if check_abbreviation_match(comp1, comp2):
        return True
    # ======================================================================

    # 7) FUZZY & WORD-BASED CHECKS (rest of the function)
    if fuzzy_match(n1, n2):
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
    """
    Given a dict mapping source_name → list of match dicts,
    return a list of groups. Each group is a list of match dicts
    (from different sources) referring to the same fixture.
    """
    sources = sorted(matches_by_source.keys())
    match_index: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # STEP 1: Exact-signature indexing
    for source in sources:
        for match in matches_by_source[source]:
            home = match.get("home_team", "")
            away = match.get("away_team", "")
            # skip combined entries
            if "/" in home and "/" in away:
                continue

            sig = (
                f"{normalize_team_name(home)}|"
                f"{normalize_team_name(away)}|"
                f"{match.get('date', '')}|"
                f"{match.get('time', '').strip()}"
            )
            match.setdefault("source", source)
            match_index.setdefault(sig, {})[source] = match

    # STEP 2: Fuzzy matching for incomplete groups
    fuzzy_matches: List[List[Dict[str, Any]]] = []
    processed: Dict[str, Set[str]] = {src: set() for src in sources}

    for src1 in sources:
        for m1 in matches_by_source[src1]:
            h1, a1 = m1.get("home_team", ""), m1.get("away_team", "")
            if "/" in h1 and "/" in a1:
                continue

            sig1 = (
                f"{normalize_team_name(h1)}|"
                f"{normalize_team_name(a1)}|"
                f"{m1.get('date', '')}|"
                f"{m1.get('time', '').strip()}"
            )
            if sig1 in match_index and len(match_index[sig1]) == len(sources):
                continue

            id1 = f"{h1}|{a1}|{m1.get('date', '')}|{m1.get('time', '')}"
            if id1 in processed[src1]:
                continue

            processed[src1].add(id1)
            group = {src1: m1}

            for src2 in sources:
                if src2 == src1:
                    continue

                best_match = None
                best_score = 0.0

                for m2 in matches_by_source[src2]:
                    # ─── IMPORTANT‑TERM GUARD (UPDATED LOGIC) ───
                    text1 = (m1.get("home_team", "") + " " + m1.get("away_team", "")).lower()
                    text2 = (m2.get("home_team", "") + " " + m2.get("away_team", "")).lower()

                    is_mismatch = False
                    for term_group in IMPORTANT_TERM_GROUPS:
                        found1 = any(term.lower() in text1 for term in term_group)
                        found2 = any(term.lower() in text2 for term in term_group)
                        if found1 ^ found2:
                            is_mismatch = True
                            break
                    if is_mismatch:
                        continue
                    # ──────────────────────────────────────────────

                    h2, a2 = m2.get("home_team", ""), m2.get("away_team", "")
                    if "/" in h2 and "/" in a2:
                        continue

                    id2 = f"{h2}|{a2}|{m2.get('date', '')}|{m2.get('time', '')}"
                    if id2 in processed[src2]:
                        continue

                    d1 = parse_date(m1.get("date", ""))
                    d2 = parse_date(m2.get("date", ""))
                    date_match = bool(d1 and d2 and d1 == d2)

                    t1, t2 = m1.get("time", "").strip(), m2.get("time", "").strip()
                    try:
                        # Your original time logic
                        fmt = "%H:%M"
                        dt1 = datetime.strptime(t1, fmt)
                        dt2 = datetime.strptime(t2, fmt)
                        diff_min = abs((dt1 - dt2).total_seconds()) / 60
                        # Make sure TIME_DIFF_TOLERANCE is a single value, not a set
                        time_match = diff_min <= list(TIME_DIFF_TOLERANCE)[0]
                    except:
                        time_match = (t1 == t2)

                    if not (date_match and time_match):
                        continue

                    # ===================== NEW ROBUST SCORING LOGIC =====================
                    # Calculate similarity for home and away teams.
                    home_score = calculate_team_similarity(h1, h2)
                    away_score = calculate_team_similarity(a1, a2)

                    # We now use multiplicative scoring. This ensures that a single
                    # poor match (e.g., home_score = 0.2) severely penalizes
                    # the total score, even if the other match is perfect (away_score = 1.0).
                    # 0.2 * 1.0 = 0.2, which is a much better reflection of reality.
                    combined_score = home_score * away_score

                    # We replace the complex gatekeepers and thresholds with a single,
                    # clear check against the combined score.
                    # A threshold of 0.65 means that both scores must be reasonably high
                    # (e.g., 0.8 * 0.82 > 0.65). A perfect match on one side and a weak
                    # match on the other (e.g., 1.0 * 0.6) will fail.
                    MATCH_THRESHOLD = 0.65
                    if combined_score < MATCH_THRESHOLD:
                        continue

                    # The rest of the logic finds the best match above this threshold.
                    if combined_score > best_score:
                        best_score = combined_score
                        best_match = m2

                if best_match:
                    best_match.setdefault("source", src2)
                    group[src2] = best_match
                    processed[src2].add(
                        f"{best_match['home_team']}|"
                        f"{best_match['away_team']}|"
                        f"{best_match.get('date', '')}|"
                        f"{best_match.get('time', '')}"
                    )

            if len(group) >= 2:
                fuzzy_matches.append(list(group.values()))

    # STEP 3: Add exact matches that appear in ≥2 sources
    exact = [list(g.values()) for g in match_index.values() if len(g) >= 2]

    return exact + fuzzy_matches
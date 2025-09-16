# matcher.py

import re
import unicodedata
import difflib
from datetime import datetime
from typing import Dict, List, Set, Any, Optional

# --- Important terms placeholder (populated in main.py) ---
IMPORTANT_TERMS: Set[str] = set()

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
    is_arabic = any(name.lower().startswith(("al ", "al-")) for name in (a, b))

    if threshold is None:
        threshold = FUZZY_THRESHOLD_ARABIC if is_arabic else FUZZY_THRESHOLD

    return difflib.SequenceMatcher(None, a_lower, b_lower).ratio() >= threshold


def teams_match(t1: str, t2: str) -> bool:
    """
    Decide if two team-name strings refer to the same team, using:
    1) Exact match on normalize_team_name
    2) Exact match on get_canonical_name
    3) Exact match on get_phonetic_representation
    4) Fuzzy match on normalized names
    5) Fuzzy match on phonetic representations (if >5 chars)
    6) Single-vs-multi-word fuzzy
    7) Very short name loose fuzzy
    8) check_team_synonyms
    9) Simplified names exact/fuzzy
    10) Significant-word overlap (raw or phonetic)
    """
    if not t1 or not t2:
        return False

    # ─── IMPORTANT‑TERM CHECK ───
    # If one name contains a required tag (e.g. "U21") that the other lacks, they don't match.
    for term in IMPORTANT_TERMS:
        has1 = term.lower() in t1.lower()
        has2 = term.lower() in t2.lower()
        if has1 ^ has2:
            return False
    # ───────────────────────────────

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
    s1 = simplify_team_name(t1)
    s2 = simplify_team_name(t2)
    if s1 and s2 and (s1 == s2 or fuzzy_match(s1, s2)):
        return True

    # 8) Significant‑word overlap
    sig1 = extract_significant_words(t1)
    sig2 = extract_significant_words(t2)
    if sig1 and sig2:
        # If both single‑word sets, compare them directly
        if len(sig1) == len(sig2) == 1:
            w1_clean = re.sub(r"[^a-z0-9]", "", next(iter(sig1)))
            w2_clean = re.sub(r"[^a-z0-9]", "", next(iter(sig2)))
            if w1_clean == w2_clean or fuzzy_match(w1_clean, w2_clean, threshold=0.8):
                return True
            if get_phonetic_representation(w1_clean) == get_phonetic_representation(w2_clean):
                return True
        # Otherwise require >50% overlap (normal or phonetic)
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
            # ── SKIP any "combination" entry ──
            home = match.get("home_team", "")
            away = match.get("away_team", "")
            if "/" in home and "/" in away:
                continue

            home_norm = normalize_team_name(home)
            away_norm = normalize_team_name(away)
            date_str = match.get("date", "")
            time_str = match.get("time", "").strip()

            sig = f"{home_norm}|{away_norm}|{date_str}|{time_str}"
            if sig not in match_index:
                match_index[sig] = {}
            match["source"] = source
            match_index[sig][source] = match

    # STEP 2: Fuzzy matching for incomplete groups
    fuzzy_matches: List[List[Dict[str, Any]]] = []
    processed_matches: Dict[str, Set[str]] = {src: set() for src in sources}

    for source1 in sources:
        for match1 in matches_by_source[source1]:
            # ── SKIP if match1 is a combination entry ──
            h1 = match1.get("home_team", "")
            a1 = match1.get("away_team", "")
            if "/" in h1 and "/" in a1:
                continue

            sig1 = (
                f"{normalize_team_name(h1)}|"
                f"{normalize_team_name(a1)}|"
                f"{match1.get('date','')}|"
                f"{match1.get('time','').strip()}"
            )
            # Skip if this exact signature already covers all sources
            if sig1 in match_index and len(match_index[sig1]) == len(sources):
                continue

            id1 = f"{h1}|{a1}|{match1.get('date','')}|{match1.get('time','')}"
            if id1 in processed_matches[source1]:
                continue

            fuzzy_group: Dict[str, Dict[str, Any]] = {source1: match1}
            processed_matches[source1].add(id1)

            # Try to match match1 against every other source
            for source2 in sources:
                if source2 == source1:
                    continue

                best_match = None
                best_score = 0.0

                for match2 in matches_by_source[source2]:
                    # ── SKIP if match2 is a combination entry ──
                    h2 = match2.get("home_team", "")
                    a2 = match2.get("away_team", "")
                    if "/" in h2 and "/" in a2:
                        continue

                    id2 = f"{h2}|{a2}|{match2.get('date','')}|{match2.get('time','')}"
                    if id2 in processed_matches[source2]:
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
                            h1_simple = simplify_team_name(match1.get("home_team", ""))
                            a1_simple = simplify_team_name(match1.get("away_team", ""))
                            h2_simple = simplify_team_name(match2.get("home_team", ""))
                            a2_simple = simplify_team_name(match2.get("away_team", ""))

                            home_score = difflib.SequenceMatcher(None, h1_simple, h2_simple).ratio()
                            away_score = difflib.SequenceMatcher(None, a1_simple, a2_simple).ratio()
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

    # STEP 3: Collect exact matches that appear in ≥ 2 sources
    exact_matches: List[List[Dict[str, Any]]] = []
    for sig, group_dict in match_index.items():
        if len(group_dict) >= 2:
            exact_matches.append(list(group_dict.values()))

    return exact_matches + fuzzy_matches
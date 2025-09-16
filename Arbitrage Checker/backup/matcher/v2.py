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
    1) Important-term presence check
    2) Exact match on normalized names
    3) Exact match on canonical names
    4) Exact match on phonetic forms
    5) Fuzzy/phonetic and other heuristics
    """
    if not t1 or not t2:
        return False

    # 1) IMPORTANT‑TERM PRESENCE CHECK
    # Ensure that if one name contains a term, the other must too.
    for term in IMPORTANT_TERMS:
        has1 = term.lower() in t1.lower()
        has2 = term.lower() in t2.lower()
        if has1 ^ has2:
            return False

    # 2) STRIP IMPORTANT TERMS FOR COMPARISON ONLY
    # Use local copies so original t1/t2 remain intact.
    comp1, comp2 = t1, t2
    for term in IMPORTANT_TERMS:
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

    # 5) PHONETIC REPRESENTATION COMPARISON
    p1 = get_phonetic_representation(comp1)
    p2 = get_phonetic_representation(comp2)
    if p1 and p1 == p2:
        return True

    # 6) FUZZY MATCH ON NORMALIZED
    if fuzzy_match(n1, n2):
        return True

    # 7) FUZZY MATCH ON PHONETIC (long names)
    if len(p1) > 5 and len(p2) > 5 and fuzzy_match(p1, p2):
        return True

    # 8) SINGLE‑VS‑MULTI‑WORD FUZZY
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

    # 9) VERY‑SHORT‑NAME LOOSE FUZZY
    if len(n1) <= 5 or len(n2) <= 5:
        if fuzzy_match(n1, n2, threshold=0.5):
            return True

    # 10) KNOWN SYNONYMS
    if check_team_synonyms(t1, t2):
        return True

    # 11) SIMPLIFIED NAME + FUZZY
    s1 = simplify_team_name(comp1)
    s2 = simplify_team_name(comp2)
    if s1 and s2 and (s1 == s2 or fuzzy_match(s1, s2)):
        return True

    # 12) SIGNIFICANT‑WORD OVERLAP
    sig1 = extract_significant_words(comp1)
    sig2 = extract_significant_words(comp2)
    if sig1 and sig2:
        # single-word case
        if len(sig1) == len(sig2) == 1:
            w1_clean = re.sub(r"[^a-z0-9]", "", next(iter(sig1)))
            w2_clean = re.sub(r"[^a-z0-9]", "", next(iter(sig2)))
            if w1_clean == w2_clean or fuzzy_match(w1_clean, w2_clean, threshold=0.8):
                return True
            if get_phonetic_representation(w1_clean) == get_phonetic_representation(w2_clean):
                return True
        # multi-word overlap
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
                f"{match.get('date','')}|"
                f"{match.get('time','').strip()}"
            )
            match.setdefault("source", source)
            match_index.setdefault(sig, {})[source] = match

    # STEP 2: Fuzzy matching for incomplete groups
    fuzzy_matches: List[List[Dict[str, Any]]] = []
    processed: Dict[str, Set[str]] = {src: set() for src in sources}

    for src1 in sources:
        for m1 in matches_by_source[src1]:
            h1, a1 = m1.get("home_team",""), m1.get("away_team","")
            if "/" in h1 and "/" in a1:
                continue

            sig1 = (
                f"{normalize_team_name(h1)}|"
                f"{normalize_team_name(a1)}|"
                f"{m1.get('date','')}|"
                f"{m1.get('time','').strip()}"
            )
            # if exact group already full, skip
            if sig1 in match_index and len(match_index[sig1]) == len(sources):
                continue

            id1 = f"{h1}|{a1}|{m1.get('date','')}|{m1.get('time','')}"
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
                    # ─── IMPORTANT‑TERM GUARD ───
                    # If m1 has U23 (etc.) and m2 doesn’t—or vice versa—skip immediately.
                    for term in IMPORTANT_TERMS:
                        has1 = term.lower() in m1["home_team"].lower() or term.lower() in m1["away_team"].lower()
                        has2 = term.lower() in m2["home_team"].lower() or term.lower() in m2["away_team"].lower()
                        if has1 ^ has2:
                            break
                    else:
                        # no break → passed important-term check, continue with date/time and scoring
                        pass
                    # if break happened, skip this candidate:
                    if any(term.lower() in m1["home_team"].lower() + m1["away_team"].lower() and
                        term.lower() not in m2["home_team"].lower() + m2["away_team"].lower()
                        for term in IMPORTANT_TERMS):
                        continue
                    # ────────────────────────────────
                    h2, a2 = m2.get("home_team",""), m2.get("away_team","")
                    if "/" in h2 and "/" in a2:
                        continue

                    id2 = f"{h2}|{a2}|{m2.get('date','')}|{m2.get('time','')}"
                    if id2 in processed[src2]:
                        continue

                    # date tolerance ±1 day
                    d1 = parse_date(m1.get("date",""))
                    d2 = parse_date(m2.get("date",""))
                    date_match = (d1 and d2 and abs((d1 - d2).days) <= 1) \
                                 or m1.get("date","") == m2.get("date","")

                    # time tolerance
                    t1, t2 = m1.get("time","").strip(), m2.get("time","").strip()
                    try:
                        fmt = "%H:%M"
                        dt1 = datetime.strptime(t1, fmt)
                        dt2 = datetime.strptime(t2, fmt)
                        diff_min = abs((dt1 - dt2).total_seconds())/60
                        time_match = diff_min <= TIME_DIFF_TOLERANCE
                    except:
                        time_match = (t1 == t2)

                    if not(date_match and time_match):
                        continue

                    # compute per‑leg similarity
                    home_score = difflib.SequenceMatcher(
                        None,
                        simplify_team_name(h1),
                        simplify_team_name(h2)
                    ).ratio()
                    away_score = difflib.SequenceMatcher(
                        None,
                        simplify_team_name(a1),
                        simplify_team_name(a2)
                    ).ratio()

                    # require both legs pass a minimum threshold
                    if not (
                            (home_score >= 0.75 and away_score >= 0.465)
                            or (away_score >= 0.75 and home_score >= 0.465)
                    ):
                        continue

                    avg_score = (home_score + away_score) / 2
                    if avg_score > best_score:
                        best_score = avg_score
                        best_match = m2

                if best_match:
                    best_match.setdefault("source", src2)
                    group[src2] = best_match
                    processed[src2].add(
                        f"{best_match['home_team']}|"
                        f"{best_match['away_team']}|"
                        f"{best_match.get('date','')}|"
                        f"{best_match.get('time','')}"
                    )

            if len(group) >= 2:
                fuzzy_matches.append(list(group.values()))

    # STEP 3: Add exact matches that appear in ≥2 sources
    exact = [list(g.values()) for g in match_index.values() if len(g) >= 2]

    return exact + fuzzy_matches
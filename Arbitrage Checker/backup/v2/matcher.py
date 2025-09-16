# matcher.py

import re
import unicodedata
import difflib
from datetime import datetime, timedelta
from typing import Dict, List, Set, Any, Optional

# --- Placeholders populated by main.py ---
IMPORTANT_TERM_GROUPS: List[List[str]] = []
SYN_PRIMARY: Dict[str, str] = {}
SYN_GROUPS: List[List[str]] = []
COMMON_TEAM_WORDS: Set[str] = set()
LOCATION_IDENTIFIERS: Set[str] = set()
TEAM_SYNONYMS: List[Set[str]] = []

# --- Configurable values with defaults ---
STRONG_THRESHOLD: float = 0.9
MODERATE_THRESHOLD: float = 0.65
TIME_DIFF_TOLERANCE: int = 65
GATEKEEPER_THRESHOLD: float = 0.3
DAY_DIFF_TOLERANCE: int = 1

# --- Pre-compiled Regexes for Performance ---
_RE_PARENTHETICALS = re.compile(r"\([^)]*\)")
_RE_NON_WORD_CHARS = re.compile(r"[^\w\s]")
_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_ALPHANUM_ONLY = re.compile(r"[^a-z0-9]")
_RE_NATIONALITY_SUFFIX = re.compile(r"(ienne|ien|aise|ais|oise|ois|ine|in|é)$")

_PHONETIC_SUBS = [
    (re.compile(r"k['`\-\s]*un"), "kun"),
    (re.compile(r"j['`\-\s]*in"), "jin"),
    (re.compile(r"zh['`\-\s]*ou"), "zhou"),
    (re.compile(r"([aeiou])['`]"), r"\1"),
    (re.compile(r"\bsaint\b"), "st"),
    (re.compile(r"\bfc\b"), ""),
    (re.compile(r"[\s\-]+"), ""),
]

# --- Regexes populated by the initializer ---
_RE_ROMAN_NUMERALS = None
_RE_SIMPLIFY_STRIP = None
_RE_CORE_NAME_STRIP = None


def initialize_matcher_globals():
    """
    One-time setup to create combined, pre-compiled regexes from global lists.
    MUST be called from main.py after populating the global lists.
    """
    global _RE_ROMAN_NUMERALS, _RE_SIMPLIFY_STRIP, _RE_CORE_NAME_STRIP, TEAM_SYNONYMS, SYN_PRIMARY

    roman_numerals = [
        "XVIII", "XVII", "XVI", "XIII", "XIV", "XII", "XIX", "XV",
        "VIII", "VII", "III", "IV", "IX", "VI", "XI", "XX", "II"
    ]
    _RE_ROMAN_NUMERALS = re.compile(r'\b(' + '|'.join(roman_numerals) + r')\b', re.IGNORECASE)

    simplify_strip_words = COMMON_TEAM_WORDS.union(LOCATION_IDENTIFIERS)
    if simplify_strip_words:
        pattern_str = r'\b(' + '|'.join(re.escape(w) for w in simplify_strip_words) + r')\b'
        _RE_SIMPLIFY_STRIP = re.compile(pattern_str, re.IGNORECASE)

    all_important_terms = {term.lower() for group in IMPORTANT_TERM_GROUPS for term in group}
    core_strip_words = simplify_strip_words.union(all_important_terms)
    if core_strip_words:
        pattern_str = r'\b(' + '|'.join(re.escape(w) for w in core_strip_words) + r')\b'
        _RE_CORE_NAME_STRIP = re.compile(pattern_str, re.IGNORECASE)

    TEAM_SYNONYMS = [set(g) for g in TEAM_SYNONYMS]

    temp_syn_primary = {}
    for group in SYN_GROUPS:
        if group:
            primary = group[0]
            for syn in group:
                temp_syn_primary[syn] = primary
    SYN_PRIMARY.update(temp_syn_primary)


def remove_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if not unicodedata.combining(c)) if text else ""


def normalize_team_name(name: str) -> str:
    if not name: return ""
    n = remove_accents(name.lower())
    n = _RE_PARENTHETICALS.sub("", n)
    n = _RE_NON_WORD_CHARS.sub(" ", n)
    n = _RE_MULTI_SPACE.sub(" ", n)
    return n.strip()


def get_canonical_name(name: str) -> str:
    return _RE_ALPHANUM_ONLY.sub("", normalize_team_name(name)) if name else ""


def canonical(base_name: str) -> str:
    base = base_name[:-5] if base_name.lower().endswith(".json") else base_name
    if base in SYN_PRIMARY: return SYN_PRIMARY[base]
    for group in SYN_GROUPS:
        primary = group[0]
        if any(syn.lower() in base.lower() for syn in group[1:]): return primary
    return base


def get_phonetic_representation(name: str) -> str:
    if not name: return ""
    n = normalize_team_name(name)
    for pattern, replacement in _PHONETIC_SUBS:
        n = pattern.sub(replacement, n)
    return n


# --- Compatibility Functions (for test_team_matching) ---

def simplify_team_name(name: str) -> str:
    if not name: return ""
    n = normalize_team_name(name)
    if _RE_ROMAN_NUMERALS: n = _RE_ROMAN_NUMERALS.sub("", n)
    if _RE_SIMPLIFY_STRIP: n = _RE_SIMPLIFY_STRIP.sub("", n)
    n = _RE_NATIONALITY_SUFFIX.sub("", n)
    return _RE_MULTI_SPACE.sub(" ", n).strip()


def extract_significant_words(name: str) -> Set[str]:
    if not name: return set()
    simplified = simplify_team_name(name)
    return {w for w in simplified.split() if len(w) > 2}


def fuzzy_match(a: str, b: str, threshold: float = 0.85) -> bool:
    if not a or not b: return False
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio() >= threshold


def teams_match(t1: str, t2: str) -> bool:
    """Compatibility function for test suites. The main logic uses a more integrated approach."""
    if not t1 or not t2: return False

    # Check important term consistency
    t1_terms = {idx for idx, grp in enumerate(IMPORTANT_TERM_GROUPS) if any(term.lower() in t1.lower() for term in grp)}
    t2_terms = {idx for idx, grp in enumerate(IMPORTANT_TERM_GROUPS) if any(term.lower() in t2.lower() for term in grp)}
    if t1_terms != t2_terms: return False

    # Strip important terms for core comparison
    comp1, comp2 = t1, t2
    if _RE_CORE_NAME_STRIP:
        comp1 = _RE_CORE_NAME_STRIP.sub("", comp1)
        comp2 = _RE_CORE_NAME_STRIP.sub("", comp2)

    # Run a series of checks
    n1, n2 = normalize_team_name(comp1), normalize_team_name(comp2)
    if n1 == n2: return True
    if get_canonical_name(comp1) == get_canonical_name(comp2): return True
    if fuzzy_match(n1, n2): return True

    s1, s2 = simplify_team_name(comp1), simplify_team_name(comp2)
    if s1 and s2 and (s1 == s2 or fuzzy_match(s1, s2, 0.9)): return True

    if check_team_synonyms(normalize_team_name(t1), normalize_team_name(t2)): return True

    return False


# --- Core Logic Functions ---

def get_core_name_words(name: str) -> Set[str]:
    """Optimized function to get the set of core words of a team name."""
    if not name: return set()
    n = normalize_team_name(name)
    if _RE_ROMAN_NUMERALS: n = _RE_ROMAN_NUMERALS.sub("", n)
    if _RE_CORE_NAME_STRIP: n = _RE_CORE_NAME_STRIP.sub("", n)
    n = _RE_NATIONALITY_SUFFIX.sub("", n)
    n = _RE_MULTI_SPACE.sub(" ", n).strip()
    return set(n.split()) if n else set()


def check_team_synonyms(n1: str, n2: str) -> bool:
    """Return True if both normalized names contain any synonym from the same group."""
    for synonym_group in TEAM_SYNONYMS:
        if any(syn in n1 for syn in synonym_group) and any(syn in n2 for syn in synonym_group):
            return True
    return False


def calculate_jaccard_score(set1: Set[str], set2: Set[str]) -> float:
    """Calculates Jaccard similarity between two pre-computed sets of words."""
    if not set1 and not set2: return 1.0
    if not set1 or not set2: return 0.0
    intersection_len = len(set1.intersection(set2))
    if intersection_len == 0: return 0.0
    union_len = len(set1) + len(set2) - intersection_len
    return intersection_len / union_len


def parse_date(date_str: str) -> Optional[datetime.date]:
    if not date_str: return None
    s = date_str.strip()
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%d.%m.%Y"]:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def find_all_matching_matches(matches_by_source: Dict[str, List[Dict[str, Any]]]) -> List[List[Dict[str, Any]]]:
    """Finds matching groups using pre-computation and date-based indexing for performance."""
    all_matches, matches_by_date = [], {}

    # 1. Pre-computation: Enrich each match object with computed data once
    for source, matches in matches_by_source.items():
        for match in matches:
            match['source'] = source
            home_team, away_team = match.get('home_team', ''), match.get('away_team', '')
            match['_parsed_date'] = parse_date(match.get('date', ''))
            try:
                match['_parsed_time'] = datetime.strptime(match.get('time', '').strip(), "%H:%M")
            except ValueError:
                match['_parsed_time'] = None

            match['_norm_home'], match['_norm_away'] = normalize_team_name(home_team), normalize_team_name(away_team)
            match['_core_home_words'], match['_core_away_words'] = get_core_name_words(home_team), get_core_name_words(
                away_team)

            text = (home_team + " " + away_team).lower()
            match['_important_terms'] = {idx for idx, grp in enumerate(IMPORTANT_TERM_GROUPS) if
                                         any(term.lower() in text for term in grp)}

            all_matches.append(match)
            if match['_parsed_date']:
                matches_by_date.setdefault(match['_parsed_date'], []).append(match)

    # 2. Matching Loop with Date Indexing
    processed_ids, groups = set(), []
    for m1 in all_matches:
        m1_id = f"{m1['source']}-{m1.get('match_id')}"
        if m1_id in processed_ids or not m1['_parsed_date']: continue
        processed_ids.add(m1_id)

        candidate_matches = []
        for i in range(-DAY_DIFF_TOLERANCE, DAY_DIFF_TOLERANCE + 1):
            check_date = m1['_parsed_date'] + timedelta(days=i)
            candidate_matches.extend(matches_by_date.get(check_date, []))

        best_matches_for_m1 = {}
        for m2 in candidate_matches:
            m2_id = f"{m2['source']}-{m2.get('match_id')}"
            if m1_id == m2_id or m2_id in processed_ids: continue

            if m1['_important_terms'] != m2['_important_terms']: continue

            if m1['_parsed_time'] and m2['_parsed_time']:
                if abs((m1['_parsed_time'] - m2['_parsed_time']).total_seconds() / 60) > TIME_DIFF_TOLERANCE: continue
            elif m1.get('time', '').strip() != m2.get('time', '').strip():
                continue

            home_score = 1.0 if check_team_synonyms(m1['_norm_home'], m2['_norm_home']) else calculate_jaccard_score(
                m1['_core_home_words'], m2['_core_home_words'])
            away_score = 1.0 if check_team_synonyms(m1['_norm_away'], m2['_norm_away']) else calculate_jaccard_score(
                m1['_core_away_words'], m2['_core_away_words'])

            if min(home_score, away_score) < GATEKEEPER_THRESHOLD: continue
            if not ((home_score >= STRONG_THRESHOLD and away_score >= MODERATE_THRESHOLD) or \
                    (away_score >= STRONG_THRESHOLD and home_score >= MODERATE_THRESHOLD)): continue

            avg_score = (home_score + away_score) / 2
            existing_best_score = best_matches_for_m1.get(m2['source'], (None, 0.0))[1]
            if avg_score > existing_best_score:
                best_matches_for_m1[m2['source']] = (m2, avg_score)

        current_group = [m1]
        for best_match, score in best_matches_for_m1.values():
            best_match_id = f"{best_match['source']}-{best_match.get('match_id')}"
            if best_match_id not in processed_ids:
                current_group.append(best_match)
                processed_ids.add(best_match_id)

        if len(current_group) > 1:
            groups.append(current_group)

    # 3. Final Annotation
    for group in groups:
        sorted_ids = sorted([str(m.get('match_id')) for m in group], key=lambda x: (-len(x), x))
        group_id = "-".join(sorted_ids)
        for m in group:
            m['matching_group_id'] = group_id
            for key in list(m.keys()):
                if key.startswith('_'): del m[key]

    return groups
# arb_calculator.py

import re
import itertools
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import quote

# ─── Placeholder for football market sets ───────────────────────────────────────
# `main.py` will assign this to the "market_sets" dict loaded from {SPORT}/markets.json.
MARKET_SETS: Dict[str, List[str]] = {}

# ─── Placeholders for URL building ───────────────────────────────────────────────
# `main.py` will load these from settings/url_builder.json
URL_TEMPLATES: Dict[str, Any] = {}
SPORT_NAME: str = ""
MODE_NAME: str = ""

# ─── Global for Market Categories ──────────────────────────────────────────
MARKET_CATEGORIES: Dict[str, str] = {}


# ─── Function to Build Market Categories ──────────────────────────────────
def build_market_categories():
    """
    Pre-computes a mapping from any odd key to its general market category.
    NOTE: Call this function from `main.py` after `MARKET_SETS` is loaded.
    """
    global MARKET_CATEGORIES
    if MARKET_CATEGORIES:
        return

    cat_map = {}
    for set_name, keys in MARKET_SETS.items():
        category = None
        if "under_" in set_name or "over_" in set_name:
            category = "totals"
        elif "ah_" in set_name:
            category = "handicap"
        elif set_name == "three_way":
            category = "3-way"
        elif set_name in ["one_vs_x2", "two_vs_1x", "x_vs_12"]:
            category = "double-chance"
        elif set_name == "both_score":
            category = "btts"

        if category:
            for key in keys:
                cat_map[key] = category
    MARKET_CATEGORIES = cat_map


# ──────────────────────────────────────────────────────────────────────────────
# slugify function remains unchanged
def slugify(text: str, rules: Dict[str, Any]) -> str:
    """
    Applies a set of rules to transform a string into a URL-friendly slug.
    """
    if not isinstance(text, str):
        return ""
    if rules.get("remove_digits"):
        text = "".join(c for c in text if not c.isdigit())
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text).strip()
    space_replacement = rules.get("space_replacement", "-")
    text = re.sub(r'[\s_]+', space_replacement, text)
    return text


# pick_best_odds function remains unchanged
def pick_best_odds(matches, key):
    """
    Pick the best odd across all matches for a specific market
    Returns the best odd value, its source, and the match ID
    """
    best_value = 0
    best_source = None
    best_match_id = None
    for match in matches:
        try:
            value_raw = match.get(key, "0")
            if isinstance(value_raw, (int, float)):
                value = float(value_raw)
            elif isinstance(value_raw, str) and value_raw.strip():
                value = float(value_raw)
            else:
                continue
            if value > best_value:
                best_value = value
                best_source = match.get("source")
                best_match_id = match
        except (ValueError, TypeError) as e:
            print(f"Error parsing odd {key} from match {match.get('home_team')} vs {match.get('away_team')}: {e}")
            continue
    return best_value, best_source, best_match_id


# check_arbitrage function remains unchanged
def check_arbitrage(odds):
    """Check if there's an arbitrage opportunity"""
    if any(v <= 0 for v, _ in odds.values()):
        return None
    sources = {src for _, src in odds.values()}
    if len(sources) < 2:
        return None
    total = sum(1 / float(v) for v, _ in odds.values())
    return total if total < 1 else None


def _identify_misvalue_source(
    opportunity: Dict,
    all_matches_in_group: List[Dict]
) -> Optional[str]:
    """
    Identifies an outlier source using a weighted, multi-category scoring system.
    It checks common odds from 'totals', 'handicap', and '3-way' markets,
    giving extra weight to the odd that caused the arbitrage.
    """
    # Constants
    ARB_ODD_WEIGHT = 1.5
    TARGET_CATEGORIES = ['totals', 'handicap', '3-way']

    # Pre-flight checks
    unique_sources_in_group = {m.get("source") for m in all_matches_in_group if m.get("source")}
    if len(unique_sources_in_group) < 3:
        return None

    # 1. Build a map of all available odds from all sources
    all_odds_map: Dict[str, List[Tuple[str, float]]] = {}
    for match in all_matches_in_group:
        source = match.get("source")
        if not source: continue
        for key, value in match.items():
            if key.endswith("_odd"):
                try:
                    odd_val = float(value)
                    if odd_val > 0:
                        all_odds_map.setdefault(key, []).append((source, odd_val))
                except (ValueError, TypeError):
                    continue

    # 2. Find all odds that are "common" (offered by 3+ sources)
    common_odds_keys = {k for k, v in all_odds_map.items() if len(v) >= 3}
    if not common_odds_keys:
        return None

    # 3. Select the best odds to use for comparison from different categories
    selected_keys_for_scoring = []
    used_categories = set()
    arbitrage_odd_keys = set(opportunity['best_odds'].keys())

    # Priority 1: Use the actual arbitrage odd if it's common
    for key in arbitrage_odd_keys:
        if key in common_odds_keys:
            selected_keys_for_scoring.append(key)
            category = MARKET_CATEGORIES.get(key)
            if category:
                used_categories.add(category)
            break # Only add one of the arb odds

    # Priority 2: Fill other categories with a common odd
    for category in TARGET_CATEGORIES:
        if category not in used_categories:
            for key in common_odds_keys:
                if MARKET_CATEGORIES.get(key) == category and key not in selected_keys_for_scoring:
                    selected_keys_for_scoring.append(key)
                    used_categories.add(category)
                    break # Found one for this category, move to the next

    if not selected_keys_for_scoring:
        return None

    # 4. Calculate a weighted deviation score for each source
    source_scores = defaultdict(float)
    for key in selected_keys_for_scoring:
        # Get probability data (1/odd) for the current odd key
        prob_data = [(source, 1 / odd) for source, odd in all_odds_map[key] if odd > 0]

        # Determine weight for this odd's score
        weight = ARB_ODD_WEIGHT if key in arbitrage_odd_keys else 1.0

        # For each source, calculate its deviation from the consensus of others
        for i in range(len(prob_data)):
            source_to_check, prob_to_check = prob_data[i]
            other_probs = [p[1] for j, p in enumerate(prob_data) if i != j]

            if not other_probs:
                continue

            avg_other_probs = sum(other_probs) / len(other_probs)
            deviation = abs(prob_to_check - avg_other_probs)
            source_scores[source_to_check] += deviation * weight

    # 5. The source with the highest total score is the most likely outlier
    if not source_scores:
        return None

    return max(source_scores, key=source_scores.get)


def _find_best_arb_for_combination(
        matches_in_combination: List[Dict],
        sources_to_check: Tuple[str],
        all_matches_in_group: List[Dict]
) -> Optional[Dict]:
    """
    Finds the single best arbitrage opportunity for a specific combination of sources.
    """
    best_opportunity = None
    best_arb_percentage = 1.0

    for name, keys in MARKET_SETS.items():
        if any(all(not match.get(k) or str(match.get(k)).strip() == "" for match in matches_in_combination) for k in
               keys):
            continue

        best_odds_with_details = {k: pick_best_odds(matches_in_combination, k) for k in keys}
        odds_for_check = {k: (v, s) for k, (v, s, _) in best_odds_with_details.items()}

        arb = check_arbitrage(odds_for_check)
        if arb is not None and arb < best_arb_percentage:
            formatted_odds = {k: {"value": v, "source": s} for k, (v, s, _) in best_odds_with_details.items()}

            source_to_match_map = {}
            arbitrage_match_ids = set()
            for k, (v, s, match_obj) in best_odds_with_details.items():
                if v > 0 and s and match_obj:
                    source_to_match_map[s] = match_obj
                    if match_obj.get("match_id"):
                        arbitrage_match_ids.add(str(match_obj.get("match_id")))

            arbitrage_sources_set = set(source_to_match_map.keys())
            arbitrage_sources_str = ", ".join(sorted(list(arbitrage_sources_set)))

            sorted_match_ids = sorted(list(arbitrage_match_ids), key=int, reverse=True)
            unique_id = "-".join(sorted_match_ids)

            opportunity = {
                "complementary_set": name,
                "best_odds": formatted_odds,
                "arbitrage_percentage": round(arb, 4),
                "arbitrage_sources": arbitrage_sources_str,
                "unique_id": unique_id
            }

            # --- IDENTIFY MISVALUED SOURCE ---
            misvalue_source = _identify_misvalue_source(opportunity, all_matches_in_group)
            if misvalue_source:
                opportunity['misvalue_source'] = misvalue_source
            # ---------------------------------

            for source, original_match in source_to_match_map.items():
                opportunity[f"{source}_country_name"] = original_match.get("country")
                opportunity[f"tournament_{source}"] = original_match.get("tournament_name")

                match_id_val = original_match.get("match_id")
                tourn_id_val = original_match.get("tournament_id")
                tourn_name_val = original_match.get("tournament_name")

                if match_id_val is not None:
                    try:
                        opportunity[f"{source}_match_id"] = int(match_id_val)
                    except (ValueError, TypeError):
                        opportunity[f"{source}_match_id"] = str(match_id_val)
                if tourn_id_val is not None:
                    try:
                        opportunity[f"{source}_tournament_id"] = int(tourn_id_val)
                    except (ValueError, TypeError):
                        opportunity[f"{source}_tournament_id"] = str(tourn_id_val)

                if 'match_url' in original_match:
                    opportunity[f"{source}_match_url"] = original_match['match_url']
                else:
                    source_key = source.lower()
                    if source_key in URL_TEMPLATES and match_id_val is not None:
                        config = URL_TEMPLATES.get(source_key, {})
                        template = config.get("template")
                        if template:
                            mappings = config.get("mappings", {})
                            mode_val = mappings.get("mode", {}).get(MODE_NAME, MODE_NAME)
                            sport_val = mappings.get("sport", {}).get(SPORT_NAME, SPORT_NAME)
                            slug_rules = config.get("slugify_fields", {}).get("tournament_name")
                            formatted_tourn_name = slugify(tourn_name_val, slug_rules) if slug_rules else (
                                quote(tourn_name_val) if tourn_name_val else "")
                            format_params = {"mode": mode_val, "sport": sport_val,
                                             "country_name": original_match.get("country", ""),
                                             "tournament_id": tourn_id_val or "", "match_id": match_id_val or "",
                                             "tournament_name": formatted_tourn_name}
                            try:
                                opportunity[f"{source}_match_url"] = template.format(**format_params)
                            except KeyError as e:
                                print(f"Warning: URL template for '{source}' contains an unknown placeholder: {e}")

            best_opportunity = opportunity
            best_arb_percentage = arb

    return best_opportunity


def analyze_optimal_arbitrage(matching_group: List[Dict]) -> Optional[Dict]:
    """
    Finds ALL arbitrage opportunities, structures them with common info at the top level,
    and sorts them by profitability.
    """
    if len(matching_group) < 2:
        return None

    group_id = matching_group[0].get("matching_group_id")
    if not group_id:
        print("Warning: Matching group is missing 'matching_group_id'. Skipping.")
        return None

    first_match = matching_group[0]
    home_teams = [m.get("home_team", "") for m in matching_group if m.get("home_team")]
    away_teams = [m.get("away_team", "") for m in matching_group if m.get("away_team")]
    best_home = max(home_teams, key=len) if home_teams else ""
    best_away = max(away_teams, key=len) if away_teams else ""

    invalid_names = {'null', 'unknown'}
    valid_country_names = [c for c in [m.get("country") for m in matching_group] if
                           isinstance(c, str) and c.strip() and c.lower() not in invalid_names]
    country = min(valid_country_names, key=len) if valid_country_names else (first_match.get("country") or "unknown")
    unique_sources = sorted(list({m.get("source") for m in matching_group if m.get("source")}))

    all_opportunities = []
    for r in range(2, len(unique_sources) + 1):
        for source_combo in itertools.combinations(unique_sources, r):
            matches_for_combo = [m for m in matching_group if m.get("source") in source_combo]
            # Pass the entire matching_group for misvalue analysis
            opportunity = _find_best_arb_for_combination(matches_for_combo, source_combo, matching_group)
            if opportunity:
                all_opportunities.append(opportunity)

    if not all_opportunities:
        return None

    # Sort opportunities by profitability (lowest arbitrage_percentage is best)
    all_opportunities.sort(key=lambda opp: opp['arbitrage_percentage'])

    # Build the final structured object
    final_object = {
        "group_id": group_id,
        "home_team": best_home,
        "away_team": best_away,
        "date": first_match.get("date"),
        "time": first_match.get("time"),
        "country": country,
        "all_sources": unique_sources,
        "opportunities": all_opportunities
    }

    print(f"Arbitrage Group Found: {best_home} vs {best_away} ({group_id}) with {len(all_opportunities)} combinations.")
    return final_object
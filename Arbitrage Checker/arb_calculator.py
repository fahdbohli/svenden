# arb_calculator.py

import re
import os
import itertools
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import quote
import json
from datetime import datetime
from zoneinfo import ZoneInfo

# ─── Placeholder for football market sets ───────────────────────────────────────
# `main.py` will assign this to the "market_sets" dict loaded from {SPORT}/markets.json.
MARKET_SETS: Dict[str, List[str]] = {}

# ─── Placeholders for URL building ───────────────────────────────────────────────
# `main.py` will load these from settings/url_builder.json
URL_TEMPLATES: Dict[str, Any] = {}
SPORT_NAME: str = ""
MODE_NAME: str = ""
MISVALUE_DETECTION_METHOD: str = "comparaison"
APPEARANCE_INVESTIGATION_LOGGING: bool = False
LOG_OUTPUT_ROOT: str = ""
FULL_CHECK_MARKETS: Set[str] = set()

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


def _write_arb_appearance_log(log_entry: Dict[str, Any]):
    """Saves a single arbitrage appearance investigation log entry."""
    if not APPEARANCE_INVESTIGATION_LOGGING or not LOG_OUTPUT_ROOT or not log_entry:
        return

    misvalue_source = log_entry.get("misvalue_source")
    group_id = log_entry.get("group_id")
    market_name = log_entry.get("market_name", "unknown_market")
    if not all([misvalue_source, group_id]):
        return

    today_str = datetime.now(ZoneInfo("Etc/GMT-1")).strftime("%d-%m-%Y")
    sanitized_market_name = market_name.replace('/', '_')

    log_dir = os.path.join(
        LOG_OUTPUT_ROOT, MODE_NAME, SPORT_NAME, today_str,
        misvalue_source, group_id, "appearance_investigations"
    )
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, f"{sanitized_market_name}.json")

    try:
        if os.path.exists(log_file_path):
            with open(log_file_path, "r", encoding="utf-8") as f:
                logs = json.load(f)
        else:
            logs = []
    except (json.JSONDecodeError, IOError):
        logs = []

    logs.append(log_entry)
    with open(log_file_path, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, ensure_ascii=False)


def _identify_misvalue_by_appearance(
    opportunity: Dict,
    all_matches_in_group: List[Dict],
    previous_match_data: Dict[str, List[Dict]],
    activity_data: Dict[str, Any]
) -> Optional[str]:
    """
    Identifies the misvalue source by checking which source's odds changed
    from the previous cycle to create the arbitrage opportunity.
    """
    if not all_matches_in_group:
        return None

    group_id = all_matches_in_group[0].get("matching_group_id")
    if not group_id or group_id not in previous_match_data:
        return None # No previous data to compare against

    previous_group = previous_match_data[group_id]
    previous_matches_by_src = {m['source']: m for m in previous_group}

    involved_sources = opportunity.get("arbitrage_sources", "").split(", ")
    odds_data = opportunity.get("best_odds", {})
    if len(involved_sources) < 2:
        return None

    changes = {}
    log_details = {
        "old_odds": {},
        "new_odds": {},
    }

    for odd_name, details in odds_data.items():
        source = details.get("source")
        new_odd = details.get("value")
        log_details["new_odds"][source] = new_odd

        # Find the old odd for this source and odd_name
        old_odd = None
        if source in previous_matches_by_src:
            old_odd = previous_matches_by_src[source].get(odd_name)
        log_details["old_odds"][source] = old_odd

        if old_odd is not None and new_odd is not None:
            changes[source] = (new_odd != old_odd)
        else:
            changes[source] = True # Treat missing old odd as a change

    changed_sources = [src for src, has_changed in changes.items() if has_changed]

    misvalue_source = None
    if len(changed_sources) == 1:
        # If only one source changed its odds, the *other* stable sources are the stale/misvalued ones.
        changed_one = changed_sources[0]
        stable_sources = [src for src in involved_sources if src != changed_one]
        if stable_sources:
             # In a 2-way arb, this is simple. In 3-way+, we pick the first stable one.
             misvalue_source = stable_sources[0]

    if misvalue_source:
        # --- Persist the finding in activity_data ---
        unique_id = opportunity.get("unique_id")
        if unique_id:
            # This ensures the dictionary for the unique_id exists,
            # then adds/updates the misvalue_source key.
            # setdefault returns the dict if it exists, or creates a new {} and returns that.
            activity_data.setdefault(unique_id, {})['misvalue_source'] = misvalue_source
        # --- END NEW ---

        if APPEARANCE_INVESTIGATION_LOGGING:
            log_entry = {
                "misvalue_source": misvalue_source,
                "group_id": group_id,
                "market_name": opportunity.get("complementary_set"),
                "arbitrage_percentage": opportunity.get("arbitrage_percentage"),
                "investigation_details": log_details,
                "involved_sources": involved_sources,
                "appeared_at": datetime.now().isoformat(),
            }
            _write_arb_appearance_log(log_entry)

    # If len is 0 or > 1, it's ambiguous, so we return None.
    return misvalue_source


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
        all_matches_in_group: List[Dict],
        previous_match_data: Dict[str, List[Dict]],
        activity_data: Dict[str, Any]
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

        # Check if this market is in the full_check list
        if name in FULL_CHECK_MARKETS:
            # For full_check markets, try all possible combinations of odds from different sources
            best_odds_with_details = {}

            # For each key in the market, try all available sources
            for k in keys:
                available_odds = []
                for match in matches_in_combination:
                    odd_value = match.get(k)
                    if odd_value and str(odd_value).strip():
                        try:
                            value = float(odd_value)
                            if value > 0:
                                available_odds.append((value, match.get("source"), match))
                        except (ValueError, TypeError):
                            continue

                if available_odds:
                    # Sort by value descending to get best odds first
                    available_odds.sort(key=lambda x: x[0], reverse=True)
                    best_odds_with_details[k] = available_odds
                else:
                    best_odds_with_details[k] = []

            # Try all combinations of odds for this market
            if all(best_odds_with_details.get(k) for k in keys):
                import itertools
                for odds_combination in itertools.product(*[best_odds_with_details[k] for k in keys]):
                    # Check if we have at least 2 different sources
                    sources_in_combo = set(odd_data[1] for odd_data in odds_combination)
                    if len(sources_in_combo) < 2:
                        continue

                    # Create odds_for_check for this combination
                    odds_for_check = {}
                    formatted_odds = {}
                    source_to_match_map = {}

                    for i, k in enumerate(keys):
                        value, source, match_obj = odds_combination[i]
                        odds_for_check[k] = (value, source)
                        formatted_odds[k] = {"value": value, "source": source}
                        source_to_match_map[source] = match_obj

                    arb = check_arbitrage(odds_for_check)
                    if arb is not None and arb < best_arb_percentage:
                        # This is a better arbitrage opportunity
                        arbitrage_match_ids = set()
                        for source, match_obj in source_to_match_map.items():
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

                        # Apply misvalue detection logic
                        unique_id = opportunity.get("unique_id")
                        misvalue_source = None

                        if unique_id and unique_id in activity_data and 'misvalue_source' in activity_data[unique_id]:
                            misvalue_source = activity_data[unique_id]['misvalue_source']

                        if not misvalue_source:
                            if MISVALUE_DETECTION_METHOD == "appearance":
                                misvalue_source = _identify_misvalue_by_appearance(
                                    opportunity,
                                    all_matches_in_group,
                                    previous_match_data,
                                    activity_data
                                )
                            elif MISVALUE_DETECTION_METHOD == "comparaison":
                                misvalue_source = _identify_misvalue_source(opportunity, all_matches_in_group)

                        if misvalue_source:
                            opportunity['misvalue_source'] = misvalue_source
                            if unique_id:
                                activity_data.setdefault(unique_id, {})['misvalue_source'] = misvalue_source

                        # Add match details
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
                                                         "tournament_id": tourn_id_val or "",
                                                         "match_id": match_id_val or "",
                                                         "tournament_name": formatted_tourn_name}
                                        try:
                                            opportunity[f"{source}_match_url"] = template.format(**format_params)
                                        except KeyError as e:
                                            print(
                                                f"Warning: URL template for '{source}' contains an unknown placeholder: {e}")

                        best_opportunity = opportunity
                        best_arb_percentage = arb
        else:
            # Original logic for non-full_check markets
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

                # Apply misvalue detection logic (same as above)
                unique_id = opportunity.get("unique_id")
                misvalue_source = None

                if unique_id and unique_id in activity_data and 'misvalue_source' in activity_data[unique_id]:
                    misvalue_source = activity_data[unique_id]['misvalue_source']

                if not misvalue_source:
                    if MISVALUE_DETECTION_METHOD == "appearance":
                        misvalue_source = _identify_misvalue_by_appearance(
                            opportunity,
                            all_matches_in_group,
                            previous_match_data,
                            activity_data
                        )
                    elif MISVALUE_DETECTION_METHOD == "comparaison":
                        misvalue_source = _identify_misvalue_source(opportunity, all_matches_in_group)

                if misvalue_source:
                    opportunity['misvalue_source'] = misvalue_source
                    if unique_id:
                        activity_data.setdefault(unique_id, {})['misvalue_source'] = misvalue_source

                # Add match details (same as above)
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


def analyze_optimal_arbitrage(
    matching_group: List[Dict],
    previous_match_data: Dict[str, List[Dict]],
    activity_data: Dict[str, Any] # <-- ADD THIS
) -> Optional[Dict]:
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
            opportunity = _find_best_arb_for_combination(
                matches_for_combo, source_combo, matching_group, previous_match_data, activity_data
            )
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
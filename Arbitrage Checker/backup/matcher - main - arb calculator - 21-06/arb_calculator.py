# arb_calculator.py

import re
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


# ──────────────────────────────────────────────────────────────────────────────

def slugify(text: str, rules: Dict[str, Any]) -> str:
    """
    Applies a set of rules to transform a string into a URL-friendly slug.
    """
    if not isinstance(text, str):
        return ""

    # Apply custom rules from the configuration
    if rules.get("remove_digits"):
        text = "".join(c for c in text if not c.isdigit())

    # Standard slugification process
    text = text.lower()  # Convert to lowercase
    text = re.sub(r'[^\w\s-]', '', text).strip()  # Remove non-word characters (excluding spaces and hyphens)

    # Replace spaces/underscores with the specified character (e.g., '-')
    space_replacement = rules.get("space_replacement", "-")
    text = re.sub(r'[\s_]+', space_replacement, text)

    return text


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
                # Return the entire match object to access all its fields later
                best_match_id = match
        except (ValueError, TypeError) as e:
            # Log the error for debugging
            print(f"Error parsing odd {key} from match {match.get('home_team')} vs {match.get('away_team')}: {e}")
            continue

    return best_value, best_source, best_match_id


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


def analyze_optimal_arbitrage(matching_group):
    """
    Find the optimal arbitrage opportunity across all sources for a match
    This function ensures we check all possible combinations of odds
    """
    # Only process if we have matches from at least 2 sources
    if len(matching_group) < 2:
        return None

    # Extract all sources from the original group. This list will NOT be modified.
    all_sources_for_match = [match.get("source") for match in matching_group]

    # Determine the best representative country name for the match group.
    # Rule: Use the shortest valid country name from all sources.
    # "Valid" means it is not None, 'null', or 'unknown'.
    invalid_names = {'null', 'unknown'}
    all_country_names = [m.get("country") for m in matching_group]

    valid_country_names = [
        c for c in all_country_names
        if isinstance(c, str) and c.strip() and c.lower() not in invalid_names
    ]

    if valid_country_names:
        # Pick the shortest name from the valid list
        country = min(valid_country_names, key=len)
    else:
        # Fallback: if no valid names, use the first available name or default to "unknown"
        country = matching_group[0].get("country") or "unknown"

    # Create match info using the first match as a template for date/time
    first_match = matching_group[0]

    # Check which home/away team name to use (use the most detailed one)
    home_teams = [m.get("home_team", "") for m in matching_group if m.get("home_team")]
    away_teams = [m.get("away_team", "") for m in matching_group if m.get("away_team")]

    best_home = max(home_teams, key=len) if home_teams else ""
    best_away = max(away_teams, key=len) if away_teams else ""

    # This 'info' object contains the full list of sources and will be used as the base.
    info = {
        "home_team": best_home,
        "away_team": best_away,
        "date": first_match.get("date"),
        "time": first_match.get("time"),
        "all_sources": all_sources_for_match,
        "country": country
    }

    # Match signature for debugging
    match_signature = f"{best_home} vs {best_away} on {first_match.get('date')} at {first_match.get('time')}"

    # Use the MARKET_SETS dictionary loaded by main.py
    market_sets = MARKET_SETS

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
        best_odds_with_details = {}
        for k in keys:
            best_odds_with_details[k] = pick_best_odds(matching_group, k)

        # Prepare a simplified dict for the original check_arbitrage function
        # best_match_obj is now the full match object
        odds_for_check = {k: (v, s) for k, (v, s, best_match_obj) in best_odds_with_details.items()}

        # Check if we have a valid arbitrage opportunity
        arb = check_arbitrage(odds_for_check)
        if arb is not None and arb < best_arb_percentage:
            # Format odds for better readability
            formatted_odds = {}
            for k, (v, s, best_match_obj) in best_odds_with_details.items():
                formatted_odds[k] = {"value": v, "source": s}

            # Collect the actual sources and match objects that form the arbitrage
            arbitrage_sources_set = set()
            arbitrage_match_ids = set()
            # This will now store source -> best_match_obj
            source_to_match_map = {}
            for k, (v, s, best_match_obj) in best_odds_with_details.items():
                if v > 0 and s and best_match_obj:  # Ensure the odd is valid
                    arbitrage_sources_set.add(s)
                    source_to_match_map[s] = best_match_obj
                    if best_match_obj.get("match_id"):
                        arbitrage_match_ids.add(str(best_match_obj.get("match_id")))

            arbitrage_sources_str = ", ".join(sorted(list(arbitrage_sources_set)))

            # Create the unique_id, sorted by length (desc) to ensure consistency
            sorted_match_ids = sorted(list(arbitrage_match_ids), key=len, reverse=True)
            unique_id = "-".join(sorted_match_ids)

            # Create opportunity object using a copy of the original 'info'
            # This ensures 'info.sources' remains the complete list for the next loop.
            opportunity = {
                "match_info": info.copy(),
                "complementary_set": name,
                "best_odds": formatted_odds,
                "arbitrage_percentage": round(arb, 4),
                "arbitrage_sources": arbitrage_sources_str,
                "unique_id": unique_id
            }

            # Add tournament name, ID, match ID, and URL for EACH ARBITRAGE SOURCE
            for source in arbitrage_sources_set:
                # Find the match details for this specific source from our map
                original_match = source_to_match_map.get(source)
                if original_match:
                    # Add the source-specific country name (from the raw filename)
                    opportunity['match_info'][f"{source}_country_name"] = original_match.get("country")
                    # Add tournament name to this opportunity's match_info
                    opportunity['match_info'][f"tournament_{source}"] = original_match.get("tournament_name")

                    # Get match and tournament IDs and name
                    match_id_val = original_match.get("match_id")
                    tourn_id_val = original_match.get("tournament_id")
                    tourn_name_val = original_match.get("tournament_name")

                    # Safely add match_id, attempting to convert to int
                    if match_id_val is not None:
                        try:
                            opportunity[f"{source}_match_id"] = int(match_id_val)
                        except (ValueError, TypeError):
                            opportunity[f"{source}_match_id"] = str(match_id_val)

                    # Safely add tournament_id, attempting to convert to int
                    if tourn_id_val is not None:
                        try:
                            opportunity[f"{source}_tournament_id"] = int(tourn_id_val)
                        except (ValueError, TypeError):
                            opportunity[f"{source}_tournament_id"] = str(tourn_id_val)

                    # ----- START OF MODIFICATION -----
                    # 1. Check if a URL already exists from the scraper
                    if 'match_url' in original_match:
                        opportunity[f"{source}_match_url"] = original_match['match_url']

                    # 2. Else, try to build it using templates
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
                                if slug_rules:
                                    formatted_tourn_name = slugify(tourn_name_val, slug_rules)
                                else:
                                    formatted_tourn_name = quote(tourn_name_val) if tourn_name_val else ""

                                format_params = {
                                    "mode": mode_val,
                                    "sport": sport_val,
                                    "country_name": original_match.get("country", ""),
                                    "tournament_id": tourn_id_val or "",
                                    "match_id": match_id_val or "",
                                    "tournament_name": formatted_tourn_name
                                }

                                try:
                                    url = template.format(**format_params)
                                    opportunity[f"{source}_match_url"] = url
                                except KeyError as e:
                                    print(f"Warning: URL template for '{source}' contains an unknown placeholder: {e}")
                    # ----- END OF MODIFICATION -----

            best_opportunity = opportunity
            best_arb_percentage = arb

            # Debug info
            print(f"Arbitrage found ({name}): {match_signature}, {arb:.4f} between {arbitrage_sources_str}")

    # Return the best opportunity (or None if no arbitrage found)
    return [best_opportunity] if best_opportunity else None
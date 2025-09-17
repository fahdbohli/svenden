# arb_calculator.py

from typing import Any, Dict, List, Optional, Tuple

# ─── Placeholder for football market sets ───────────────────────────────────────
# `main.py` will assign this to the "market_sets" dict loaded from {SPORT}/markets.json.
MARKET_SETS: Dict[str, List[str]] = {}


# ──────────────────────────────────────────────────────────────────────────────


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
                best_match_id = match.get("match_id")
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
        odds_for_check = {k: (v, s) for k, (v, s, mid) in best_odds_with_details.items()}

        # Check if we have a valid arbitrage opportunity
        arb = check_arbitrage(odds_for_check)
        arbitrage_sources = ""
        if arb is not None and arb < best_arb_percentage:
            # Format odds for better readability and get arbitrage sources/match_ids
            formatted_odds = {}
            arbitrage_match_ids = set()

            for k, (v, s, mid) in best_odds_with_details.items():
                formatted_odds[k] = {"value": v, "source": s}
                if s not in arbitrage_sources:
                    arbitrage_sources += f"{s}, "
                if mid:
                    arbitrage_match_ids.add(str(mid))

            arbitrage_sources = arbitrage_sources[:-2]

            # Create the unique_id, sorted by length (desc) to ensure consistency
            sorted_match_ids = sorted(list(arbitrage_match_ids), key=len, reverse=True)
            unique_id = "-".join(sorted_match_ids)


            # Create opportunity object
            opportunity = {
                "match_info": info,
                "complementary_set": name,
                "best_odds": formatted_odds,
                "arbitrage_percentage": round(arb, 4),
                "arbitrage_sources": arbitrage_sources,
                "unique_id": unique_id
            }

            best_opportunity = opportunity
            best_arb_percentage = arb

            # Debug info
            print(f"Arbitrage found ({name}): {match_signature}, {arb:.4f} between {arbitrage_sources}")

    # Return the best opportunity (or None if no arbitrage found)
    return [best_opportunity] if best_opportunity else None
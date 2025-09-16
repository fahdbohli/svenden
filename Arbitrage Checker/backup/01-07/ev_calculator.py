# ev_calculator.py

import os
import json
from typing import Dict, List, Any, Optional

# --- Global placeholders (populated by main.py) ---
METHOD: str = "ONE_SHARPING"
SHARP_SOURCE: str = ""
SHARPING_GROUP: List[str] = []
EV_SOURCE: str = ""
ODDS_INTERVAL: List[float] = [1.0, 10.0]
MIN_OVERPRICE: float = 0.0
MARKET_SETS: Dict[str, List[str]] = {}
URL_TEMPLATES: Dict[str, str] = {}
SPORT_NAME: str = ""
MODE_NAME: str = ""


# ----------------------------------------------------

def build_source_url(source_name: str, match_data: Dict[str, Any]) -> str:
    """
    Builds a specific match URL for a given source using URL_TEMPLATES.
    This version handles case-insensitivity and the new config structure
    with 'template' and 'mappings' keys.
    """
    # --- FIX 1: Make the lookup case-insensitive ---
    # Create a case-insensitive lookup dictionary on the fly.
    url_templates_lower = {k.lower(): v for k, v in URL_TEMPLATES.items()}
    template_config = url_templates_lower.get(source_name.lower())

    # Check if the match data contain a match url
    if match_data.get("match_url"):
        return match_data["match_url"]

    if not template_config:
        # Warning for a missing template
        if source_name not in getattr(build_source_url, "warned_sources", set()):
            print(f"[URL_BUILDER_WARN] No URL template found for source: '{source_name}' in url_builder.json")
            if not hasattr(build_source_url, "warned_sources"):
                build_source_url.warned_sources = set()
            build_source_url.warned_sources.add(source_name)
        return ""

    # --- FIX 2: Handle the new nested structure ---
    template = template_config.get("template")
    mappings = template_config.get("mappings", {})

    if not template:
        print(f"[URL_BUILDER_WARN] Template config for '{source_name}' is missing the 'template' string.")
        return ""

    try:
        # Prepare all data needed for formatting
        format_data = {
            "sport": SPORT_NAME,
            "mode": MODE_NAME,
            **match_data
        }

        # Apply mappings if they exist
        if 'mode' in mappings and MODE_NAME in mappings.get('mode', {}):
            format_data['mode'] = mappings['mode'][MODE_NAME]
        if 'sport' in mappings and SPORT_NAME in mappings.get('sport', {}):
            format_data['sport'] = mappings['sport'][SPORT_NAME]

        # Check for required keys before attempting to format
        # This part helps debug if match data is missing an ID
        required_keys = [k.split('}')[0] for k in template.split('{')[1:]]

        missing_keys = [key for key in required_keys if key not in format_data or not format_data[key]]
        if missing_keys:
            # This is a helpful warning for the user
            # print(f"[URL_BUILDER_WARN] Cannot build URL for {source_name} (match_id: {format_data.get('match_id')}). "
            #       f"Missing or empty data for keys: {missing_keys}")
            return ""

        return template.format(**format_data)

    except (KeyError, TypeError) as e:
        # Catch any other formatting errors
        print(f"[URL_BUILDER_ERROR] Failed to format URL for {source_name} (match_id: {match_data.get('match_id')}). "
              f"Error: {e}. Check template placeholders and mappings.")
        return ""


# ... (The rest of ev_calculator.py remains unchanged) ...

def get_fair_odds_one_sharp(market_set: List[str], sharp_match: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """Calculates fair odds by removing the vig from a single sharp source."""
    odds_values = [sharp_match.get(odd_name) for odd_name in market_set]
    if not all(isinstance(o, (int, float)) and o > 0 for o in odds_values):
        return None

    vig_sum = sum(1.0 / o for o in odds_values)
    if vig_sum <= 0:
        return None

    fair_odds = {
        market_set[i]: val * vig_sum
        for i, val in enumerate(odds_values)
    }
    return fair_odds


def get_fair_odds_multiple_sharp(market_set: List[str], matches_by_src: Dict[str, Dict[str, Any]]) -> Optional[
    Dict[str, float]]:
    """Calculates fair odds based on the average odds from a group of sharp sources."""
    avg_odds_calculator = {odd_name: {'sum': 0.0, 'count': 0} for odd_name in market_set}

    for src in SHARPING_GROUP:
        if src in matches_by_src:
            match = matches_by_src[src]
            for odd_name in market_set:
                odd_value = match.get(odd_name)
                if isinstance(odd_value, (int, float)) and odd_value > 0:
                    avg_odds_calculator[odd_name]['sum'] += odd_value
                    avg_odds_calculator[odd_name]['count'] += 1

    avg_odds = {}
    for odd_name, data in avg_odds_calculator.items():
        if data['count'] > 0:
            avg_odds[odd_name] = data['sum'] / data['count']
        else:
            return None

    odds_values = list(avg_odds.values())
    vig_sum = sum(1.0 / o for o in odds_values)
    if vig_sum <= 0:
        return None

    fair_odds = {
        name: val * vig_sum
        for name, val in avg_odds.items()
    }
    return fair_odds


def analyze_ev_opportunities(group: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Analyzes a group of matched matches to find Positive Expected Value (+EV) opportunities.
    """
    if not group:
        return None

    matches_by_src = {m['source']: m for m in group}

    if EV_SOURCE not in matches_by_src:
        return None

    if METHOD == "ONE_SHARPING" and SHARP_SOURCE not in matches_by_src:
        return None
    if METHOD == "MULTIPLE_SHARPING" and not any(s in matches_by_src for s in SHARPING_GROUP):
        return None

    found_opportunities = []
    ev_source_match = matches_by_src[EV_SOURCE]

    for market_name, market_odds_list in MARKET_SETS.items():
        fair_odds = None

        if METHOD == "ONE_SHARPING":
            sharp_match = matches_by_src[SHARP_SOURCE]
            fair_odds = get_fair_odds_one_sharp(market_odds_list, sharp_match)
        elif METHOD == "MULTIPLE_SHARPING":
            fair_odds = get_fair_odds_multiple_sharp(market_odds_list, matches_by_src)

        if not fair_odds:
            continue

        for odd_name, fair_value in fair_odds.items():

            if not (ODDS_INTERVAL[0] <= fair_value <= ODDS_INTERVAL[1]):
                continue

            ev_source_odd = ev_source_match.get(odd_name)
            if not isinstance(ev_source_odd, (int, float)) or ev_source_odd <= 0:
                continue

            if ev_source_odd > fair_value:
                overprice = (ev_source_odd / fair_value) - 1.0

                if overprice >= MIN_OVERPRICE:
                    ev_opp = {
                        "source": EV_SOURCE,
                        "odd_name": odd_name,
                        "overpriced_odd_value": ev_source_odd,
                        "fair_odd_value": round(fair_value, 4),
                        "overprice": round(overprice, 4),
                        "unique_id": f"{ev_source_match.get('match_id')}-{odd_name}",
                        f"{EV_SOURCE}_country_name": ev_source_match.get("country_name", ""),
                        f"tournament_{EV_SOURCE}": ev_source_match.get("tournament_name", ""),
                        f"{EV_SOURCE}_match_id": ev_source_match.get("match_id", ""),
                        f"{EV_SOURCE}_tournament_id": ev_source_match.get("tournament_id", ""),
                        f"{EV_SOURCE}_match_url": build_source_url(EV_SOURCE, ev_source_match),
                    }
                    found_opportunities.append(ev_opp)

    if found_opportunities:
        # use the ev source match details
        for m in group:
            if m.get("source") == EV_SOURCE:
                base_match = m
        country_canonical = base_match.get('country')
        if not country_canonical and group:
            country_canonical = group[0].get('country_name')

        ev_group_object = {
            "group_id": base_match.get("matching_group_id"),
            "home_team": base_match.get("home_team"),
            "away_team": base_match.get("away_team"),
            "date": base_match.get("date"),
            "time": base_match.get("time"),
            "country": country_canonical,
            "all_sources": sorted(list(matches_by_src.keys())),
            "opportunities": found_opportunities
        }
        return ev_group_object

    return None
import os
import json
import csv
import argparse
from typing import Optional, Dict, Any, List


def convert_arbitrage_to_csv(
        mode: str,
        sport: str,
        sort_by: str = "arbitrage_percentage",
        ascending: bool = True,
        filter_country: Optional[str] = None,
        min_arb_percentage: Optional[float] = None,
        max_arb_percentage: Optional[float] = None,
) -> None:
    """
    Convert arbitrage opportunity JSON files to a single CSV file with enhanced display format

    Args:
        mode: Either 'live' or 'prematch'
        sport: Sport name (football, basketball, tennis, etc.) or 'all' for all sports
        sort_by: Field to sort results by
        ascending: Whether to sort in ascending order
        filter_country: Optional country filter
        min_arb_percentage: Optional minimum arbitrage percentage filter
        max_arb_percentage: Optional maximum arbitrage percentage filter
    """
    # Determine input directory and output paths based on mode and sport
    if sport == 'all':
        input_dir = f"arbs/{mode}"
        output_dir = f"arbs/{mode}"
        output_filename = f"all_{mode}_arbs.csv"
    else:
        input_dir = f"arbs/{mode}/{sport}"
        output_dir = f"arbs/{mode}/{sport}"
        output_filename = f"{mode}_arbs_{sport}.csv"

    output_csv = os.path.join(output_dir, output_filename)

    print(f"🔍 Scanning for arbitrage opportunities in '{input_dir}'...")

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_opportunities = []
    country_counts = {}
    errors = []

    # Function to process files in a directory
    def process_directory(dir_path):
        if not os.path.exists(dir_path):
            print(f"⚠️ Directory '{dir_path}' does not exist.")
            return

        for file_name in os.listdir(dir_path):
            if not file_name.lower().endswith('.json'):
                continue

            country = os.path.splitext(file_name)[0]

            # Apply country filter if specified
            if filter_country and filter_country.lower() != country.lower():
                continue

            file_path = os.path.join(dir_path, file_name)

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                    # Handle new structure - data is now a list of group objects
                    total_opportunities_in_file = 0

                    if isinstance(data, list):
                        for group in data:
                            if isinstance(group, dict) and "opportunities" in group:
                                opportunities_list = group["opportunities"]

                                # Only process the first opportunity from each group
                                if opportunities_list and len(opportunities_list) > 0:
                                    first_opp = opportunities_list[0]

                                    # Skip if it doesn't meet arbitrage percentage filters
                                    arb_pct = first_opp.get("arbitrage_percentage", 0)
                                    if (min_arb_percentage is not None and arb_pct < min_arb_percentage or
                                            max_arb_percentage is not None and arb_pct > max_arb_percentage):
                                        continue

                                    # Process the opportunity, passing the group data for match info
                                    processed_opp = process_opportunity_with_group(first_opp, group, country)
                                    all_opportunities.append(processed_opp)
                                    total_opportunities_in_file += 1
                            else:
                                print(f"⚠️ Unexpected group structure in {file_name}")
                    else:
                        print(f"⚠️ Expected list structure in {file_name}, got {type(data)}")

                    # Track opportunities by country
                    country_counts[country] = country_counts.get(country, 0) + total_opportunities_in_file

            except Exception as e:
                error_msg = f"Error processing {file_name}: {str(e)}"
                errors.append(error_msg)
                print(f"❌ {error_msg}")

    # Process files based on sport selection
    if sport == 'all':
        # Process all subdirectories in the mode directory
        if os.path.exists(input_dir):
            for sport_dir in os.listdir(input_dir):
                sport_path = os.path.join(input_dir, sport_dir)
                if os.path.isdir(sport_path):
                    process_directory(sport_path)
        else:
            print(f"⚠️ Base directory '{input_dir}' does not exist.")
            return
    else:
        # Process specific sport directory
        process_directory(input_dir)

    # Sort opportunities
    if sort_by in (all_opportunities[0] if all_opportunities else {}):
        # Special handling for numeric fields
        numeric_fields = ["arbitrage_percentage", "profit_margin"]
        if sort_by in numeric_fields:
            all_opportunities.sort(
                key=lambda x: float(x.get(sort_by, 0) or 0) if x.get(sort_by) else float('inf'),
                reverse=not ascending
            )
        else:
            all_opportunities.sort(
                key=lambda x: x.get(sort_by, ""),
                reverse=not ascending
            )

    # Get the fieldnames in the desired order
    fieldnames = create_ordered_fieldnames(all_opportunities)

    # Write to CSV
    if all_opportunities:
        with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in all_opportunities:
                # Ensure all fields are included, with empty strings for missing values
                writer_row = {field: row.get(field, "") for field in fieldnames}
                writer.writerow(writer_row)

        # Generate summary
        total_count = len(all_opportunities)
        print(f"\n✅ Exported {total_count} arbitrage opportunities to '{output_csv}'.")

        # Print country breakdown
        if country_counts:
            print("\n📊 Opportunities by country:")
            for country, count in sorted(country_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0:  # Only show countries with opportunities after filtering
                    print(f"  • {country}: {count}")

        # Print profit margin summary
        profit_margins = [opp.get("profit_margin", 0) for opp in all_opportunities]
        if profit_margins:
            avg_profit = sum(profit_margins) / len(profit_margins)
            max_profit = max(profit_margins)
            print(f"\n💰 Profit margin: Avg {avg_profit:.2f}%, Max {max_profit:.2f}%")
    else:
        print("⚠️ No arbitrage opportunities found to export.")

    # Print any errors
    if errors:
        print(f"\n⚠️ Encountered {len(errors)} errors during processing.")


def process_opportunity_with_group(opp: Dict[str, Any], group: Dict[str, Any], country: str) -> Dict[str, Any]:
    """
    Process an arbitrage opportunity with group information into a structured format

    Args:
        opp: The arbitrage opportunity dictionary
        group: The group dictionary containing match info
        country: The country name

    Returns:
        A processed dictionary ready for CSV export
    """
    complementary_set = opp.get("complementary_set", "")

    # Create base row with match details from group
    row = {
        "country": country,
        "home_team": group.get("home_team", ""),
        "away_team": group.get("away_team", ""),
        "date": group.get("date", ""),
        "time": group.get("time", ""),
        "group_id": group.get("group_id", ""),
        "complementary_set": complementary_set,
        "arbitrage_percentage": opp.get("arbitrage_percentage", ""),
        "profit_margin": round(100 * (1 - (opp.get("arbitrage_percentage", 1) or 1)), 2),
        "arbitrage_sources": opp.get("arbitrage_sources"),
        "unique_id": opp.get("unique_id", "")
    }

    # Add sources information from group
    sources = group.get("all_sources", [])
    if sources:
        row["all_sources"] = ", ".join(sources)

    # Add odds information in a better format
    best_odds = opp.get("best_odds", {})

    # Process different complementary sets
    if complementary_set == "three_way":
        # 1, X, 2 odds
        process_odds_pair(row, best_odds, "1_odd", "home_win")
        process_odds_pair(row, best_odds, "draw_odd", "draw")
        process_odds_pair(row, best_odds, "2_odd", "away_win")
    elif complementary_set == "pair_1":
        # 1, X2 odds
        process_odds_pair(row, best_odds, "1_odd", "home_win")
        process_odds_pair(row, best_odds, "X2_odd", "draw_or_away")
    elif complementary_set == "pair_3":
        # 2, 1X odds
        process_odds_pair(row, best_odds, "2_odd", "away_win")
        process_odds_pair(row, best_odds, "1X_odd", "home_or_draw")
    elif complementary_set == "two_vs_1x":
        # 2, 1X odds (same as pair_3 but with different name)
        process_odds_pair(row, best_odds, "2_odd", "away_win")
        process_odds_pair(row, best_odds, "1X_odd", "home_or_draw")
    elif complementary_set == "pair_4":
        # Both teams to score
        process_odds_pair(row, best_odds, "both_score_odd", "btts_yes")
        process_odds_pair(row, best_odds, "both_noscore_odd", "btts_no")
    elif complementary_set == "pair_5":
        # Over/Under
        process_odds_pair(row, best_odds, "under_2.5_odd", "under_2.5")
        process_odds_pair(row, best_odds, "over_2.5_odd", "over_2.5")
    elif complementary_set.startswith("ah_"):
        # Asian Handicap processing
        # Extract handicap values and process accordingly
        for market, odds_data in best_odds.items():
            if "handicap" in market:
                # Clean up the market name for display
                display_name = market.replace("_odd", "").replace("_", " ")
                process_odds_pair(row, best_odds, market, display_name)
    else:
        # Generic processing for any other markets
        for market, odds_data in best_odds.items():
            process_odds_pair(row, best_odds, market, market.replace("_odd", ""))

    return row


def process_opportunity(opp: Dict[str, Any], country: str) -> Dict[str, Any]:
    """
    Process an arbitrage opportunity into a structured format (legacy function)

    Args:
        opp: The arbitrage opportunity dictionary
        country: The country name

    Returns:
        A processed dictionary ready for CSV export
    """
    match_info = opp.get("match_info", {})
    complementary_set = opp.get("complementary_set", "")

    # Create base row with match details
    row = {
        "country": country,
        "home_team": match_info.get("home_team", ""),
        "away_team": match_info.get("away_team", ""),
        "date": match_info.get("date", ""),
        "time": match_info.get("time", ""),
        "complementary_set": complementary_set,
        "arbitrage_percentage": opp.get("arbitrage_percentage", ""),
        "profit_margin": round(100 * (1 - (opp.get("arbitrage_percentage", 1) or 1)), 2),
        "arbitrage_sources": opp.get("arbitrage_sources")
    }

    # Add odds information in a better format
    best_odds = opp.get("best_odds", {})

    # Process different complementary sets
    if complementary_set == "three_way":
        # 1, X, 2 odds
        process_odds_pair(row, best_odds, "1_odd", "home_win")
        process_odds_pair(row, best_odds, "draw_odd", "draw")
        process_odds_pair(row, best_odds, "2_odd", "away_win")
    elif complementary_set == "pair_1":
        # 1, X2 odds
        process_odds_pair(row, best_odds, "1_odd", "home_win")
        process_odds_pair(row, best_odds, "X2_odd", "draw_or_away")
    elif complementary_set == "pair_3":
        # 2, 1X odds
        process_odds_pair(row, best_odds, "2_odd", "away_win")
        process_odds_pair(row, best_odds, "1X_odd", "home_or_draw")
    elif complementary_set == "two_vs_1x":  # Added support for the new complementary set from your example
        # 2, 1X odds (same as pair_3 but with different name)
        process_odds_pair(row, best_odds, "2_odd", "away_win")
        process_odds_pair(row, best_odds, "1X_odd", "home_or_draw")
    elif complementary_set == "pair_4":
        # Both teams to score
        process_odds_pair(row, best_odds, "both_score_odd", "btts_yes")
        process_odds_pair(row, best_odds, "both_noscore_odd", "btts_no")
    elif complementary_set == "pair_5":
        # Over/Under
        process_odds_pair(row, best_odds, "under_2.5_odd", "under_2.5")
        process_odds_pair(row, best_odds, "over_2.5_odd", "over_2.5")
    else:
        # Generic processing for any other markets
        for market, odds_data in best_odds.items():
            process_odds_pair(row, best_odds, market, market.replace("_odd", ""))

    # Add sources information
    sources = match_info.get("all_sources", [])
    if sources:
        row["all_sources"] = ", ".join(sources)
    elif match_info.get("source_pair"):
        row["all_sources"] = match_info.get("source_pair", "")

    return row


def process_odds_pair(row: Dict[str, Any], best_odds: Dict[str, Any], odds_key: str, display_name: str) -> None:
    """
    Process an odds pair (value and source) and add it to the row

    Args:
        row: The row dictionary to update
        best_odds: The best odds dictionary
        odds_key: The key in best_odds to process
        display_name: The display name prefix for the CSV columns
    """
    if odds_key in best_odds:
        odds_data = best_odds[odds_key]
        if isinstance(odds_data, dict) and "value" in odds_data and "source" in odds_data:
            row[f"{display_name}_odds"] = odds_data["value"]
            row[f"{display_name}_source"] = odds_data["source"]
        elif isinstance(odds_data, list) and len(odds_data) >= 2:
            row[f"{display_name}_odds"] = odds_data[0]
            row[f"{display_name}_source"] = odds_data[1]


def create_ordered_fieldnames(opportunities: List[Dict[str, Any]]) -> List[str]:
    """
    Create an ordered list of fieldnames for the CSV

    Args:
        opportunities: The list of processed opportunities

    Returns:
        An ordered list of fieldnames
    """
    if not opportunities:
        return []

    # Find all unique keys across opportunities
    all_keys = set()
    for opp in opportunities:
        all_keys.update(opp.keys())

    # Define column order priority
    primary_columns = [
        "country", "date", "time", "home_team", "away_team",
        "complementary_set", "arbitrage_percentage", "profit_margin", "arbitrage_sources", "all_sources"
    ]

    # Odds columns - group related odds together
    odds_groups = {
        "home_win": ["home_win_odds", "home_win_source"],
        "draw": ["draw_odds", "draw_source"],
        "away_win": ["away_win_odds", "away_win_source"],
        "home_or_draw": ["home_or_draw_odds", "home_or_draw_source"],
        "draw_or_away": ["draw_or_away_odds", "draw_or_away_source"],
        "btts": ["btts_yes_odds", "btts_yes_source", "btts_no_odds", "btts_no_source"],
        "over_under": ["over_2.5_odds", "over_2.5_source", "under_2.5_odds", "under_2.5_source"],
    }

    # Flatten the odds groups
    odds_columns = []
    for group in odds_groups.values():
        for col in group:
            if col in all_keys:
                odds_columns.append(col)

    # Add any other odds columns not in predefined groups
    other_odds_columns = [k for k in all_keys if (k.endswith("_odds") or k.endswith("_source"))
                          and k not in odds_columns]
    odds_columns.extend(sorted(other_odds_columns))

    # Combine all columns in the desired order
    remaining_columns = [k for k in all_keys if k not in primary_columns and
                         k not in odds_columns]

    return [col for col in (primary_columns + odds_columns + sorted(remaining_columns))
            if col in all_keys]


def main():
    """Parse command line arguments and run the converter"""

    # ========== MODIFY THESE VALUES DIRECTLY IN THE CODE ==========
    MODE = "live"  # Change this to 'live' or 'prematch'
    SPORT = "football"  # Change this to sport name or 'all' for all sports

    # Optional parameters you can also modify here
    SORT_BY = "arbitrage_percentage"
    DESCENDING = False  # Set to True for descending order
    FILTER_COUNTRY = None  # Set to country name to filter, or None for all
    MIN_ARB_PERCENTAGE = None  # Set minimum arbitrage percentage or None
    MAX_ARB_PERCENTAGE = None  # Set maximum arbitrage percentage or None
    # ==============================================================

    parser = argparse.ArgumentParser(description="Convert arbitrage JSON files to CSV with enhanced display format")

    parser.add_argument("-m", "--mode", default=MODE, choices=['live', 'prematch'],
                        help=f"Mode: 'live' or 'prematch' (default: {MODE})")
    parser.add_argument("--sport", default=SPORT,
                        help=f"Sport name (football, basketball, tennis, etc.) or 'all' for all sports (default: {SPORT})")
    parser.add_argument("-s", "--sort-by", default=SORT_BY,
                        help=f"Field to sort results by (default: {SORT_BY})")
    parser.add_argument("-d", "--descending", action="store_true", default=DESCENDING,
                        help=f"Sort in descending order (default: {'descending' if DESCENDING else 'ascending'})")
    parser.add_argument("-c", "--country", default=FILTER_COUNTRY,
                        help=f"Filter by country (default: {'all countries' if FILTER_COUNTRY is None else FILTER_COUNTRY})")
    parser.add_argument("--min-arb", type=float, default=MIN_ARB_PERCENTAGE,
                        help=f"Minimum arbitrage percentage (default: {'no minimum' if MIN_ARB_PERCENTAGE is None else MIN_ARB_PERCENTAGE})")
    parser.add_argument("--max-arb", type=float, default=MAX_ARB_PERCENTAGE,
                        help=f"Maximum arbitrage percentage (default: {'no maximum' if MAX_ARB_PERCENTAGE is None else MAX_ARB_PERCENTAGE})")

    args = parser.parse_args()

    convert_arbitrage_to_csv(
        mode=args.mode,
        sport=args.sport,
        sort_by=args.sort_by,
        ascending=not args.descending,
        filter_country=args.country,
        min_arb_percentage=args.min_arb,
        max_arb_percentage=args.max_arb
    )


if __name__ == "__main__":
    main()
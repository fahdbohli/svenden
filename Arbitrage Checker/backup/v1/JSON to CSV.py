import os
import json
import csv
import argparse
from typing import Optional, Dict, Any, List


def convert_arbitrage_to_csv(
        input_dir: str = "arbitrage_opportunities",
        output_csv: str = "football_arbitrage_opportunities.csv",
        sort_by: str = "arbitrage_percentage",
        ascending: bool = True,
        filter_country: Optional[str] = None,
        min_arb_percentage: Optional[float] = None,
        max_arb_percentage: Optional[float] = None,
) -> None:
    """
    Convert arbitrage opportunity JSON files to a single CSV file with enhanced display format

    Args:
        input_dir: Directory containing arbitrage JSON files
        output_csv: Path to output CSV file
        sort_by: Field to sort results by
        ascending: Whether to sort in ascending order
        filter_country: Optional country filter
        min_arb_percentage: Optional minimum arbitrage percentage filter
        max_arb_percentage: Optional maximum arbitrage percentage filter
    """
    print(f"🔍 Scanning for arbitrage opportunities in '{input_dir}'...")

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_csv)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_opportunities = []
    country_counts = {}
    errors = []

    # Load all arbitrage opportunities
    for file_name in os.listdir(input_dir):
        if not file_name.lower().endswith('.json'):
            continue

        country = os.path.splitext(file_name)[0]

        # Apply country filter if specified
        if filter_country and filter_country.lower() != country.lower():
            continue

        file_path = os.path.join(input_dir, file_name)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

                # Track opportunities by country
                country_counts[country] = country_counts.get(country, 0) + len(data)

                for opp in data:
                    # Skip if it doesn't meet arbitrage percentage filters
                    arb_pct = opp.get("arbitrage_percentage", 0)
                    if (min_arb_percentage is not None and arb_pct < min_arb_percentage or
                            max_arb_percentage is not None and arb_pct > max_arb_percentage):
                        country_counts[country] -= 1
                        continue

                    # Process the opportunity
                    processed_opp = process_opportunity(opp, country)
                    all_opportunities.append(processed_opp)

        except Exception as e:
            error_msg = f"Error processing {file_name}: {str(e)}"
            errors.append(error_msg)
            print(f"❌ {error_msg}")

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


def process_opportunity(opp: Dict[str, Any], country: str) -> Dict[str, Any]:
    """
    Process an arbitrage opportunity into a structured format

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
    }

    # Add sources information
    sources = match_info.get("sources", [])
    if sources:
        row["sources"] = ", ".join(sources)
    elif match_info.get("source_pair"):
        row["sources"] = match_info.get("source_pair", "")

    # Add tournament information for each source
    for key, value in match_info.items():
        if key.startswith("tournament_") and value:
            row[key] = value

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
        "complementary_set", "arbitrage_percentage", "profit_margin", "sources"
    ]

    # Tournament columns
    tournament_columns = sorted([k for k in all_keys if k.startswith("tournament_")])

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
                         k not in tournament_columns and k not in odds_columns]

    return [col for col in (primary_columns + tournament_columns + odds_columns + sorted(remaining_columns))
            if col in all_keys]


def main():
    """Parse command line arguments and run the converter"""
    parser = argparse.ArgumentParser(description="Convert arbitrage JSON files to CSV with enhanced display format")

    parser.add_argument("-i", "--input-dir", default="arbitrage_opportunities",
                        help="Directory containing arbitrage JSON files")
    parser.add_argument("-o", "--output-csv", default="football_arbitrage_opportunities.csv",
                        help="Path to output CSV file")
    parser.add_argument("-s", "--sort-by", default="arbitrage_percentage",
                        help="Field to sort results by")
    parser.add_argument("-d", "--descending", action="store_true",
                        help="Sort in descending order")
    parser.add_argument("-c", "--country",
                        help="Filter by country")
    parser.add_argument("--min-arb", type=float,
                        help="Minimum arbitrage percentage")
    parser.add_argument("--max-arb", type=float,
                        help="Maximum arbitrage percentage")

    args = parser.parse_args()

    convert_arbitrage_to_csv(
        input_dir=args.input_dir,
        output_csv=args.output_csv,
        sort_by=args.sort_by,
        ascending=not args.descending,
        filter_country=args.country,
        min_arb_percentage=args.min_arb,
        max_arb_percentage=args.max_arb
    )


if __name__ == "__main__":
    main()
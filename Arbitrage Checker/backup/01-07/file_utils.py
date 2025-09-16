import os
import shutil
import json
from datetime import datetime

# Import `canonical` from matcher.py (ensure matcher.py is in the same directory or on PYTHONPATH)
from matcher import canonical


def reset_output(output_dir: str):
    """
    DEPRECATED for the main flow. This function deletes and recreates the output directory.
    The new process updates files in place and uses `cleanup_old_files` instead.
    """
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)


def cleanup_old_files(output_dir: str, updated_filenames: set[str]):
    """
    Deletes .json files from the output directory that were not generated or updated in the current run.

    Args:
        output_dir (str): The path to the directory to clean.
        updated_filenames (set): A set of filenames (e.g., {"France.json", "Spain.json"})
                                 that were created or updated in this run.
    """
    if not os.path.isdir(output_dir):
        print(f"Warning: Output directory {output_dir} not found for cleanup.")
        return

    deleted_count = 0
    for filename in os.listdir(output_dir):
        if filename.lower().endswith('.json'):
            if filename not in updated_filenames:
                try:
                    file_path = os.path.join(output_dir, filename)
                    os.remove(file_path)
                    print(f"Deleted stale arbitrage file: {filename}")
                    deleted_count += 1
                except OSError as e:
                    print(f"Error deleting file {filename}: {e}")
    if deleted_count > 0:
        print(f"Deleted {deleted_count} old file(s).")


def load_matches(filename: str) -> list:
    """
    Load matches from a JSON file. Injects "country" and "country_name" fields
    (based on the filename without extension) and copies parent "tournament_id"
    and "tournament_name" into each match.
    Returns a list of match dictionaries.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse JSON file {filename}: {e}. Skipping this file.")
                return []
    except (IOError, OSError) as e:
        print(f"Warning: Could not open file {filename}: {e}. Skipping this file.")
        return []

    matches = []
    # Get the raw filename, e.g., "Ecuador source 2" from "Ecuador source 2.json"
    base_filename = os.path.splitext(os.path.basename(filename))[0]

    # Let's use canonical() to get the clean country name like "Ecuador"
    # This might not be what you want for display, so we will use the raw base_filename
    # for the `country_name` key.
    canonical_country = canonical(base_filename)

    if isinstance(data, dict):
        tournament_id = data.get("tournament_id")
        tournament_name = data.get("tournament_name")
        match_list = data.get("matches", [data] if "match_id" in data else [])

        for match in match_list:
            # Add BOTH keys for compatibility.
            match["country"] = canonical_country  # The canonical name for grouping
            match["country_name"] = base_filename  # The raw file name for display
            if tournament_id:
                match["tournament_id"] = tournament_id
            if tournament_name:
                match["tournament_name"] = tournament_name
        matches.extend(match_list)

    elif isinstance(data, list):
        for element in data:
            if isinstance(element, dict) and "matches" in element:
                tournament_id = element.get("tournament_id")
                tournament_name = element.get("tournament_name")
                for match in element["matches"]:
                    # Add BOTH keys for compatibility.
                    match["country"] = canonical_country  # The canonical name for grouping
                    match["country_name"] = base_filename  # The raw file name for display
                    match["tournament_id"] = tournament_id
                    match["tournament_name"] = tournament_name
                matches.extend(element["matches"])

            elif isinstance(element, dict) and "match_id" in element:
                # Add BOTH keys for compatibility.
                element["country"] = canonical_country  # The canonical name for grouping
                element["country_name"] = base_filename  # The raw file name for display
                matches.append(element)

    return matches


def find_actual_filename(base: str, dir_path: str) -> str | None:
    """
    Given a base name (possibly a synonym) and a directory path,
    returns the actual filename (with .json) if it exists in dir_path.
    """
    primary = canonical(base)
    want = primary + ".json"
    candidate_path = os.path.join(dir_path, want)
    if os.path.exists(candidate_path):
        return want

    from matcher import SYN_GROUPS  # noqa: E402
    group = next((g for g in SYN_GROUPS if primary in g), None)
    if group:
        for syn in group:
            fname = syn + ".json"
            if os.path.exists(os.path.join(dir_path, fname)):
                return fname

    for fn in os.listdir(dir_path):
        if fn.lower().endswith('.json') and base.lower() in fn.lower():
            return fn

    return None


def get_all_canonical_countries(source_directories: list[tuple[str, str]]) -> set[str]:
    """
    Scan each source directory for .json files and return the set of all canonical names.
    """
    all_countries: set[str] = set()

    for _, source_dir in source_directories:
        if not os.path.isdir(source_dir):
            continue
        for fname in os.listdir(source_dir):
            if not fname.lower().endswith('.json'):
                continue
            raw = os.path.splitext(fname)[0]
            canon = canonical(raw)
            all_countries.add(canon)

    return all_countries


def get_country_file_paths(
    country_name: str,
    source_directories: list[tuple[str, str]]
) -> dict[str, list[str]]:
    """
    For a given canonical country_name, find all matching JSON file paths in each source directory.
    """
    paths: dict[str, list[str]] = {}

    for src_name, src_dir in source_directories:
        if not os.path.isdir(src_dir):
            continue

        matching_files: list[str] = []
        for fname in os.listdir(src_dir):
            if not fname.lower().endswith('.json'):
                continue
            raw = os.path.splitext(fname)[0]
            if canonical(raw) == country_name:
                matching_files.append(os.path.join(src_dir, fname))

        if matching_files:
            paths[src_name] = matching_files

    return paths


def load_activity_data(tracker_path: str) -> dict[str, str]:
    """
    Loads the activity tracker data from a JSON file.
    The data is a dictionary mapping unique_id -> first_seen_timestamp (ISO format).
    Returns an empty dictionary if the file doesn't exist or is invalid.
    """
    if not os.path.exists(tracker_path):
        return {}
    try:
        with open(tracker_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load or parse activity tracker file {tracker_path}. Starting fresh. Error: {e}")
        return {}


def save_activity_data(tracker_path: str, data: dict[str, str]):
    """
    Saves the activity tracker data to a JSON file.
    """
    try:
        with open(tracker_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        print(f"Error: Could not save activity tracker file to {tracker_path}. Error: {e}")

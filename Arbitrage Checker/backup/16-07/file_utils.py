# file_utils.py

import os
import shutil
import json
from datetime import datetime, timezone
from typing import Dict, Any, Set, List, Tuple, Optional

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


def cleanup_old_files(output_dir: str, updated_filenames: Set[str]):
    """
    Deletes .json files from the output directory that were not generated or updated in the current run.

    Args:
        output_dir (str): The path to the directory to clean.
        updated_filenames (set): A set of filenames (e.g., {"France.json", "Spain.json"})
                                 that were created or updated in this run.
    """
    if not os.path.isdir(output_dir):
        # Allow silent failure if directory doesn't exist, e.g. on first run.
        return

    deleted_count = 0
    # Create a set of all files that should be preserved.
    files_to_preserve = updated_filenames | {"activity_tracker.json", "unconfirmed_opportunities.json"}

    for filename in os.listdir(output_dir):
        # Only clean up .json files, leave other files/folders (like _cache) alone
        if filename.lower().endswith('.json'):
            # Check against the full set of preserved files.
            if filename not in files_to_preserve:
                try:
                    file_path = os.path.join(output_dir, filename)
                    os.remove(file_path)
                    print(f"Deleted stale file: {filename}")
                    deleted_count += 1
                except OSError as e:
                    print(f"Error deleting file {filename}: {e}")
    if deleted_count > 0:
        print(f"Deleted {deleted_count} old file(s).")


def load_matches(filename: str) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
    """
    Load matches from a JSON file and extracts the 'last_updated' timestamp.
    Injects "country" and "country_name" fields into each match.
    Returns a tuple: (list of match dictionaries, last_updated_datetime).
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse JSON file {filename}: {e}. Skipping this file.")
                return [], None
    except (IOError, OSError) as e:
        print(f"Warning: Could not open file {filename}: {e}. Skipping this file.")
        return [], None

    matches = []
    last_updated_dt = None
    base_filename = os.path.splitext(os.path.basename(filename))[0]
    canonical_country = canonical(base_filename)

    # Function to parse the timestamp string into a datetime object
    def parse_timestamp(ts_str):
        try:
            # fromisoformat handles timezone-aware strings correctly
            return datetime.fromisoformat(ts_str)
        except (ValueError, TypeError):
            return None

    # Handle different possible JSON structures (list or dict)
    if isinstance(data, list):
        # Look for the last_updated dict, which is often at the start
        temp_data_list = []
        for item in data:
            if isinstance(item, dict) and 'last_updated' in item:
                last_updated_dt = parse_timestamp(item['last_updated'])
            else:
                temp_data_list.append(item)
        data = temp_data_list # The rest of the list is data
    elif isinstance(data, dict) and 'last_updated' in data:
        last_updated_dt = parse_timestamp(data.pop('last_updated'))

    # Process the remaining data which should contain matches
    if isinstance(data, dict):
        # Case: Root is a single tournament object
        tournament_id = data.get("tournament_id")
        tournament_name = data.get("tournament_name")
        match_list = data.get("matches", [data] if "match_id" in data else [])

        for match in match_list:
            match["country"] = canonical_country
            match["country_name"] = base_filename
            if tournament_id: match["tournament_id"] = tournament_id
            if tournament_name: match["tournament_name"] = tournament_name
        matches.extend(match_list)

    elif isinstance(data, list):
        # Case: Root is a list of tournaments or matches
        for element in data:
            if isinstance(element, dict) and "matches" in element:
                tournament_id = element.get("tournament_id")
                tournament_name = element.get("tournament_name")
                for match in element["matches"]:
                    match["country"] = canonical_country
                    match["country_name"] = base_filename
                    if tournament_id: match["tournament_id"] = tournament_id
                    if tournament_name: match["tournament_name"] = tournament_name
                matches.extend(element["matches"])
            elif isinstance(element, dict) and "match_id" in element:
                element["country"] = canonical_country
                element["country_name"] = base_filename
                matches.append(element)

    return matches, last_updated_dt


# --- The rest of the file_utils.py remains unchanged ---
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


def get_all_canonical_countries(source_directories: List[tuple[str, str]]) -> Set[str]:
    """
    Scan each source directory for .json files and return the set of all canonical names.
    """
    all_countries: Set[str] = set()

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
    source_directories: List[tuple[str, str]]
) -> Dict[str, List[str]]:
    """
    For a given canonical country_name, find all matching JSON file paths in each source directory.
    """
    paths: Dict[str, List[str]] = {}

    for src_name, src_dir in source_directories:
        if not os.path.isdir(src_dir):
            continue

        matching_files: List[str] = []
        for fname in os.listdir(src_dir):
            if not fname.lower().endswith('.json'):
                continue
            raw = os.path.splitext(fname)[0]
            if canonical(raw) == country_name:
                matching_files.append(os.path.join(src_dir, fname))

        if matching_files:
            paths[src_name] = matching_files

    return paths


def load_activity_data(tracker_path: str) -> Dict[str, Any]:
    """
    Loads the activity tracker data from a JSON file.
    Returns an empty dictionary if the file doesn't exist or is invalid.
    """
    if not os.path.exists(tracker_path):
        return {}
    try:
        with open(tracker_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            data = json.load(f)
            # Validate structure
            if not isinstance(data, dict):
                print(f"Warning: Expected dict in activity tracker, got {type(data)}. Resetting.")
                return {}
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load or parse activity tracker file {tracker_path}. Starting fresh. Error: {e}")
        return {}


def save_activity_data(tracker_path: str, data: Dict[str, Any]):
    """
    Saves the activity tracker data to a JSON file.
    """
    try:
        os.makedirs(os.path.dirname(tracker_path), exist_ok=True)
        with open(tracker_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"Error: Could not save activity tracker file to {tracker_path}. Error: {e}")


def load_json_from_file(file_path: str) -> Dict[str, Any]:
    """
    Loads data from a generic JSON file, used for caching.
    Returns an empty dictionary if the file doesn't exist or is invalid.
    """
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            data = json.loads(content)
            # Validate that it's a dictionary
            if not isinstance(data, dict):
                print(f"Warning: Expected dict in {file_path}, got {type(data)}. Resetting to empty dict.")
                return {}
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load or parse cache file {file_path}. Starting fresh. Error: {e}")
        return {}


def save_json_to_file(file_path: str, data: Dict[str, Any]):
    """
    Saves a dictionary to a JSON file with indentation, used for caching.
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"Error: Could not save cache file to {file_path}. Error: {e}")
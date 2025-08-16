"""Cache management functionality for Parquet file operations."""

from typing import List, Dict, Tuple, Set
from pathlib import Path
import polars
import config


def get_cached_data(
    ids: List[int], columns: List[str], table_name: str
) -> Tuple[polars.DataFrame, Set[int]]:
    """
    Return cached data and identify IDs that are missing or incomplete for the requested columns.

    Args:
        ids: List of integer IDs to look up
        columns: List of column names to check for (including JSON dot notation)
        table_name: Table identifier in format "schema_name.table_name"

    Returns:
        Tuple of (found_data, missing_or_incomplete_ids) where:
        - found_data: Polars DataFrame with available data for complete IDs
        - missing_or_incomplete_ids: Set of IDs that are missing or don't have all requested columns
    """
    cache_path = get_table_cache_path(table_name)

    # If cache file doesn't exist, all IDs are missing
    if not cache_path.exists():
        return polars.DataFrame(), set(ids)

    try:
        # Read the cache file
        cached_df = polars.read_parquet(cache_path)

        # If cache is empty, all IDs are missing
        if cached_df.height == 0:
            return polars.DataFrame(), set(ids)

        # Filter for requested IDs
        available_df = cached_df.filter(polars.col("id").is_in(ids))

        # Check which columns are available in the cache
        available_columns = set(cached_df.columns)
        requested_columns = set(columns)

        # Find IDs that have all requested columns
        complete_ids = set()
        missing_or_incomplete_ids = set(ids)

        if available_df.height > 0:
            # Check if all requested columns exist in cache
            missing_columns = requested_columns - available_columns

            if not missing_columns:
                # All columns exist, check which IDs are present
                cached_ids = set(available_df["id"].to_list())
                complete_ids = cached_ids.intersection(set(ids))
                missing_or_incomplete_ids = set(ids) - complete_ids

                # Return data for complete IDs only
                if complete_ids:
                    result_df = available_df.filter(
                        polars.col("id").is_in(list(complete_ids))
                    )
                    return result_df.select(["id"] + columns), missing_or_incomplete_ids
            else:
                # Some columns are missing, so all IDs are considered incomplete
                missing_or_incomplete_ids = set(ids)

        return polars.DataFrame(), missing_or_incomplete_ids

    except Exception as e:
        # If there's any error reading the cache, treat all IDs as missing
        print(f"Warning: Error reading cache file {cache_path}: {e}")
        return polars.DataFrame(), set(ids)


def store_data(data: List[Dict], columns: List[str], table_name: str) -> None:
    """
    Store new data in cache, merging with existing data for specific table.

    Uses insert-or-replace logic: if an ID already exists in cache, replace the entire row
    with the new data. This handles the simplified approach where incomplete IDs are
    completely re-fetched.

    Args:
        data: List of dictionaries containing the fetched data
        columns: List of column names that were requested (for validation)
        table_name: Table identifier in format "schema_name.table_name"
    """
    if not data:
        return  # Nothing to store

    # Ensure cache directory exists
    ensure_cache_directory()

    cache_path = get_table_cache_path(table_name)

    # Convert data to Polars DataFrame
    new_df = polars.DataFrame(data)

    # Ensure 'id' column exists and is first
    if "id" not in new_df.columns:
        raise ValueError("Data must contain 'id' column")

    # Reorder columns to have 'id' first
    other_columns = [col for col in new_df.columns if col != "id"]
    new_df = new_df.select(["id"] + other_columns)

    if cache_path.exists():
        try:
            # Read existing cache
            existing_df = polars.read_parquet(cache_path)

            # Get IDs that are being updated
            new_ids = set(new_df["id"].to_list())

            # Remove existing rows for these IDs (insert-or-replace logic)
            updated_existing_df = existing_df.filter(
                ~polars.col("id").is_in(list(new_ids))
            )

            # Combine existing data (without updated IDs) with new data
            combined_df = polars.concat([updated_existing_df, new_df], how="diagonal")

            # Sort by ID for consistent ordering
            combined_df = combined_df.sort("id")

        except Exception as e:
            print(f"Warning: Error reading existing cache file {cache_path}: {e}")
            print("Creating new cache file with current data")
            combined_df = new_df.sort("id")
    else:
        # No existing cache, just use new data
        combined_df = new_df.sort("id")

    # Write the combined data back to cache
    try:
        combined_df.write_parquet(cache_path)
    except Exception as e:
        raise RuntimeError(f"Failed to write cache file {cache_path}: {e}")


def expand_cache_files(table_name: str) -> None:
    """Split cache into multiple files when size limits are reached."""
    # Implementation will be added in later tasks
    pass


def get_table_cache_path(table_name: str) -> Path:
    """
    Get the cache file path for a specific table.

    Args:
        table_name: Table identifier in format "schema_name.table_name"

    Returns:
        Path to the Parquet cache file for this table

    Examples:
        >>> get_table_cache_path("public.users")
        Path("~/data/cached_data/public_users.parquet")
        >>> get_table_cache_path("analytics.events")
        Path("~/data/cached_data/analytics_events.parquet")
    """
    cache_dir = config.CACHED_DATA_FOLDER

    # Convert table_name to safe filename by replacing dots with underscores
    safe_filename = table_name.replace(".", "_") + ".parquet"

    return cache_dir / safe_filename


def ensure_cache_directory() -> None:
    """
    Ensure the cache directory exists, creating it if necessary.

    Creates the full directory path including any parent directories.
    """
    cache_dir = config.CACHED_DATA_FOLDER
    cache_dir.mkdir(parents=True, exist_ok=True)

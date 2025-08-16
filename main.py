"""Main API interface for the database cache query system."""

from typing import List, Callable, Dict
import polars
import cache_manager
import batch_processor
import json_parser


class DataNotFoundError(ValueError):
    """Raised when requested IDs are not found in cache or database."""

    pass


class ColumnNotFoundError(ValueError):
    """Raised when requested columns are not available in database."""

    pass


def get_data(
    ids: List[int], columns: List[str], table_name: str, fetcher: Callable
) -> polars.DataFrame:
    """
    Retrieve data for specified IDs and columns, using cache when available.

    This function orchestrates the entire process:
    1. Check cache using cache_manager.get_cached_data()
    2. Fetch missing data using batch_processor.fetch_missing_data()
    3. Store new data using cache_manager.store_data()
    4. Return complete dataset

    Args:
        ids: List of integer IDs to retrieve
        columns: List of column names (supports dot notation for JSON fields)
        table_name: Table identifier in format "schema_name.table_name"
        fetcher: Callable that takes (ids: List[int], table_name: str) -> List[Dict[str, Any]]

    Returns:
        Polars DataFrame with requested data

    Raises:
        DataNotFoundError: If IDs not found in cache or database
        ColumnNotFoundError: If columns not available in database
        ValueError: If input validation fails
    """
    # Input validation
    if not ids:
        return polars.DataFrame()

    if not columns:
        raise ValueError("At least one column must be specified")

    if not table_name or "." not in table_name:
        raise ValueError("table_name must be in format 'schema_name.table_name'")

    # Validate that all IDs are integers
    if not all(isinstance(id, int) for id in ids):
        raise ValueError("All IDs must be integers")

    # Parse column names to identify JSON fields and base columns
    base_columns = set()
    json_fields = {}  # Maps full column name to (base_column, json_path)

    for column in columns:
        base_column, json_path = json_parser.parse_column_name(column)
        base_columns.add(base_column)
        if json_path is not None:
            json_fields[column] = (base_column, json_path)

    # Step 1: Check cache for existing data
    cached_df, missing_or_incomplete_ids = cache_manager.get_cached_data(
        ids, list(base_columns), table_name
    )

    # Step 2: Fetch missing data if needed
    if missing_or_incomplete_ids:
        try:
            fetched_data = batch_processor.fetch_missing_data(
                list(missing_or_incomplete_ids), list(base_columns), table_name, fetcher
            )

            # Validate that we got data for all requested IDs
            if fetched_data:
                fetched_ids = {item["id"] for item in fetched_data}
                still_missing_ids = missing_or_incomplete_ids - fetched_ids

                if still_missing_ids:
                    raise DataNotFoundError(
                        f"IDs not found in database: {sorted(still_missing_ids)}"
                    )

                # Check that all base columns are present in fetched data
                if fetched_data:
                    available_columns = set(fetched_data[0].keys())
                    missing_columns = base_columns - available_columns
                    if missing_columns:
                        raise ColumnNotFoundError(
                            f"Columns not available in database: {sorted(missing_columns)}"
                        )

                # Step 3: Store new data in cache
                cache_manager.store_data(fetched_data, list(base_columns), table_name)
            else:
                # No data was fetched, all IDs are missing
                raise DataNotFoundError(
                    f"IDs not found in database: {sorted(missing_or_incomplete_ids)}"
                )

        except Exception as e:
            if isinstance(e, (DataNotFoundError, ColumnNotFoundError)):
                raise
            else:
                raise RuntimeError(f"Error fetching data: {e}")

    # Step 4: Get final data from cache (now includes newly fetched data)
    final_cached_df, remaining_missing = cache_manager.get_cached_data(
        ids, list(base_columns), table_name
    )

    if remaining_missing:
        raise DataNotFoundError(f"IDs not found: {sorted(remaining_missing)}")

    # Step 5: Process JSON fields if needed
    if json_fields:
        final_cached_df = _process_json_fields(final_cached_df, json_fields)

    # Step 6: Select only requested columns and return
    final_columns = ["id"] + columns
    available_final_columns = [
        col for col in final_columns if col in final_cached_df.columns
    ]

    return final_cached_df.select(available_final_columns).sort("id")


def _process_json_fields(
    df: polars.DataFrame, json_fields: Dict[str, tuple]
) -> polars.DataFrame:
    """
    Process JSON fields by extracting nested values using dot notation.

    Args:
        df: DataFrame with base columns
        json_fields: Dict mapping full column name to (base_column, json_path)

    Returns:
        DataFrame with additional columns for JSON fields
    """
    result_df = df.clone()

    for full_column_name, (base_column, json_path) in json_fields.items():
        if base_column in df.columns:
            # Extract JSON values for each row
            json_values = []

            for row in df.iter_rows(named=True):
                base_value = row[base_column]
                try:
                    if base_value is not None:
                        extracted_value = json_parser.extract_json_value(
                            base_value, json_path
                        )
                        json_values.append(extracted_value)
                    else:
                        json_values.append(None)
                except json_parser.JSONParseError:
                    # If JSON parsing fails, use None
                    json_values.append(None)

            # Add the new column to the result DataFrame
            result_df = result_df.with_columns(
                polars.Series(full_column_name, json_values)
            )

    return result_df

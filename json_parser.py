"""
JSON parsing utilities for handling dot notation column names.

This module provides functions to parse column names with dot notation
(e.g., "vendor.address.city") and extract values from JSON data.
"""

import json
from typing import Tuple, Optional, Any, Union


class JSONParseError(ValueError):
    """Raised when JSON parsing fails for dot notation columns."""

    pass


def parse_column_name(column: str) -> Tuple[str, Optional[str]]:
    """
    Parse column name to identify base column and JSON path.

    Args:
        column: Column name, potentially with dot notation (e.g., "vendor.address.city")

    Returns:
        Tuple of (base_column, json_path) where:
        - base_column: The actual database column name (e.g., "vendor")
        - json_path: The JSON path after the first dot (e.g., "address.city"), or None if no dots

    Examples:
        >>> parse_column_name("name")
        ("name", None)
        >>> parse_column_name("vendor.address.city")
        ("vendor", "address.city")
        >>> parse_column_name("metadata.user_id")
        ("metadata", "user_id")
    """
    if "." not in column:
        return column, None

    parts = column.split(".", 1)  # Split on first dot only
    base_column = parts[0]
    json_path = parts[1]

    return base_column, json_path


def extract_json_value(data: Union[str, dict, Any], json_path: str) -> Any:
    """
    Extract value from JSON data using dot notation.

    Args:
        data: JSON data as string, dictionary, or other type
        json_path: Dot-separated path to extract (e.g., "address.city")

    Returns:
        The extracted value from the JSON path

    Raises:
        JSONParseError: If JSON parsing fails or path doesn't exist

    Examples:
        >>> extract_json_value('{"address": {"city": "NYC"}}', "address.city")
        "NYC"
        >>> extract_json_value({"user": {"name": "John"}}, "user.name")
        "John"
        >>> extract_json_value({"settings": {"theme": "dark"}}, "settings.theme")
        "dark"
    """
    # Handle string JSON data
    if isinstance(data, str):
        try:
            parsed_data = json.loads(data)
        except json.JSONDecodeError as e:
            raise JSONParseError(f"Failed to parse JSON string: {e}")
    elif isinstance(data, dict):
        parsed_data = data
    else:
        # If data is not string or dict, try to convert to dict
        try:
            parsed_data = dict(data)
        except (TypeError, ValueError):
            raise JSONParseError(
                f"Cannot extract JSON path from data type: {type(data)}"
            )

    # Handle empty path - return the entire parsed data
    if not json_path:
        return parsed_data

    # Navigate through the JSON path
    current_value = parsed_data
    path_parts = json_path.split(".")

    try:
        for part in path_parts:
            if isinstance(current_value, dict):
                current_value = current_value[part]
            else:
                raise JSONParseError(
                    f"Cannot access key '{part}' on non-dict value: {type(current_value)}"
                )
    except KeyError as e:
        raise JSONParseError(f"JSON path '{json_path}' not found: missing key {e}")
    except TypeError as e:
        raise JSONParseError(f"JSON path '{json_path}' navigation failed: {e}")

    return current_value

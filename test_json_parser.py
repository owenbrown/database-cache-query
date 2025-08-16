"""
Unit tests for json_parser module.
"""

import pytest
from json_parser import parse_column_name, extract_json_value, JSONParseError


class TestParseColumnName:
    """Test cases for parse_column_name function."""

    def test_simple_column_name(self):
        """Test parsing simple column names without dots."""
        result = parse_column_name("name")
        assert result == ("name", None)

        result = parse_column_name("id")
        assert result == ("id", None)

        result = parse_column_name("user_count")
        assert result == ("user_count", None)

    def test_single_dot_notation(self):
        """Test parsing column names with single dot."""
        result = parse_column_name("vendor.name")
        assert result == ("vendor", "name")

        result = parse_column_name("metadata.user_id")
        assert result == ("metadata", "user_id")

        result = parse_column_name("config.enabled")
        assert result == ("config", "enabled")

    def test_nested_dot_notation(self):
        """Test parsing column names with multiple dots (nested JSON)."""
        result = parse_column_name("vendor.address.city")
        assert result == ("vendor", "address.city")

        result = parse_column_name("user.profile.settings.theme")
        assert result == ("user", "profile.settings.theme")

        result = parse_column_name("data.nested.very.deep.value")
        assert result == ("data", "nested.very.deep.value")

    def test_edge_cases(self):
        """Test edge cases for column name parsing."""
        # Empty string
        result = parse_column_name("")
        assert result == ("", None)

        # Single dot at end
        result = parse_column_name("column.")
        assert result == ("column", "")

        # Single dot at start
        result = parse_column_name(".field")
        assert result == ("", "field")

        # Multiple consecutive dots
        result = parse_column_name("data..field")
        assert result == ("data", ".field")


class TestExtractJsonValue:
    """Test cases for extract_json_value function."""

    def test_extract_from_json_string(self):
        """Test extracting values from JSON strings."""
        json_str = '{"address": {"city": "NYC", "state": "NY"}, "name": "John"}'

        result = extract_json_value(json_str, "address.city")
        assert result == "NYC"

        result = extract_json_value(json_str, "address.state")
        assert result == "NY"

        result = extract_json_value(json_str, "name")
        assert result == "John"

    def test_extract_from_dict(self):
        """Test extracting values from dictionary objects."""
        data = {
            "user": {"profile": {"name": "Alice", "age": 30}},
            "settings": {"theme": "dark", "notifications": True},
        }

        result = extract_json_value(data, "user.profile.name")
        assert result == "Alice"

        result = extract_json_value(data, "user.profile.age")
        assert result == 30

        result = extract_json_value(data, "settings.theme")
        assert result == "dark"

        result = extract_json_value(data, "settings.notifications")
        assert result is True

    def test_extract_nested_objects(self):
        """Test extracting nested objects and arrays."""
        data = {
            "vendor": {
                "address": {"city": "Boston", "zip": "02101"},
                "contacts": [{"type": "email", "value": "test@example.com"}],
            }
        }

        # Extract nested object
        result = extract_json_value(data, "vendor.address")
        assert result == {"city": "Boston", "zip": "02101"}

        # Extract from nested object
        result = extract_json_value(data, "vendor.address.city")
        assert result == "Boston"

        # Extract array
        result = extract_json_value(data, "vendor.contacts")
        assert result == [{"type": "email", "value": "test@example.com"}]

    def test_invalid_json_string(self):
        """Test error handling for invalid JSON strings."""
        with pytest.raises(JSONParseError, match="Failed to parse JSON string"):
            extract_json_value('{"invalid": json}', "invalid")

        with pytest.raises(JSONParseError, match="Failed to parse JSON string"):
            extract_json_value("not json at all", "field")

    def test_missing_json_path(self):
        """Test error handling for missing JSON paths."""
        data = {"user": {"name": "John"}}

        with pytest.raises(JSONParseError, match="JSON path 'user.age' not found"):
            extract_json_value(data, "user.age")

        with pytest.raises(JSONParseError, match="JSON path 'missing.field' not found"):
            extract_json_value(data, "missing.field")

    def test_invalid_path_navigation(self):
        """Test error handling for invalid path navigation."""
        data = {"user": {"name": "John", "age": 30}}

        # Try to access key on non-dict value
        with pytest.raises(
            JSONParseError, match="Cannot access key 'field' on non-dict value"
        ):
            extract_json_value(data, "user.name.field")

        with pytest.raises(
            JSONParseError, match="Cannot access key 'something' on non-dict value"
        ):
            extract_json_value(data, "user.age.something")

    def test_non_string_non_dict_data(self):
        """Test handling of data that's neither string nor dict."""
        # Test with list (should fail)
        with pytest.raises(
            JSONParseError, match="Cannot extract JSON path from data type"
        ):
            extract_json_value([1, 2, 3], "field")

        # Test with number (should fail)
        with pytest.raises(
            JSONParseError, match="Cannot extract JSON path from data type"
        ):
            extract_json_value(42, "field")

    def test_empty_json_path(self):
        """Test handling of empty JSON path."""
        data = {"field": "value"}

        # Empty path should return the original data
        result = extract_json_value(data, "")
        assert result == data

    def test_single_level_path(self):
        """Test single-level JSON paths (no dots)."""
        data = {"name": "Alice", "age": 25}

        result = extract_json_value(data, "name")
        assert result == "Alice"

        result = extract_json_value(data, "age")
        assert result == 25


if __name__ == "__main__":
    pytest.main([__file__])

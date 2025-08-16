"""
Unit tests for main module (get_data function).
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch
import polars
from main import get_data, DataNotFoundError, ColumnNotFoundError


class TestGetDataFunction:
    """Test cases for the main get_data function."""

    def test_get_data_empty_ids(self):
        """Test get_data with empty IDs list."""

        def mock_fetcher(ids, table_name):
            return []

        result = get_data([], ["name"], "schema.table", mock_fetcher)
        assert result.height == 0

    def test_get_data_input_validation(self):
        """Test input validation for get_data function."""

        def mock_fetcher(ids, table_name):
            return []

        # Test empty columns
        with pytest.raises(ValueError, match="At least one column must be specified"):
            get_data([1, 2], [], "schema.table", mock_fetcher)

        # Test invalid table name format
        with pytest.raises(ValueError, match="table_name must be in format"):
            get_data([1, 2], ["name"], "invalid_table_name", mock_fetcher)

        # Test non-integer IDs
        with pytest.raises(ValueError, match="All IDs must be integers"):
            get_data([1, "2", 3], ["name"], "schema.table", mock_fetcher)

    def test_get_data_cache_miss_simple(self):
        """Test get_data when no data is in cache (simple case)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):

                def mock_fetcher(ids, table_name):
                    return [
                        {"id": 1, "name": "Alice", "age": 25},
                        {"id": 2, "name": "Bob", "age": 30},
                    ]

                result = get_data([1, 2], ["name", "age"], "schema.users", mock_fetcher)

                assert result.height == 2
                assert set(result["id"].to_list()) == {1, 2}
                assert "name" in result.columns
                assert "age" in result.columns

                # Verify data was cached
                cache_path = test_cache_dir / "schema_users.parquet"
                assert cache_path.exists()

    def test_get_data_cache_hit(self):
        """Test get_data when all data is in cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Pre-populate cache
            cache_data = polars.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
            cache_path = test_cache_dir / "schema_users.parquet"
            cache_data.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):

                def mock_fetcher(ids, table_name):
                    # Should not be called since data is in cache
                    raise Exception("Fetcher should not be called")

                result = get_data([1, 2], ["name", "age"], "schema.users", mock_fetcher)

                assert result.height == 2
                assert set(result["id"].to_list()) == {1, 2}
                assert (
                    result.filter(polars.col("id") == 1)["name"].to_list()[0] == "Alice"
                )
                assert (
                    result.filter(polars.col("id") == 2)["name"].to_list()[0] == "Bob"
                )

    def test_get_data_json_fields(self):
        """Test get_data with JSON field extraction using dot notation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):

                def mock_fetcher(ids, table_name):
                    return [
                        {
                            "id": 1,
                            "name": "Alice",
                            "metadata": '{"address": {"city": "NYC", "state": "NY"}, "age": 25}',
                        },
                        {
                            "id": 2,
                            "name": "Bob",
                            "metadata": '{"address": {"city": "LA", "state": "CA"}, "age": 30}',
                        },
                    ]

                result = get_data(
                    [1, 2],
                    ["name", "metadata.address.city", "metadata.age"],
                    "schema.users",
                    mock_fetcher,
                )

                assert result.height == 2
                assert "name" in result.columns
                assert "metadata.address.city" in result.columns
                assert "metadata.age" in result.columns

                # Check extracted JSON values
                alice_row = result.filter(polars.col("id") == 1)
                assert alice_row["metadata.address.city"].to_list()[0] == "NYC"
                assert alice_row["metadata.age"].to_list()[0] == 25

                bob_row = result.filter(polars.col("id") == 2)
                assert bob_row["metadata.address.city"].to_list()[0] == "LA"
                assert bob_row["metadata.age"].to_list()[0] == 30

    def test_get_data_missing_ids_error(self):
        """Test error handling when IDs are not found in database."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):

                def mock_fetcher(ids, table_name):
                    # Only return data for some IDs
                    return [{"id": 1, "name": "Alice"}]

                with pytest.raises(
                    DataNotFoundError, match="IDs not found in database: \\[2, 3\\]"
                ):
                    get_data([1, 2, 3], ["name"], "schema.users", mock_fetcher)

    def test_get_data_missing_columns_error(self):
        """Test error handling when columns are not available in database."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):

                def mock_fetcher(ids, table_name):
                    # Return data without requested column
                    return [{"id": 1, "name": "Alice"}]  # Missing 'age' column

                with pytest.raises(
                    ColumnNotFoundError,
                    match="Columns not available in database: \\['age'\\]",
                ):
                    get_data([1], ["name", "age"], "schema.users", mock_fetcher)

    def test_get_data_fetcher_error_handling(self):
        """Test error handling when fetcher function fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):

                def failing_fetcher(ids, table_name):
                    raise Exception("Database connection failed")

                with pytest.raises(RuntimeError, match="Error fetching data"):
                    get_data([1, 2], ["name"], "schema.users", failing_fetcher)


if __name__ == "__main__":
    # Simple test runner
    import sys

    test_main = TestGetDataFunction()
    test_main.test_get_data_empty_ids()
    test_main.test_get_data_input_validation()
    test_main.test_get_data_cache_miss_simple()
    test_main.test_get_data_cache_hit()
    test_main.test_get_data_json_fields()
    test_main.test_get_data_missing_ids_error()
    test_main.test_get_data_missing_columns_error()
    test_main.test_get_data_fetcher_error_handling()

    print("All main function tests passed!")
    sys.exit(0)

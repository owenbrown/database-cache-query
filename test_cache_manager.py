"""
Unit tests for cache_manager module.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch
import polars
from cache_manager import get_table_cache_path, ensure_cache_directory


class TestCacheFileOperations:
    """Test cases for basic cache file operations."""

    def test_get_table_cache_path_simple(self):
        """Test path generation for simple table names."""
        with patch("config.CACHED_DATA_FOLDER", Path("/tmp/test_cache")):
            result = get_table_cache_path("public.users")
            expected = Path("/tmp/test_cache/public_users.parquet")
            assert result == expected

    def test_get_table_cache_path_complex(self):
        """Test path generation for complex table names."""
        with patch("config.CACHED_DATA_FOLDER", Path("/tmp/test_cache")):
            result = get_table_cache_path("analytics.user_events")
            expected = Path("/tmp/test_cache/analytics_user_events.parquet")
            assert result == expected

            result = get_table_cache_path("reporting.daily_stats")
            expected = Path("/tmp/test_cache/reporting_daily_stats.parquet")
            assert result == expected

    def test_get_table_cache_path_multiple_dots(self):
        """Test path generation for table names with multiple dots."""
        with patch("config.CACHED_DATA_FOLDER", Path("/tmp/test_cache")):
            result = get_table_cache_path("schema.sub.table")
            expected = Path("/tmp/test_cache/schema_sub_table.parquet")
            assert result == expected

    def test_ensure_cache_directory_creates_directory(self):
        """Test that ensure_cache_directory creates the directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "test_cache" / "nested"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                # Directory should not exist initially
                assert not test_cache_dir.exists()

                # Call ensure_cache_directory
                ensure_cache_directory()

                # Directory should now exist
                assert test_cache_dir.exists()
                assert test_cache_dir.is_dir()

    def test_ensure_cache_directory_existing_directory(self):
        """Test that ensure_cache_directory works with existing directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "existing_cache"
            test_cache_dir.mkdir()

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                # Directory already exists
                assert test_cache_dir.exists()

                # Call ensure_cache_directory (should not raise error)
                ensure_cache_directory()

                # Directory should still exist
                assert test_cache_dir.exists()
                assert test_cache_dir.is_dir()

    def test_ensure_cache_directory_creates_parents(self):
        """Test that ensure_cache_directory creates parent directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "parent" / "child" / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                # No directories should exist initially
                assert not test_cache_dir.exists()
                assert not test_cache_dir.parent.exists()

                # Call ensure_cache_directory
                ensure_cache_directory()

                # All directories should now exist
                assert test_cache_dir.exists()
                assert test_cache_dir.parent.exists()
                assert test_cache_dir.is_dir()


class TestCacheDataRetrieval:
    """Test cases for cache data retrieval functionality."""

    def test_get_cached_data_no_cache_file(self):
        """Test behavior when cache file doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import get_cached_data

                result_df, missing_ids = get_cached_data(
                    [1, 2, 3], ["name", "age"], "test.table"
                )

                # Should return empty DataFrame and all IDs as missing
                assert result_df.height == 0
                assert missing_ids == {1, 2, 3}

    def test_get_cached_data_empty_cache_file(self):
        """Test behavior when cache file exists but is empty."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create empty parquet file
            empty_df = polars.DataFrame(
                {"id": [], "name": [], "age": []},
                schema={"id": polars.Int64, "name": polars.Utf8, "age": polars.Int32},
            )
            cache_path = test_cache_dir / "test_table.parquet"
            empty_df.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import get_cached_data

                result_df, missing_ids = get_cached_data(
                    [1, 2, 3], ["name", "age"], "test.table"
                )

                # Should return empty DataFrame and all IDs as missing
                assert result_df.height == 0
                assert missing_ids == {1, 2, 3}

    def test_get_cached_data_complete_cache_hit(self):
        """Test behavior when all requested data is in cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create cache file with test data
            test_data = polars.DataFrame(
                {
                    "id": [1, 2, 3, 4],
                    "name": ["Alice", "Bob", "Charlie", "David"],
                    "age": [25, 30, 35, 40],
                }
            )
            cache_path = test_cache_dir / "test_table.parquet"
            test_data.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import get_cached_data

                result_df, missing_ids = get_cached_data(
                    [1, 2], ["name", "age"], "test.table"
                )

                # Should return data for IDs 1 and 2, no missing IDs
                assert result_df.height == 2
                assert missing_ids == set()
                assert set(result_df["id"].to_list()) == {1, 2}
                assert "name" in result_df.columns
                assert "age" in result_df.columns

    def test_get_cached_data_partial_cache_hit(self):
        """Test behavior when some requested IDs are not in cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create cache file with test data
            test_data = polars.DataFrame(
                {"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]}
            )
            cache_path = test_cache_dir / "test_table.parquet"
            test_data.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import get_cached_data

                result_df, missing_ids = get_cached_data(
                    [1, 2, 3, 4], ["name", "age"], "test.table"
                )

                # Should return data for IDs 1 and 2, IDs 3 and 4 are missing
                assert result_df.height == 2
                assert missing_ids == {3, 4}
                assert set(result_df["id"].to_list()) == {1, 2}

    def test_get_cached_data_missing_columns(self):
        """Test behavior when requested columns are not in cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create cache file with test data (missing 'email' column)
            test_data = polars.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
            cache_path = test_cache_dir / "test_table.parquet"
            test_data.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import get_cached_data

                result_df, missing_ids = get_cached_data(
                    [1, 2], ["name", "age", "email"], "test.table"
                )

                # Should return empty DataFrame and all IDs as missing due to missing column
                assert result_df.height == 0
                assert missing_ids == {1, 2}

    def test_get_cached_data_corrupted_file(self):
        """Test behavior when cache file is corrupted."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create corrupted file
            cache_path = test_cache_dir / "test_table.parquet"
            cache_path.write_text("This is not a valid parquet file")

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import get_cached_data

                result_df, missing_ids = get_cached_data(
                    [1, 2], ["name", "age"], "test.table"
                )

                # Should handle error gracefully and return all IDs as missing
                assert result_df.height == 0
                assert missing_ids == {1, 2}


class TestCacheDataStorage:
    """Test cases for cache data storage functionality."""

    def test_store_data_new_cache_file(self):
        """Test storing data when no cache file exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import store_data

                test_data = [
                    {"id": 1, "name": "Alice", "age": 25},
                    {"id": 2, "name": "Bob", "age": 30},
                ]

                store_data(test_data, ["name", "age"], "test.table")

                # Check that cache file was created
                cache_path = test_cache_dir / "test_table.parquet"
                assert cache_path.exists()

                # Verify stored data
                stored_df = polars.read_parquet(cache_path)
                assert stored_df.height == 2
                assert set(stored_df["id"].to_list()) == {1, 2}
                assert "name" in stored_df.columns
                assert "age" in stored_df.columns

    def test_store_data_merge_with_existing(self):
        """Test storing data that merges with existing cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create existing cache file
            existing_data = polars.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
            cache_path = test_cache_dir / "test_table.parquet"
            existing_data.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import store_data

                # Add new data
                new_data = [
                    {"id": 4, "name": "David", "age": 40},
                    {"id": 5, "name": "Eve", "age": 45},
                ]

                store_data(new_data, ["name", "age"], "test.table")

                # Verify merged data
                stored_df = polars.read_parquet(cache_path)
                assert stored_df.height == 5
                assert set(stored_df["id"].to_list()) == {1, 2, 3, 4, 5}

    def test_store_data_replace_existing_ids(self):
        """Test insert-or-replace logic for existing IDs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create existing cache file
            existing_data = polars.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
            cache_path = test_cache_dir / "test_table.parquet"
            existing_data.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import store_data

                # Update existing IDs with new data
                updated_data = [
                    {"id": 2, "name": "Robert", "age": 31},  # Updated Bob
                    {"id": 3, "name": "Chuck", "age": 36},  # Updated Charlie
                ]

                store_data(updated_data, ["name", "age"], "test.table")

                # Verify data was replaced
                stored_df = polars.read_parquet(cache_path)
                assert stored_df.height == 3

                # Check that ID 1 is unchanged
                alice_row = stored_df.filter(polars.col("id") == 1)
                assert alice_row["name"].to_list()[0] == "Alice"
                assert alice_row["age"].to_list()[0] == 25

                # Check that ID 2 was updated
                bob_row = stored_df.filter(polars.col("id") == 2)
                assert bob_row["name"].to_list()[0] == "Robert"
                assert bob_row["age"].to_list()[0] == 31

                # Check that ID 3 was updated
                charlie_row = stored_df.filter(polars.col("id") == 3)
                assert charlie_row["name"].to_list()[0] == "Chuck"
                assert charlie_row["age"].to_list()[0] == 36

    def test_store_data_schema_evolution(self):
        """Test adding new columns to existing cache (schema evolution)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create existing cache file with limited columns
            existing_data = polars.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            cache_path = test_cache_dir / "test_table.parquet"
            existing_data.write_parquet(cache_path)

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import store_data

                # Add data with new column
                new_data = [
                    {
                        "id": 3,
                        "name": "Charlie",
                        "age": 35,
                        "email": "charlie@example.com",
                    }
                ]

                store_data(new_data, ["name", "age", "email"], "test.table")

                # Verify schema evolution
                stored_df = polars.read_parquet(cache_path)
                assert stored_df.height == 3
                assert "age" in stored_df.columns
                assert "email" in stored_df.columns

                # Check that existing rows have null values for new columns
                alice_row = stored_df.filter(polars.col("id") == 1)
                assert alice_row["name"].to_list()[0] == "Alice"
                # age and email should be null for existing rows

    def test_store_data_empty_data(self):
        """Test storing empty data list."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import store_data

                # Should not create any files or raise errors
                store_data([], ["name", "age"], "test.table")

                cache_path = test_cache_dir / "test_table.parquet"
                assert not cache_path.exists()

    def test_store_data_missing_id_column(self):
        """Test error handling when data is missing ID column."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import store_data

                # Data without ID column should raise error
                invalid_data = [
                    {"name": "Alice", "age": 25},
                    {"name": "Bob", "age": 30},
                ]

                with pytest.raises(ValueError, match="Data must contain 'id' column"):
                    store_data(invalid_data, ["name", "age"], "test.table")

    def test_store_data_corrupted_existing_cache(self):
        """Test handling of corrupted existing cache file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_cache_dir = Path(temp_dir) / "cache"
            test_cache_dir.mkdir()

            # Create corrupted cache file
            cache_path = test_cache_dir / "test_table.parquet"
            cache_path.write_text("This is not a valid parquet file")

            with patch("config.CACHED_DATA_FOLDER", test_cache_dir):
                from cache_manager import store_data

                new_data = [{"id": 1, "name": "Alice", "age": 25}]

                # Should handle corrupted file gracefully and create new cache
                store_data(new_data, ["name", "age"], "test.table")

                # Verify new cache was created
                stored_df = polars.read_parquet(cache_path)
                assert stored_df.height == 1
                assert stored_df["id"].to_list()[0] == 1


if __name__ == "__main__":
    # Simple test runner
    import sys

    test_ops = TestCacheFileOperations()
    test_ops.test_get_table_cache_path_simple()
    test_ops.test_get_table_cache_path_complex()
    test_ops.test_get_table_cache_path_multiple_dots()
    test_ops.test_ensure_cache_directory_creates_directory()
    test_ops.test_ensure_cache_directory_existing_directory()
    test_ops.test_ensure_cache_directory_creates_parents()

    test_retrieval = TestCacheDataRetrieval()
    test_retrieval.test_get_cached_data_no_cache_file()
    test_retrieval.test_get_cached_data_empty_cache_file()
    test_retrieval.test_get_cached_data_complete_cache_hit()
    test_retrieval.test_get_cached_data_partial_cache_hit()
    test_retrieval.test_get_cached_data_missing_columns()
    test_retrieval.test_get_cached_data_corrupted_file()

    test_storage = TestCacheDataStorage()
    test_storage.test_store_data_new_cache_file()
    test_storage.test_store_data_merge_with_existing()
    test_storage.test_store_data_replace_existing_ids()
    test_storage.test_store_data_schema_evolution()
    test_storage.test_store_data_empty_data()
    test_storage.test_store_data_corrupted_existing_cache()

    print("All cache manager tests passed!")
    sys.exit(0)

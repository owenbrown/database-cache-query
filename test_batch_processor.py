"""
Unit tests for batch_processor module.
"""

import pytest
from batch_processor import calculate_batch_size


class TestBatchSizeCalculation:
    """Test cases for batch size calculation functionality."""

    def test_calculate_batch_size_small_datasets(self):
        """Test batch size calculation for small datasets."""
        # Small datasets should use minimum batch size of 100
        assert calculate_batch_size(50) == 100
        assert calculate_batch_size(100) == 100
        assert calculate_batch_size(99) == 100
        assert calculate_batch_size(1) == 100

    def test_calculate_batch_size_medium_datasets(self):
        """Test batch size calculation for medium datasets."""
        # 500 items: ceil(500/100) = 5, max(100, 5) = 100
        assert calculate_batch_size(500) == 100

        # 1000 items: ceil(1000/100) = 10, max(100, 10) = 100
        assert calculate_batch_size(1000) == 100

        # 5000 items: ceil(5000/100) = 50, max(100, 50) = 100
        assert calculate_batch_size(5000) == 100

        # 10000 items: ceil(10000/100) = 100, max(100, 100) = 100
        assert calculate_batch_size(10000) == 100

    def test_calculate_batch_size_large_datasets(self):
        """Test batch size calculation for large datasets."""
        # 15000 items: ceil(15000/100) = 150, max(100, 150) = 150
        assert calculate_batch_size(15000) == 150

        # 25000 items: ceil(25000/100) = 250, max(100, 250) = 250
        assert calculate_batch_size(25000) == 250

        # 100000 items: ceil(100000/100) = 1000, max(100, 1000) = 1000
        assert calculate_batch_size(100000) == 1000

    def test_calculate_batch_size_very_large_datasets(self):
        """Test batch size calculation for very large datasets."""
        # 1000000 items: ceil(1000000/100) = 10000, max(100, 10000) = 10000
        assert calculate_batch_size(1000000) == 10000

        # 5000000 items: ceil(5000000/100) = 50000, max(100, 50000) = 50000
        assert calculate_batch_size(5000000) == 50000

        # 10000000 items: ceil(10000000/100) = 100000, max(100, 100000) = 100000
        assert calculate_batch_size(10000000) == 100000

    def test_calculate_batch_size_edge_cases(self):
        """Test batch size calculation for edge cases."""
        # Zero items should return minimum batch size
        assert calculate_batch_size(0) == 100

        # Negative items should return minimum batch size
        assert calculate_batch_size(-1) == 100
        assert calculate_batch_size(-100) == 100

    def test_calculate_batch_size_ensures_max_100_batches(self):
        """Test that batch size calculation ensures at most 100 batches."""
        # For any dataset, number of batches should be <= 100
        test_sizes = [101, 500, 1000, 5000, 10000, 15000, 50000, 100000, 1000000]

        for size in test_sizes:
            batch_size = calculate_batch_size(size)
            num_batches = (size + batch_size - 1) // batch_size  # Ceiling division
            assert num_batches <= 100, (
                f"Size {size} with batch_size {batch_size} results in {num_batches} batches"
            )

    def test_calculate_batch_size_ensures_min_100_items(self):
        """Test that batch size calculation ensures minimum 100 items per batch."""
        test_sizes = [1, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 1000000]

        for size in test_sizes:
            batch_size = calculate_batch_size(size)
            assert batch_size >= 100, (
                f"Size {size} resulted in batch_size {batch_size} which is less than 100"
            )

    def test_calculate_batch_size_specific_examples(self):
        """Test specific examples from the requirements."""
        # Examples from the design document

        # 50 missing IDs → batch_size = 100 (1 batch)
        batch_size = calculate_batch_size(50)
        assert batch_size == 100
        num_batches = (50 + batch_size - 1) // batch_size
        assert num_batches == 1

        # 500 missing IDs → batch_size = 100 (5 batches)
        batch_size = calculate_batch_size(500)
        assert batch_size == 100
        num_batches = (500 + batch_size - 1) // batch_size
        assert num_batches == 5

        # 15,000 missing IDs → batch_size = 150 (100 batches)
        batch_size = calculate_batch_size(15000)
        assert batch_size == 150
        num_batches = (15000 + batch_size - 1) // batch_size
        assert num_batches == 100

        # 1,000,000 missing IDs → batch_size = 10,000 (100 batches)
        batch_size = calculate_batch_size(1000000)
        assert batch_size == 10000
        num_batches = (1000000 + batch_size - 1) // batch_size
        assert num_batches == 100


class TestBatchFetching:
    """Test cases for batch fetching functionality."""

    def test_fetch_missing_data_empty_list(self):
        """Test fetching with empty missing IDs list."""
        from batch_processor import fetch_missing_data

        def mock_fetcher(ids, table_name):
            return []

        result = fetch_missing_data([], ["name", "age"], "test.table", mock_fetcher)
        assert result == []

    def test_fetch_missing_data_single_batch(self):
        """Test fetching data that fits in a single batch."""
        from batch_processor import fetch_missing_data

        def mock_fetcher(ids, table_name):
            # Return mock data for the requested IDs
            return [{"id": id, "name": f"User{id}", "age": 20 + id} for id in ids]

        missing_ids = [1, 2, 3]
        result = fetch_missing_data(
            missing_ids, ["name", "age"], "test.table", mock_fetcher
        )

        assert len(result) == 3
        assert result[0]["id"] == 1
        assert result[0]["name"] == "User1"
        assert result[1]["id"] == 2
        assert result[2]["id"] == 3

    def test_fetch_missing_data_multiple_batches(self):
        """Test fetching data that requires multiple batches."""
        from batch_processor import fetch_missing_data

        call_count = 0
        batch_sizes = []

        def mock_fetcher(ids, table_name):
            nonlocal call_count, batch_sizes
            call_count += 1
            batch_sizes.append(len(ids))
            return [{"id": id, "name": f"User{id}", "age": 20 + id} for id in ids]

        # Create a list that will require multiple batches (250 IDs)
        missing_ids = list(range(1, 251))
        result = fetch_missing_data(
            missing_ids, ["name", "age"], "test.table", mock_fetcher
        )

        # Should have called fetcher multiple times
        assert call_count > 1
        assert len(result) == 250

        # Verify all IDs were fetched
        fetched_ids = {item["id"] for item in result}
        assert fetched_ids == set(missing_ids)

    def test_fetch_missing_data_fetcher_validation(self):
        """Test validation of fetcher return type."""
        from batch_processor import fetch_missing_data

        def bad_fetcher(ids, table_name):
            return "not a list"  # Should return list

        with pytest.raises(
            Exception, match="All .* batches failed.*Fetcher must return a list"
        ):
            fetch_missing_data([1, 2, 3], ["name"], "test.table", bad_fetcher)

    def test_fetch_missing_data_partial_batch_failure(self):
        """Test handling of partial batch failures."""
        from batch_processor import fetch_missing_data

        call_count = 0

        def flaky_fetcher(ids, table_name):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # Second batch fails
                raise Exception("Database connection failed")
            return [{"id": id, "name": f"User{id}"} for id in ids]

        # Use enough IDs to ensure multiple batches
        missing_ids = list(range(1, 251))

        # Should not raise exception, but should return partial data
        result = fetch_missing_data(missing_ids, ["name"], "test.table", flaky_fetcher)

        # Should have some data (from successful batches)
        assert len(result) > 0
        assert len(result) < 250  # But not all data due to failed batch

    def test_fetch_missing_data_all_batches_fail(self):
        """Test handling when all batches fail."""
        from batch_processor import fetch_missing_data

        def failing_fetcher(ids, table_name):
            raise Exception("Database is down")

        missing_ids = [1, 2, 3]

        with pytest.raises(Exception, match="All .* batches failed"):
            fetch_missing_data(missing_ids, ["name"], "test.table", failing_fetcher)

    def test_fetch_missing_data_fetcher_receives_correct_params(self):
        """Test that fetcher receives correct parameters."""
        from batch_processor import fetch_missing_data

        received_calls = []

        def tracking_fetcher(ids, table_name):
            received_calls.append((ids, table_name))
            return [{"id": id, "name": f"User{id}"} for id in ids]

        missing_ids = [1, 2, 3]
        table_name = "schema.users"

        fetch_missing_data(missing_ids, ["name", "age"], table_name, tracking_fetcher)

        # Should have been called at least once
        assert len(received_calls) >= 1

        # Check that table_name was passed correctly
        for call_ids, call_table_name in received_calls:
            assert call_table_name == table_name
            assert isinstance(call_ids, list)
            assert all(isinstance(id, int) for id in call_ids)

    def test_fetch_missing_data_batch_size_calculation(self):
        """Test that batch size is calculated correctly."""
        from batch_processor import fetch_missing_data

        batch_sizes = []

        def size_tracking_fetcher(ids, table_name):
            batch_sizes.append(len(ids))
            return [{"id": id, "name": f"User{id}"} for id in ids]

        # Test with 150 IDs (should result in batch size of 100, so 2 batches)
        missing_ids = list(range(1, 151))
        fetch_missing_data(missing_ids, ["name"], "test.table", size_tracking_fetcher)

        # Should have 2 batches: 100 and 50
        assert len(batch_sizes) == 2
        assert batch_sizes[0] == 100
        assert batch_sizes[1] == 50


if __name__ == "__main__":
    # Simple test runner
    import sys

    test_batch = TestBatchSizeCalculation()
    test_batch.test_calculate_batch_size_small_datasets()
    test_batch.test_calculate_batch_size_medium_datasets()
    test_batch.test_calculate_batch_size_large_datasets()
    test_batch.test_calculate_batch_size_very_large_datasets()
    test_batch.test_calculate_batch_size_edge_cases()
    test_batch.test_calculate_batch_size_ensures_max_100_batches()
    test_batch.test_calculate_batch_size_ensures_min_100_items()
    test_batch.test_calculate_batch_size_specific_examples()

    test_fetch = TestBatchFetching()
    test_fetch.test_fetch_missing_data_empty_list()
    test_fetch.test_fetch_missing_data_single_batch()
    test_fetch.test_fetch_missing_data_multiple_batches()
    test_fetch.test_fetch_missing_data_fetcher_validation()
    test_fetch.test_fetch_missing_data_partial_batch_failure()
    test_fetch.test_fetch_missing_data_fetcher_receives_correct_params()
    test_fetch.test_fetch_missing_data_batch_size_calculation()

    print("All batch processor tests passed!")
    sys.exit(0)

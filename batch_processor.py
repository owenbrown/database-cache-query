"""Batch processing functionality with progress tracking."""

from typing import List, Dict, Callable
from tqdm import tqdm


def fetch_missing_data(
    missing_ids: List[int], columns: List[str], table_name: str, fetcher: Callable
) -> List[Dict]:
    """
    Fetch missing data in batches with progress tracking.

    Args:
        missing_ids: List of IDs that need to be fetched
        columns: List of column names to fetch (for reference, not passed to fetcher)
        table_name: Table identifier in format "schema_name.table_name"
        fetcher: Callable that takes (ids: List[int], table_name: str) -> List[Dict[str, Any]]

    Returns:
        List of dictionaries containing the fetched data

    Raises:
        Exception: If fetcher function fails for all batches
    """
    if not missing_ids:
        return []

    batch_size = calculate_batch_size(len(missing_ids))
    all_fetched_data = []
    failed_batches = []

    # Create batches
    batches = []
    for i in range(0, len(missing_ids), batch_size):
        batch = missing_ids[i : i + batch_size]
        batches.append(batch)

    # Process batches with progress tracking
    with tqdm(total=len(batches), desc=f"Fetching {table_name}", unit="batch") as pbar:
        for batch_idx, batch_ids in enumerate(batches):
            try:
                # Call the fetcher function
                batch_data = fetcher(batch_ids, table_name)

                # Validate that fetcher returned a list
                if not isinstance(batch_data, list):
                    raise ValueError(
                        f"Fetcher must return a list, got {type(batch_data)}"
                    )

                # Add batch data to results
                all_fetched_data.extend(batch_data)

                # Update progress
                pbar.set_postfix(
                    {
                        "IDs": f"{len(all_fetched_data)}/{len(missing_ids)}",
                        "Batch": f"{batch_idx + 1}/{len(batches)}",
                    }
                )
                pbar.update(1)

            except Exception as e:
                # Log the error but continue with other batches
                error_info = {
                    "batch_idx": batch_idx,
                    "batch_ids": batch_ids,
                    "error": str(e),
                }
                failed_batches.append(error_info)

                pbar.set_postfix(
                    {
                        "IDs": f"{len(all_fetched_data)}/{len(missing_ids)}",
                        "Batch": f"{batch_idx + 1}/{len(batches)}",
                        "Errors": len(failed_batches),
                    }
                )
                pbar.update(1)

                print(f"Warning: Batch {batch_idx + 1} failed: {e}")

    # Check if we have any successful data
    if not all_fetched_data and failed_batches:
        # All batches failed
        raise Exception(
            f"All {len(failed_batches)} batches failed. First error: {failed_batches[0]['error']}"
        )

    if failed_batches:
        print(f"Warning: {len(failed_batches)} out of {len(batches)} batches failed")

    return all_fetched_data


def calculate_batch_size(missing_count: int) -> int:
    """
    Calculate batch size: max(100, ceil(missing_count / 100)).

    This ensures:
    - Maximum of 100 batches total
    - Minimum batch size of 100 items

    Args:
        missing_count: Number of missing IDs that need to be fetched

    Returns:
        Optimal batch size for the given missing count

    Examples:
        >>> calculate_batch_size(50)
        100
        >>> calculate_batch_size(500)
        100
        >>> calculate_batch_size(15000)
        150
        >>> calculate_batch_size(1000000)
        10000
    """
    import math

    if missing_count <= 0:
        return 100  # Default minimum batch size

    # Calculate batch size to ensure at most 100 batches
    calculated_size = math.ceil(missing_count / 100)

    # Ensure minimum batch size of 100
    return max(100, calculated_size)

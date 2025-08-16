# Database Cache Query System

A high-performance Python library for intelligent database caching with automatic batch processing and JSON field extraction.

## Features

- **Smart Caching**: Automatically caches query results in Parquet format using Polars for optimal performance
- **Batch Processing**: Intelligently batches large queries with configurable sizing and progress tracking
- **JSON Field Support**: Extract nested JSON values using dot notation (e.g., `metadata.address.city`)
- **Cache Persistence**: Data persists between program runs with no invalidation needed
- **Error Handling**: Comprehensive error handling for missing data and invalid requests
- **Progress Tracking**: Visual progress bars using tqdm for long-running operations
- **Multi-Table Support**: Independent caching for different database tables

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd database-cache-query

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install polars tqdm pytest
```

## Quick Start

```python
from main import get_data

# Define your database fetcher function
def my_fetcher(ids, table_name):
    # Your database query logic here
    # Must return List[Dict[str, Any]]
    return [
        {"id": 1, "name": "Alice", "age": 25, "metadata": '{"city": "NYC"}'},
        {"id": 2, "name": "Bob", "age": 30, "metadata": '{"city": "LA"}'}
    ]

# Query data with automatic caching
result = get_data(
    ids=[1, 2],
    columns=["name", "age", "metadata.city"],  # JSON dot notation supported
    table_name="public.users",
    fetcher=my_fetcher
)

print(result)
# ┌─────┬───────┬─────┬──────────────┐
# │ id  ┆ name  ┆ age ┆ metadata.city │
# │ --- ┆ ---   ┆ --- ┆ ---          │
# │ i64 ┆ str   ┆ i64 ┆ str          │
# ╞═════╪═══════╪═════╪══════════════╡
# │ 1   ┆ Alice ┆ 25  ┆ NYC          │
# │ 2   ┆ Bob   ┆ 30  ┆ LA           │
# └─────┴───────┴─────┴──────────────┘
```

## API Reference

### Main Function

```python
get_data(ids: List[int], columns: List[str], table_name: str, fetcher: Callable) -> polars.DataFrame
```

**Parameters:**
- `ids`: List of integer IDs to retrieve
- `columns`: List of column names (supports JSON dot notation)
- `table_name`: Table identifier in format "schema_name.table_name"
- `fetcher`: Function that takes `(ids: List[int], table_name: str)` and returns `List[Dict[str, Any]]`

**Returns:**
- Polars DataFrame with requested data, sorted by ID

**Raises:**
- `DataNotFoundError`: When requested IDs are not found
- `ColumnNotFoundError`: When requested columns are unavailable
- `ValueError`: For invalid input parameters

### Configuration

The cache directory is configured in `config.py`:

```python
import pathlib

CACHED_DATA_FOLDER = pathlib.Path.home() / "data" / "cached_data"
```

## JSON Field Extraction

Use dot notation to extract nested JSON values:

```python
# If you have a column 'profile' with JSON: {"address": {"city": "NYC", "zip": "10001"}}
result = get_data(
    ids=[1, 2, 3],
    columns=[
        "name",
        "profile.address.city",    # Extracts city from nested JSON
        "profile.address.zip"      # Extracts zip from nested JSON
    ],
    table_name="public.users",
    fetcher=my_fetcher
)
```

## Batch Processing

The system automatically handles large queries by batching them:

- **Batch Size Formula**: `max(100, ceil(missing_count / 100))`
- **Maximum Batches**: 100 batches total
- **Minimum Batch Size**: 100 items
- **Progress Tracking**: Visual progress bars with tqdm

Examples:
- 50 IDs → 1 batch of 100 items
- 500 IDs → 5 batches of 100 items each
- 15,000 IDs → 100 batches of 150 items each

## Cache Behavior

### Cache Strategy
- **Insert-or-Replace**: If an ID exists in cache, the entire row is replaced
- **Schema Evolution**: New columns are automatically added to existing cache files
- **File Organization**: Each table gets its own Parquet file (e.g., `schema_table.parquet`)
- **No Invalidation**: Data never expires (assumes immutable database)

### Cache Lookup Logic
1. Check if all requested columns exist for each ID
2. If ANY column is missing for an ID, mark that ID as "incomplete"
3. Re-fetch ALL requested columns for incomplete IDs
4. Merge new data with existing cache

## Error Handling

```python
from main import get_data, DataNotFoundError, ColumnNotFoundError

try:
    result = get_data(ids=[1, 2, 999], columns=["name"], table_name="public.users", fetcher=my_fetcher)
except DataNotFoundError as e:
    print(f"Missing IDs: {e}")
except ColumnNotFoundError as e:
    print(f"Missing columns: {e}")
except ValueError as e:
    print(f"Invalid input: {e}")
```

## Examples

Run the comprehensive example script:

```bash
python example_usage.py
```

This demonstrates:
- Basic usage with mock data
- JSON field extraction
- Batch processing with progress tracking
- Cache hit/miss scenarios
- Error handling
- Multiple table support

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run specific test modules
pytest test_json_parser.py
pytest test_cache_manager.py
pytest test_batch_processor.py
pytest test_main.py

# Run with verbose output
pytest -v
```

## Architecture

### Module Structure

```
├── config.py              # Configuration settings
├── main.py                 # Main API and orchestration
├── cache_manager.py        # Parquet cache operations
├── batch_processor.py      # Batch processing and progress tracking
├── json_parser.py          # JSON field parsing utilities
└── example_usage.py        # Comprehensive examples
```

### Data Flow

1. **Input Validation**: Validate IDs, columns, and table name
2. **Column Parsing**: Parse JSON dot notation to identify base columns
3. **Cache Lookup**: Check cache for existing data, identify missing/incomplete IDs
4. **Batch Fetching**: Fetch missing data in optimally-sized batches with progress tracking
5. **Data Validation**: Ensure all requested IDs and columns are present
6. **Cache Storage**: Store new data using insert-or-replace logic
7. **JSON Processing**: Extract nested JSON values using dot notation
8. **Result Assembly**: Return complete dataset sorted by ID

## Performance Characteristics

- **Cache Format**: Parquet with Polars for optimal I/O performance
- **Memory Efficient**: Streaming operations for large datasets
- **Batch Optimization**: Configurable batch sizes to balance database load and performance
- **Schema Evolution**: Efficient handling of new columns without cache invalidation
- **Concurrent Safe**: Designed for single-threaded access (no thread safety overhead)

## Requirements

- Python 3.8+
- polars >= 0.20.0
- tqdm >= 4.60.0
- pytest >= 6.0.0 (for testing)

## License

[Add your license here]

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `pytest`
5. Format code: `ruff format .`
6. Check code quality: `ruff check .`
7. Submit a pull request

## Changelog

### v1.0.0
- Initial release
- Smart caching with Parquet/Polars
- Batch processing with progress tracking
- JSON field extraction with dot notation
- Comprehensive error handling
- Multi-table support
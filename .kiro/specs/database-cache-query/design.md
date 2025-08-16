# Design Document

## Overview

The database cache query system is designed as a high-performance caching layer that sits between applications and databases. It uses Parquet files with Polars for optimal storage and retrieval of large datasets (1-10 million rows), implementing intelligent cache management with batch processing and progress tracking.

The system follows a simple workflow:
1. Check cache for existing data
2. Identify missing data (IDs and columns)
3. Batch query the database for missing data with progress tracking
4. Merge new data with existing cache
5. Return complete dataset

## Architecture

### Core Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │───▶│  Cache Manager  │───▶│   Data Fetcher  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │  Parquet Cache  │
                       │   (Polars)      │
                       └─────────────────┘
```

### Component Responsibilities

- **Cache Manager**: Main orchestrator that handles cache lookups, batch processing, and data merging
- **Data Fetcher**: Abstracted database interface using user-provided callable
- **Parquet Cache**: Persistent storage using Polars for efficient querying and storage
- **Batch Processor**: Handles chunking of large queries with progress tracking
- **JSON Parser**: Handles dot notation parsing for nested JSON fields

## Components and Interfaces

### Main API Interface (main.py)

```python
def get_data(ids: List[int], columns: List[str], table_name: str, fetcher: Callable) -> polars.DataFrame:
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
        ValueError: If IDs not found or columns unavailable
    """
```

### Module Structure

The system is organized into separate Python modules:

- `config.py`: Configuration settings including cache directory
- `cache_manager.py`: Cache operations and file management
- `batch_processor.py`: Batch processing and progress tracking
- `json_parser.py`: JSON column parsing utilities
- `main.py`: Main get_data function and orchestration

### config.py

```python
import pathlib

CACHED_DATA_FOLDER = pathlib.Path.home() / "data" / "cached_data"
```

### cache_manager.py

```python
import config

cache_dir = config.CACHED_DATA_FOLDER

def get_cached_data(ids: List[int], columns: List[str], table_name: str) -> Tuple[polars.DataFrame, Set[int]]:
    """Return cached data and identify IDs that are missing or incomplete for the requested columns"""
    
def store_data(data: List[Dict], columns: List[str], table_name: str) -> None:
    """Store new data in cache, merging with existing data for specific table"""
    
def expand_cache_files(table_name: str) -> None:
    """Split cache into multiple files when size limits are reached"""
    
def get_table_cache_path(table_name: str) -> Path:
    """Get the cache file path for a specific table"""
```

### batch_processor.py

```python
def fetch_missing_data(missing_ids: List[int], columns: List[str], table_name: str, fetcher: Callable) -> List[Dict]:
    """Fetch missing data in batches with progress tracking"""
    
def calculate_batch_size(missing_count: int) -> int:
    """Calculate batch size: max(100, ceil(missing_count / 100))"""
```

### json_parser.py

```python
def parse_column_name(column: str) -> Tuple[str, Optional[str]]:
    """Parse column name to identify base column and JSON path"""
    
def extract_json_value(data: Any, json_path: str) -> Any:
    """Extract value from JSON data using dot notation"""
```

## Data Models

### Cache File Structure
Cache directory is specified in config.py and Path.home() / "cached_data"
Each table gets its own Parquet file(s) in the cache directory:

```
~/data/cached_data/
├── schema1_table1.parquet
├── schema1_table2.parquet
├── schema2_table1.parquet
└── schema2_table1_part2.parquet  # When files get too large
```

### Parquet Schema

Each cache file stores data with this structure:

```python
# Example for a table cache file - columns are the actual database column names
{
    "id": polars.Int64,              # The ID column (always present)
    "purchase_quantity": polars.Int64, # Example: actual column from database
    "vendor.address.city": polars.Utf8,             # Example: parsed column from database
    "vender.address.state": polars.Int32,             # Example: parsed column from database
    # ... any other columns that have been requested and cached
}
```

**Key Points:**
- **Database columns**: Stored with their original names (e.g., "purchase_quantity")
- **JSON-derived fields**: Stored with dot notation preserved (e.g., "vendor.address.city")
- **Dynamic schema**: New columns/fields are added as different requests are made
- **Data preservation**: Both original database columns and parsed JSON fields are stored

### Data Flow Models

```python
# Input data from fetcher function
FetcherResponse = List[Dict[str, Any]]

# Cache lookup result (simplified approach)
CacheLookupResult = {
    "found_data": polars.DataFrame,
    "missing_or_incomplete_ids": Set[int]
}

# Processing strategy: If any requested column is missing for an ID, 
# treat that entire ID as "incomplete" and re-fetch all requested columns for that ID
```

## Error Handling

### Exception Types

```python
class DataNotFoundError(ValueError):
    """Raised when requested IDs are not found in cache or database"""
    
class ColumnNotFoundError(ValueError):
    """Raised when requested columns are not available in database"""
    
class JSONParseError(ValueError):
    """Raised when JSON parsing fails for dot notation columns"""
    
class CacheCorruptionError(Exception):
    """Raised when cache files are corrupted or unreadable"""
```

### Error Handling Strategy

1. **Missing IDs**: Raise `DataNotFoundError` with specific missing IDs
2. **Missing Columns**: Raise `ColumnNotFoundError` with unavailable columns
3. **JSON Parsing**: Raise `JSONParseError` with details about parsing failure
4. **Cache Issues**: Attempt recovery, fallback to full database query if needed
5. **Batch Failures**: Continue processing remaining batches, collect errors for final report

## Testing Strategy

### Unit Tests

1. **Cache Manager Tests**
   - Cache hit/miss scenarios
   - Data merging logic
   - File expansion behavior
   - Concurrent access simulation

2. **Batch Processor Tests**
   - Batch size calculation edge cases
   - Progress tracking accuracy
   - Error handling in batch processing
   - Memory usage with large datasets

3. **JSON Parser Tests**
   - Dot notation parsing
   - Nested JSON extraction
   - Error cases (invalid JSON, missing keys)
   - Mixed data types

### Integration Tests

1. **End-to-End Workflow**
   - Complete cache miss scenario
   - Partial cache hit scenario
   - Large dataset processing (1M+ rows)
   - Cache persistence across restarts

2. **Performance Tests**
   - Cache lookup performance
   - Parquet read/write performance
   - Memory usage profiling
   - Batch processing efficiency

### Test Data Strategy

- Use synthetic datasets with known patterns
- Test with various data types (strings, numbers, JSON, nulls)
- Simulate real-world ID distributions
- Test edge cases (empty results, single row, maximum batch sizes)

## Implementation Notes

### Parquet File Management

- Start with single file: `cache.parquet`
- Expand to multiple files when size exceeds ~100MB per file
- Use partitioning strategy based on ID ranges for efficient lookups
- Implement file rotation to maintain optimal file sizes

### Polars Optimization

- Use lazy evaluation for complex queries
- Leverage Polars' native JSON parsing capabilities
- Implement efficient joins for cache lookups
- Use streaming for large dataset processing

### Progress Tracking

- Use tqdm with batch-level granularity
- Display estimated time remaining
- Show current batch size and total progress
- Handle progress bar cleanup on errors

### Memory Management

- Process data in chunks to avoid memory overflow
- Use Polars streaming where possible
- Implement garbage collection hints for large operations
- Monitor memory usage during batch processing